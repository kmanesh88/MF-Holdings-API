"""
MF Holdings API v5 — push-based store.
Accepts: Advisorkhoj consolidated Excel, standard SEBI format Excel,
         multiple Excels, or ZIP containing Excels.
Deploy free on Render.com.
"""

import os, re, io, logging, json, zipfile
from datetime import datetime
from typing import Optional, List
from pathlib import Path

import openpyxl
from fastapi import FastAPI, Query, HTTPException, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app):
    load_db()
    yield

app = FastAPI(title="MF Holdings API", version="5.0.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["GET","POST","DELETE"], allow_headers=["*"])

DATA_DIR = Path(os.environ.get("DATA_DIR", "/tmp/mf_data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_FILE  = DATA_DIR / "holdings.json"
holdings_db: dict = {}
_amfi_cap_cache: dict = {}  # module-level cache for /amfi-cap endpoint

def save_db():
    try: DB_FILE.write_text(json.dumps(holdings_db, ensure_ascii=False))
    except Exception as e: log.warning(f"Save failed: {e}")

def load_db():
    global holdings_db
    try:
        if DB_FILE.exists():
            holdings_db = json.loads(DB_FILE.read_text())
            log.info(f"Loaded {len(holdings_db)} funds from disk")
    except Exception as e: log.warning(f"Load failed: {e}")

def norm(s: str) -> str:
    n = str(s).lower().strip()
    n = re.sub(r'\s*-?\s*(direct|regular)\s*plan.*$', '', n, flags=re.I)
    n = re.sub(r'\s*-?\s*(growth|idcw|dividend)\s*$', '', n, flags=re.I)
    n = re.sub(r'\s*\(g\)\s*$|\s*\(d\)\s*$', '', n, flags=re.I)
    return re.sub(r'\s+', ' ', n).strip()

VALID_ISIN = re.compile(r'^IN[A-Z0-9]{10}$')

SKIP_ROWS = re.compile(
    r'^(a\)|b\)|c\)|d\)|e\)|f\)|\(a\)|\(b\)|\(c\)|\(d\)|\(e\)|\(f\)|'
    r'sub.?total|grand.?total|total.?for|'
    r'treps|reverse\s*repo|margin\s*money|cash\s*and\s*other|net\s*current|'
    r'at\s*the\s*beginning|direct\s*plan|regular\s*plan|riskometer|option|nav\s*per|'
    r'scheme\s*name|name\s*of\s*instrument|sl\s*no)',
    re.I
)

# ── FORMAT 1: Advisorkhoj consolidated Excel ──────────────────────────────
# Sheet "Index" maps acronym → fund name
# Each fund sheet: row1=AMC, row2=FundName, row3=Period, row4=Headers
# Columns: SL|ISIN|Name|Sector|Qty|MktVal|%NAV|YTM
# % stored as decimal: 0.04 = 4%
def is_advisorkhoj_format(wb) -> bool:
    return 'Index' in wb.sheetnames and len(wb.sheetnames) > 3

def parse_advisorkhoj(wb, amc_name: str) -> dict:
    out = {}

    # Build index map: acronym → full fund name
    index_map = {}
    ws_idx = wb['Index']
    for row in ws_idx.iter_rows(values_only=True):
        if row[1] and row[2] and str(row[0]) != 'ACRONYM':
            index_map[str(row[1]).strip()] = str(row[2]).strip()

    for sname in wb.sheetnames:
        if sname in ('Index', 'Annexure-A') or 'annex' in sname.lower():
            continue
        ws   = wb[sname]
        rows = list(ws.iter_rows(values_only=True))
        if len(rows) < 5: continue

        # Fund name: prefer index map, fall back to row 2
        fund_name = index_map.get(sname) or str(rows[1][0] or '').strip() or sname

        # Fixed column positions (always the same in this format)
        ISIN_COL   = 1
        NAME_COL   = 2
        SECTOR_COL = 3
        PCT_COL    = 6

        holdings = []
        cash_pct = 0.0
        CASH_ROWS = re.compile(r'^(treps?|triparty\s*repo|reverse\s*repo|cblo|'
                               r'cash\s*and\s*other\s*net|net\s*current\s*asset)', re.I)
        for row in rows[4:]:  # data starts row 5 (index 4)
            if len(row) <= PCT_COL: continue
            isin    = str(row[ISIN_COL] or '').strip()
            name    = str(row[NAME_COL] or '').strip()
            sector  = str(row[SECTOR_COL] or '').strip()
            pct_raw = row[PCT_COL]

            if not name: continue

            # Capture cash/TREPS rows (no valid ISIN)
            if not VALID_ISIN.match(isin):
                if CASH_ROWS.match(name):
                    try:
                        p = float(pct_raw) * 100
                        if 0 < p < 50: cash_pct += p
                    except: pass
                continue

            # Skip known non-holding rows by name
            if SKIP_ROWS.match(name): continue

            # % is stored as decimal — multiply by 100
            try:
                pct = float(pct_raw) * 100
            except (TypeError, ValueError):
                continue

            if pct <= 0 or pct > 100: continue

            holdings.append({
                "name":   name,
                "isin":   isin,
                "sector": sector,
                "pct":    round(pct, 4),
            })

        if len(holdings) >= 2:
            key = norm(fund_name)
            out[key] = {
                "fund_name":   fund_name.strip(),
                "amc":         amc_name,
                "holdings":    holdings,
                "count":       len(holdings),
                "cashPct":     cash_pct,
                "uploaded_at": datetime.utcnow().isoformat(),
                "format":      "advisorkhoj",
            }
            log.debug(f"  [{sname}] {fund_name}: {len(holdings)} holdings, cash={cash_pct:.2f}%")

    return out

# ── FORMAT 2: Standard SEBI disclosure Excel ─────────────────────────────
# One sheet per fund OR single sheet
# Header row has "ISIN" column, % column = "% to AUM" or "% of NAV"
# % stored as actual percentage: 4.03 = 4.03%
SKIP_SEBI = re.compile(
    r'^(equity$|cash$|grand\s*total|no\.?\s*of\s*stocks|large\s*cap$|mid\s*cap$|'
    r'small\s*cap$|mf\s*/\s*etf|fixed\s*income|mutual\s*fund\s*units|derivatives|'
    r'total$|sub.?total|net\s*equity|net\s*receivable|cblo|repo|treps|scheme\s*name|'
    r'riskometer|past\s*performance|portfolio\s*as\s*on|name\s*of\s*instrument)',
    re.I
)

def parse_sebi_standard(wb, amc_name: str, filename: str = "") -> dict:
    out = {}
    for sname in wb.sheetnames:
        if re.search(r'^(index|cover|content|summary|disclaimer|back|readme|annexure)$', sname, re.I):
            continue
        ws   = wb[sname]
        rows = list(ws.iter_rows(values_only=True))
        if len(rows) < 4: continue

        # Detect fund name
        fname = sname
        for r in rows[:12]:
            cells = [str(c or '').strip() for c in r]
            c0 = cells[0] if cells else ''
            m = re.match(r'scheme\s*name\s*[:\-]\s*(.+)', c0, re.I)
            if m and len(m.group(1).strip()) > 3:
                fname = m.group(1).strip(); break
            if len(cells) > 1 and re.search(r'scheme\s*name', c0, re.I) and len(cells[1]) > 3:
                fname = cells[1]; break
            for c in cells[:3]:
                if len(c) > 8 and re.search(r'\bfund\b', c, re.I) \
                   and not re.search(r'mutual\s*fund$|asset\s*management', c, re.I):
                    fname = c; break

        if fname == sname and filename:
            fn = re.sub(r'\.(xlsx?|csv)$', '', filename, flags=re.I)
            fn = re.sub(r'[-_]', ' ', fn).strip()
            if len(fn) > 5 and re.search(r'fund', fn, re.I): fname = fn

        # Find header row (has ISIN)
        hrow = ncol = icol = scol = pcol = -1
        for i, row in enumerate(rows):
            cl = [str(c or '').lower().strip() for c in row]
            if 'isin' not in cl: continue
            hrow = i; icol = cl.index('isin')
            ncol = next((j for j,c in enumerate(cl) if c in
                         ('name','instrument','security','company','name of instrument','issuer')),
                        next((j for j,c in enumerate(cl) if 'name' in c or 'instrument' in c), -1))
            scol = next((j for j,c in enumerate(cl) if 'sector' in c or 'industry' in c), -1)
            pcol = next((j for j,c in enumerate(cl)
                         if re.search(r'%\s*(to|of)\s*(aum|nav)|nav\s*%|aum\s*%|weight\s*%', c)),
                        next((j for j,c in enumerate(cl) if '%' in c and c != 'isin'), -1))
            break

        if hrow < 0 or ncol < 0 or pcol < 0: continue

        holdings = []
        for row in rows[hrow+1:]:
            if not row or len(row) <= max(ncol, pcol): continue
            name   = str(row[ncol] or '').strip()
            isin   = str(row[icol] or '').strip() if 0 <= icol < len(row) else ''
            sector = str(row[scol] or '').strip() if 0 <= scol < len(row) else ''
            rpct   = row[pcol] if pcol < len(row) else None
            if not name or len(name) < 2 or SKIP_SEBI.match(name): continue
            try: pct = float(str(rpct).replace('%','').replace(',','').strip())
            except: pct = 0.0
            if pct <= 0 or pct > 100: continue
            holdings.append({"name":name,"isin":isin,"sector":sector,"pct":round(pct,4)})

        if len(holdings) >= 3:
            key = norm(fname)
            if key in out:
                out[key]["holdings"].extend(holdings)
                out[key]["count"] = len(out[key]["holdings"])
            else:
                out[key] = {"fund_name":fname.strip(),"amc":amc_name,
                            "holdings":holdings,"count":len(holdings),
                            "uploaded_at":datetime.utcnow().isoformat(),
                            "format":"sebi_standard"}
    return out


# ── FORMAT 3: Kotak/SEBI multi-sheet consolidated Excel ──────────────────
# No Index sheet; each sheet = one fund
# Row 1 col[2]: "Portfolio of <Fund Name> as on DD-Mon-YYYY"
# Row 2: headers (Name|None|None|ISIN|Industry|Yield|Qty|MktVal|%NAV)
# Data: col[2]=Name, col[3]=ISIN, col[4]=Sector, col[8]=% (actual %, not decimal)
def is_kotak_format(wb) -> bool:
    """Detect by checking row1 of first sheet for 'Portfolio of' pattern."""
    for sname in wb.sheetnames[:3]:
        ws = wb[sname]
        rows = list(ws.iter_rows(max_row=2, values_only=True))
        if rows and len(rows[0]) > 2:
            cell = str(rows[0][2] or '').strip()
            if re.match(r'Portfolio of .+ as on', cell, re.I):
                return True
    return False

def parse_kotak(wb, amc_name: str) -> dict:
    out = {}
    for sname in wb.sheetnames:
        ws   = wb[sname]
        rows = list(ws.iter_rows(values_only=True))
        if len(rows) < 3: continue

        # Fund name from row 1 col 2
        fund_raw  = str(rows[0][2] or '').strip() if len(rows[0]) > 2 else ''
        m         = re.match(r'Portfolio of (.+?)\s+as on', fund_raw, re.I)
        fund_name = m.group(1).strip() if m else (fund_raw or sname)
        if not fund_name or fund_name == sname: continue

        holdings = []
        for row in rows[2:]:
            if len(row) < 9: continue
            name    = str(row[2] or '').strip()
            isin    = str(row[3] or '').strip()
            sector  = str(row[4] or '').strip()
            pct_raw = row[8]
            if not VALID_ISIN.match(isin): continue
            if not name or len(name) < 2: continue
            try: pct = float(pct_raw)
            except: continue
            if pct <= 0 or pct > 100: continue
            holdings.append({"name":name,"isin":isin,"sector":sector,"pct":round(pct,4)})

        if len(holdings) >= 2:
            key = norm(fund_name)
            out[key] = {"fund_name":fund_name,"amc":amc_name,
                        "holdings":holdings,"count":len(holdings),
                        "uploaded_at":datetime.utcnow().isoformat(),
                        "format":"kotak"}
    return out


# ── FORMAT 4: ICICI Prudential per-fund Excel (one sheet per file) ────────
# Row 1 col[1]: AMC name, Row 2 col[1]: Fund name, Row 3: Period, Row 4: Headers
# Cols: [1]=Name, [2]=ISIN, [4]=Sector, [7]=% to Nav (decimal, ×100)
def is_icici_format(wb) -> bool:
    ws = wb[wb.sheetnames[0]]
    rows = list(ws.iter_rows(max_row=4, values_only=True))
    if len(rows) < 4: return False
    amc = str(rows[0][1] or '').strip().lower() if len(rows[0]) > 1 else ''
    hdr = [str(c or '').lower() for c in rows[3]] if len(rows) > 3 else []
    return ('icici' in amc or 'mutual fund' in amc) and any('% to nav' in h or '% nav' in h for h in hdr)

def parse_icici(wb, amc_name: str, filename: str = "") -> dict:
    out = {}
    ws   = wb[wb.sheetnames[0]]
    rows = list(ws.iter_rows(values_only=True))
    if len(rows) < 5: return out

    # Fund name from row 2, col 1
    fund_name = str(rows[1][1] or '').strip() if len(rows) > 1 and len(rows[1]) > 1 else ''
    if not fund_name:
        fund_name = re.sub(r'\.xlsx?$', '', filename, flags=re.I).strip()

    holdings = []
    for row in rows[4:]:
        if len(row) < 8: continue
        name    = str(row[1] or '').strip()
        isin    = str(row[2] or '').strip()
        sector  = str(row[4] or '').strip()
        pct_raw = row[7]
        if not VALID_ISIN.match(isin): continue
        if not name or len(name) < 2: continue
        try: pct = float(pct_raw) * 100
        except: continue
        if pct <= 0 or pct > 100: continue
        holdings.append({"name":name,"isin":isin,"sector":sector,"pct":round(pct,4)})

    if len(holdings) >= 2:
        key = norm(fund_name)
        out[key] = {"fund_name":fund_name,"amc":amc_name,
                    "holdings":holdings,"count":len(holdings),
                    "uploaded_at":datetime.utcnow().isoformat(),
                    "format":"icici"}
    return out

# ── ZIP extractor ─────────────────────────────────────────────────────────
def extract_excels_from_zip(raw: bytes) -> list:
    results = []
    try:
        with zipfile.ZipFile(io.BytesIO(raw)) as zf:
            for name in zf.namelist():
                if name.startswith(('__','.')) or name.endswith('/'): continue
                data  = zf.read(name)
                fname = Path(name).name
                if fname.lower().endswith(('.xlsx','.xls')):
                    results.append((fname, data))
                elif fname.lower().endswith('.zip'):
                    results.extend(extract_excels_from_zip(data))
    except Exception as e:
        log.warning(f"ZIP extract failed: {e}")
    return results

# ── Main dispatch ─────────────────────────────────────────────────────────
def process_upload(raw: bytes, filename: str, amc_name: str) -> dict:
    if filename.lower().endswith('.zip'):
        excels = extract_excels_from_zip(raw)
        log.info(f"ZIP '{filename}': {len(excels)} Excel files")
        combined = {}
        for xname, xbytes in excels:
            combined.update(process_upload(xbytes, xname, amc_name))
        return combined

    try:
        wb = openpyxl.load_workbook(io.BytesIO(raw), read_only=True, data_only=True)
    except Exception as e:
        log.warning(f"Cannot open '{filename}': {e}"); return {}

    if is_advisorkhoj_format(wb):
        log.info(f"'{filename}': Advisorkhoj format detected")
        result = parse_advisorkhoj(wb, amc_name)
    elif is_kotak_format(wb):
        log.info(f"'{filename}': Kotak/SEBI multi-sheet format detected")
        result = parse_kotak(wb, amc_name)
    elif is_icici_format(wb):
        log.info(f"'{filename}': ICICI per-fund format detected")
        result = parse_icici(wb, amc_name, filename)
    else:
        log.info(f"'{filename}': SEBI standard format")
        result = parse_sebi_standard(wb, amc_name, filename)

    wb.close()
    return result

# ── App startup & auth ────────────────────────────────────────────────────


def check_secret(secret: str):
    exp = os.environ.get("UPLOAD_SECRET","")
    if exp and secret != exp: raise HTTPException(403,"Invalid secret")

# ── Routes ────────────────────────────────────────────────────────────────
@app.get("/")
async def root():
    amcs = sorted({v["amc"] for v in holdings_db.values()})
    return {"service":"MF Holdings API v5","funds":len(holdings_db),"amcs":amcs,
            "last_updated":max((v.get("uploaded_at","") for v in holdings_db.values()),default=None)}

@app.get("/health")
async def health():
    amcs = sorted({v["amc"] for v in holdings_db.values()})
    return {"status":"ok","funds":len(holdings_db),"amcs":amcs}

@app.get("/funds")
async def list_funds(amc: Optional[str]=None):
    out = [{"name":v["fund_name"],"amc":v["amc"],"key":k,
             "count":v["count"],"uploaded_at":v.get("uploaded_at",""),
             "format":v.get("format","")}
            for k,v in holdings_db.items()
            if not amc or amc.lower() in v.get("amc","").lower()]
    out.sort(key=lambda x:(x["amc"],x["name"]))
    return {"total":len(out),"funds":out}

@app.get("/holdings")
async def get_holdings(fund: str = Query(..., min_length=2)):
    if not holdings_db: raise HTTPException(503,"No data yet — upload via POST /upload")
    q = norm(fund)
    if q in holdings_db: return holdings_db[q]
    qw = set(q.split()); best_s,best_v = 0.0,None
    for k,v in holdings_db.items():
        kw = set(k.split())
        s  = len(qw & kw)/max(len(qw),len(kw),1)
        if q in k: s+=0.6
        if k in q: s+=0.4
        if s>best_s: best_s,best_v=s,v
    if best_v and best_s>=0.25: return best_v
    raise HTTPException(404,f"Not found: '{fund}' — try /search?q=...")

@app.get("/search")
async def search(q: str = Query(..., min_length=2)):
    qn=norm(q); qw=set(qn.split()); res=[]
    for k,v in holdings_db.items():
        kw=set(k.split())
        s=len(qw & kw)/max(len(qw),len(kw),1)
        if qn in k: s+=0.6
        if k in qn: s+=0.4
        if s>0: res.append({"score":round(s,2),"name":v["fund_name"],
                             "amc":v["amc"],"key":k,"count":v["count"]})
    res.sort(key=lambda x:-x["score"])
    return {"query":q,"results":res[:15]}

@app.post("/upload")
async def upload(
    files:  List[UploadFile] = File(...),
    amc:    str              = Form(...),
    secret: str              = Form(default=""),
):
    """
    Upload AMC portfolio files. Supported formats:
    - Advisorkhoj consolidated Excel (has Index sheet — all funds in one file)
    - SEBI standard multi-sheet Excel (one fund per sheet)
    - Multiple individual fund Excels
    - ZIP containing any of the above
    """
    check_secret(secret)
    total_funds = 0; fund_names = []
    for f in files:
        fname = f.filename or "upload"
        if not re.search(r'\.(xlsx|xls|zip)$', fname, re.I): continue
        raw    = await f.read()
        parsed = process_upload(raw, fname, amc.strip())
        if parsed:
            holdings_db.update(parsed)
            total_funds += len(parsed)
            fund_names.extend(v["fund_name"] for v in parsed.values())
            log.info(f"[{amc}] '{fname}' → {len(parsed)} funds")

    if total_funds == 0:
        raise HTTPException(422,"No fund data found — ensure file has ISIN column and % of NAV")
    save_db()
    return {"status":"ok","amc":amc,"files":len(files),
            "funds_added":total_funds,"funds_total":len(holdings_db),"funds":fund_names}

@app.delete("/amc")
async def delete_amc(amc: str, secret: str=""):
    check_secret(secret)
    keys=[k for k,v in holdings_db.items() if v.get("amc","").lower()==amc.lower()]
    for k in keys: del holdings_db[k]
    save_db(); return {"deleted":len(keys),"funds_remaining":len(holdings_db)}

@app.delete("/fund")
async def delete_fund(key: str, secret: str=""):
    check_secret(secret)
    if key not in holdings_db: raise HTTPException(404,f"Key not found: {key}")
    del holdings_db[key]; save_db()
    return {"deleted":key,"funds_remaining":len(holdings_db)}

@app.get("/amfi-cap")
async def amfi_cap():
    """
    Proxy + parse AMFI biannual cap classification Excel.
    Returns { large: ["HDFC BANK", ...], mid: [...], updated: "Dec 2025" }
    Cached in memory for 12 hours.
    """
    import time, urllib.request

    # Use module-level cache
    global _amfi_cap_cache
    if _amfi_cap_cache and (time.time() - _amfi_cap_cache.get("ts", 0)) < 43200:
        return _amfi_cap_cache["data"]

    URLS = [
        ("Dec 2025", "https://www.amfiindia.com/Themes/Theme1/downloads/AverageMarketCapitalization31Dec2025.xlsx"),
        ("Jun 2025", "https://www.amfiindia.com/Themes/Theme1/downloads/AverageMarketCapitalization30Jun2025.xlsx"),
    ]

    def norm_name(n):
        n = str(n).upper().strip()
        n = re.sub(r'\bLTD\.?\b|\bLIMITED\b|\bPVT\.?\b', '', n)
        n = re.sub(r'[.\-,()]', ' ', n)
        return re.sub(r'\s+', ' ', n).strip()

    for label, url in URLS:
        try:
            req = urllib.request.Request(url, headers={
                'User-Agent': 'Mozilla/5.0',
                'Referer': 'https://www.amfiindia.com/'
            })
            raw = urllib.request.urlopen(req, timeout=20).read()
            wb  = openpyxl.load_workbook(io.BytesIO(raw), read_only=True, data_only=True)
            ws  = wb.active
            rows = list(ws.iter_rows(values_only=True))

            # Find header row (has "Company" and "Category" or rank columns)
            name_col = cat_col = rank_col = -1
            hdr_row = -1
            for i, row in enumerate(rows[:10]):
                cells = [str(c or '').lower().strip() for c in row]
                nc = next((j for j, c in enumerate(cells) if 'company' in c or 'name' in c), -1)
                cc = next((j for j, c in enumerate(cells) if 'categor' in c or 'large' in c), -1)
                rc = next((j for j, c in enumerate(cells) if c in ('rank', 'sr', 'sr.', 'sl', 'no', 'no.')), -1)
                if nc >= 0:
                    name_col = nc; cat_col = cc; rank_col = rc; hdr_row = i
                    break

            if name_col < 0:
                log.warning(f"AMFI: header not found in {label}")
                continue

            large, mid, small = [], [], []
            for row in rows[hdr_row + 1:]:
                if not row or not row[name_col]: continue
                name = norm_name(row[name_col])
                if not name or len(name) < 3: continue

                # Determine category
                if cat_col >= 0:
                    cat = str(row[cat_col] or '').lower()
                    if 'large' in cat:   large.append(name)
                    elif 'mid' in cat:   mid.append(name)
                    elif 'small' in cat: small.append(name)
                elif rank_col >= 0:
                    try:
                        rank = int(row[rank_col])
                        if rank <= 100:  large.append(name)
                        elif rank <= 250: mid.append(name)
                        else:            small.append(name)
                    except: pass

            if len(large) >= 90:  # sanity check
                result = {"large": large, "mid": mid, "small": small,
                          "updated": label, "total": len(large)+len(mid)+len(small)}
                _amfi_cap_cache = {"ts": time.time(), "data": result}
                log.info(f"AMFI cap loaded: {len(large)}L {len(mid)}M {len(small)}S ({label})")
                return result

        except Exception as e:
            log.warning(f"AMFI fetch failed ({label}): {e}")

    raise HTTPException(503, "AMFI data temporarily unavailable")


# ── Market Monitor endpoint ───────────────────────────────────────────────
@app.get("/market-data")
async def market_data(stocks: str = ""):
    """
    Uses Claude to generate current Indian market data summary.
    stocks: comma-separated list of portfolio stock names for news context
    """
    try:
        import anthropic as _anthropic
    except ImportError:
        raise HTTPException(503, "anthropic package not installed. Check requirements.txt.")
    from datetime import date
    today = date.today().strftime("%d %B %Y")
    
    prompt = f"""You are a market data assistant for an Indian mutual fund portfolio analyser. Today is {today}.

Provide a structured JSON response with the following for Indian markets:

1. indices: Array of {{name, value, change, changePct}} for: NIFTY 50, SENSEX, NIFTY MIDCAP 150, NIFTY SMALLCAP 250, NIFTY BANK, INDIA VIX. Use your best knowledge of recent values.
2. fii_dii: {{date, fii_net_crore, dii_net_crore, fii_buy, fii_sell, dii_buy, dii_sell}} — recent FII/DII provisional data in crores
3. earnings: Array of {{company, result_date, revenue_growth_pct, profit_growth_pct, beat_miss}} for 6 major companies with recent results
4. market_news: Array of {{headline, category, sentiment}} — 6 key market-moving news items (macro, RBI, global, sector)
5. portfolio_news: Array of {{stock, headline, sentiment}} — news for these stocks: {stocks or "HDFC Bank, Reliance Industries, Infosys, ICICI Bank, Axis Bank, Larsen & Toubro"}

Return ONLY valid JSON, no markdown, no explanation. sentiment values: Positive, Negative, Neutral."""

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        raise HTTPException(503, "ANTHROPIC_API_KEY not set on server. Add it in Render dashboard → Environment.")
    
    try:
        client = _anthropic.Anthropic(api_key=api_key)
        msg = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=1500,
            messages=[{"role": "user", "content": prompt}]
        )
        text = msg.content[0].text.strip()
        import json as _json
        if text.startswith("```"):
            import re as _re
            text = _re.sub(r"^```json?\n?|```$", "", text, flags=_re.M).strip()
        data = _json.loads(text)
        return data
    except _anthropic.AuthenticationError:
        raise HTTPException(503, "Invalid ANTHROPIC_API_KEY. Check the key in Render dashboard.")
    except _json.JSONDecodeError as e:
        log.error(f"market_data JSON parse error: {e}, text: {text[:200]}")
        raise HTTPException(500, f"Invalid response from AI: {str(e)[:80]}")
    except Exception as e:
        log.error(f"market_data error: {e}")
        raise HTTPException(500, f"Market data unavailable: {str(e)[:100]}")


if __name__ == "__main__":
    uvicorn.run("main:app",host="0.0.0.0",port=int(os.environ.get("PORT",8000)),reload=False)

