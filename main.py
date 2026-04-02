"""
MF Holdings API v6 — Universal AMC Parser
Formats confirmed:
  A: Sundaram/Nippon/Axis/ABSL — Index + fund sheets, % decimal
  B: SBI — Index + fund sheets, % actual
  C: Kotak — No Index, name offset in data rows, % actual
  D: ICICI/HDFC — One xlsx per fund (ZIP of individual files)
  E: UTI — Single sheet, SCHEME CODE###STARTS/ENDS markers
"""

import os, re, io, logging, json, zipfile, httpx
from datetime import datetime
from typing import Optional, List
from pathlib import Path

import openpyxl
from fastapi import FastAPI, Query, HTTPException, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

try:
    import xlrd
    HAS_XLRD = True
except ImportError:
    HAS_XLRD = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app):
    load_db()
    yield

app = FastAPI(title="MF Holdings API", version="6.0.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["GET","POST","DELETE"], allow_headers=["*"])

DATA_DIR = Path(os.environ.get("DATA_DIR", "/tmp/mf_data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_FILE  = DATA_DIR / "holdings.json"
holdings_db: dict = {}
_amfi_cap_cache: dict = {}

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

SKIP_ROW = re.compile(
    r'^(sub.?total|grand.?total|total$|total\s+for|'
    r'equity\s*&|equity\s+and|debt\s+instru|listed\s*/|unlisted|'
    r'money\s*market|government\s*securities$|'
    r'treps?|reverse\s*repo|margin|cblo|'
    r'net\s*current|cash\s*and\s*other|'
    r'a\)|b\)|c\)|d\)|e\)|\(a\)|\(b\)|\(c\)|\(d\)|'
    r'nil$|n\.a\.$)',
    re.I
)

CASH_ROW = re.compile(
    r'^(treps?|triparty\s*repo|reverse\s*repo|cblo|'
    r'cash\s*and\s*other\s*net|net\s*current\s*asset)',
    re.I
)


# =============================================================================
# CORE: Universal sheet parser
# =============================================================================
def parse_sheet_universal(rows: list, fund_name: str = "") -> tuple:
    """
    Parse rows into (holdings, cash_pct, fund_name).
    Auto-detects: ISIN col, name col, pct col, decimal vs actual %.
    """
    header_row_idx = isin_col = name_col = pct_col = -1

    for r, row in enumerate(rows[:15]):
        if not row: continue
        vals = {i: str(c or '').strip() for i, c in enumerate(row)}
        vl   = {i: v.lower() for i, v in vals.items()}

        # Pick up fund name from content before header
        if not fund_name and header_row_idx < 0:
            for i, v in vals.items():
                vlo = v.lower()
                if vlo.startswith('scheme:') and len(v) > 10:
                    fund_name = v[7:].strip(); break
                if vlo.startswith('portfolio of '):
                    m = re.match(r'portfolio of (.+?)\s+as on', v, re.I)
                    if m: fund_name = m.group(1).strip(); break
                if re.search(r'scheme\s*name', vlo) and i + 1 in vals:
                    nv = vals.get(i + 1, '')
                    if len(nv) > 5 and 'scheme' not in nv.lower():
                        fund_name = nv; break
                # Motilal: R1 = ['Back to Index', 'Fund Name', ...]
                if vlo in ('back to index', 'back to table of contents') and i + 1 in vals:
                    nv = vals.get(i + 1, '')
                    if len(nv) > 8 and any(k in nv.lower() for k in ['fund', 'etf', 'scheme']):
                        fund_name = nv; break
                if (i < 3 and len(v) > 8 and 'fund' in vlo
                        and 'mutual fund' not in vlo
                        and 'asset management' not in vlo
                        and not fund_name):
                    fund_name = v

        if any('isin' in v for v in vl.values()):
            header_row_idx = r
            for ci, v in vl.items():
                if 'isin' in v and isin_col < 0:
                    isin_col = ci
                if name_col < 0 and any(k in v for k in
                        ['name of', 'instrument', 'issuer', 'company/issuer', 'company']):
                    name_col = ci
                if pct_col < 0 and re.search(
                        r'%\s*(to|of|net)\s*(nav|aum|asset|net)|nav\s*%|aum\s*%', v):
                    pct_col = ci
            break

    if header_row_idx < 0 or isin_col < 0:
        return [], 0.0, fund_name

    if name_col < 0:
        name_col = max(0, isin_col - 1)

    if pct_col < 0:
        hrow = {i: str(c or '').strip().lower()
                for i, c in enumerate(rows[header_row_idx] or [])}
        for ci in range(max(hrow.keys(), default=0), isin_col, -1):
            if '%' in hrow.get(ci, '') or 'nav' in hrow.get(ci, ''):
                pct_col = ci; break
        if pct_col < 0:
            pct_col = max(hrow.keys(), default=isin_col + 4)

    # Detect name offset (Kotak-style indented rows)
    actual_name_col = name_col
    for row in rows[header_row_idx + 1: header_row_idx + 8]:
        if not row: continue
        vals = {i: str(c or '').strip() for i, c in enumerate(row)}
        if VALID_ISIN.match(vals.get(isin_col, '')):
            if not vals.get(name_col, ''):
                for ci in range(isin_col - 1, -1, -1):
                    v = vals.get(ci, '')
                    if v and len(v) > 2 and not SKIP_ROW.match(v):
                        actual_name_col = ci; break
            break

    # Detect decimal %
    pct_is_decimal = False
    for row in rows[header_row_idx + 1: header_row_idx + 8]:
        if not row: continue
        vals = {i: str(c or '').strip() for i, c in enumerate(row)}
        if VALID_ISIN.match(vals.get(isin_col, '')):
            raw = vals.get(pct_col, '').replace('%', '').replace(',', '').strip()
            try:
                pv = float(raw)
                if 0 < pv < 0.6:
                    pct_is_decimal = True
            except:
                pass
            break

    holdings  = []
    cash_pct  = 0.0

    for row in rows[header_row_idx + 1:]:
        if not row: continue
        vals = {i: str(c or '').strip() for i, c in enumerate(row)}

        isin_val = vals.get(isin_col, '')
        name_val = vals.get(actual_name_col, '').strip()

        if not VALID_ISIN.match(isin_val):
            if name_val and CASH_ROW.match(name_val):
                raw = vals.get(pct_col, '').replace('%', '').replace(',', '').strip()
                try:
                    p = float(raw)
                    if pct_is_decimal: p *= 100
                    if 0 < p < 50: cash_pct += p
                except:
                    pass
            continue

        if not name_val or len(name_val) < 2: continue
        if SKIP_ROW.match(name_val): continue

        # Strip UTI-style prefixes: "EQ - ", "DB - " etc.
        name_val = re.sub(r'^(EQ|DB|NCD|CP|TB|GB|MF|CB)\s*[-\u2013]\s*', '', name_val).strip()

        raw_pct = vals.get(pct_col, '').replace('%', '').replace(',', '').strip()
        raw_pct = re.sub(r'[^\d.\-]', '', raw_pct)
        try:
            pct = float(raw_pct)
        except:
            continue

        if pct_is_decimal:
            pct *= 100

        if pct <= 0 or pct > 100:
            continue

        sector_col = isin_col + 1
        sector = vals.get(sector_col, '')
        if sector and (re.match(r'^[\d.]+$', sector) or '%' in sector):
            sector = ''

        holdings.append({
            "name":   name_val,
            "isin":   isin_val,
            "sector": sector,
            "pct":    round(pct, 4),
        })

    return holdings, round(cash_pct, 4), fund_name


# =============================================================================
# FORMAT DETECTORS
# =============================================================================
def _has_index_sheet(wb) -> bool:
    return any(s.strip().lower() == 'index' for s in wb.sheetnames)

def _is_uti_format(wb) -> bool:
    if len(wb.sheetnames) != 1: return False
    ws = wb[wb.sheetnames[0]]
    for row in ws.iter_rows(max_row=3, values_only=True):
        if row and row[0] and 'SCHEME CODE' in str(row[0]).upper():
            return True
    return False

def _is_kotak_format(wb) -> bool:
    if _has_index_sheet(wb): return False
    for sname in wb.sheetnames[:3]:
        ws = wb[sname]
        rows = list(ws.iter_rows(max_row=2, values_only=True))
        if rows and len(rows[0]) > 2:
            cell = str(rows[0][2] or '').strip()
            if re.match(r'portfolio of .+ as on', cell, re.I):
                return True
    return False

def _is_single_fund(wb) -> bool:
    if len(wb.sheetnames) != 1: return False
    ws = wb[wb.sheetnames[0]]
    for row in ws.iter_rows(max_row=5, values_only=True):
        if any('isin' in str(c or '').lower() for c in row):
            return True
    return False


# =============================================================================
# FORMAT E: UTI
# =============================================================================
def parse_uti(wb, amc_name: str) -> dict:
    out = {}
    ws = wb[wb.sheetnames[0]]
    all_rows = list(ws.iter_rows(values_only=True))

    current_fund = None
    fund_rows    = []

    def flush(fund_name, rows, out):
        if not fund_name or not rows: return
        holdings, cash_pct, _ = parse_sheet_universal(rows, fund_name)
        if len(holdings) >= 2:
            total = sum(h['pct'] for h in holdings)
            if total >= 5:
                key = norm(fund_name)
                out[key] = {"fund_name": fund_name, "amc": amc_name,
                            "holdings": holdings, "count": len(holdings),
                            "cashPct": cash_pct,
                            "uploaded_at": datetime.utcnow().isoformat(),
                            "format": "uti"}

    for row in all_rows:
        v0 = str(row[0] or '').strip() if row else ''
        vu = v0.upper()

        if 'SCHEME CODE' in vu and 'STARTS' in vu:
            flush(current_fund, fund_rows, out)
            current_fund = None; fund_rows = []; continue

        if 'SCHEME CODE' in vu and 'ENDS' in vu:
            flush(current_fund, fund_rows, out)
            current_fund = None; fund_rows = []; continue

        if v0.upper().startswith('SCHEME:'):
            current_fund = v0[7:].strip(); fund_rows = []; continue

        if current_fund:
            fund_rows.append(row)

    flush(current_fund, fund_rows, out)
    log.info(f"UTI: {len(out)} funds")
    return out


# =============================================================================
# FORMAT A/B: Multi-sheet with Index (Sundaram, Nippon, Axis, ABSL, SBI)
# =============================================================================
def parse_multi_sheet_with_index(wb, amc_name: str) -> dict:
    out = {}
    skip_sheets = {'index', 'cover', 'summary', 'disclaimer', 'annexure',
                   'contents', 'readme', 'back', 'annexure-a'}

    # Build index map: sheet_code → full fund name
    # Handles multiple layouts:
    #   Standard: [code, name] — Sundaram/Nippon/Axis/ABSL/SBI
    #   Motilal:  [serial, name, code] — code is last, name is middle
    index_map  = {}
    idx_sheet  = next((s for s in wb.sheetnames if s.strip().lower() == 'index'), None)
    if idx_sheet:
        for row in wb[idx_sheet].iter_rows(values_only=True):
            if not row: continue
            vals = [str(c or '').strip() for c in row]
            # Try standard pattern: scan for [code, name] pair
            found = False
            for i in range(len(vals) - 1):
                code = vals[i]; name = vals[i + 1] if i + 1 < len(vals) else ''
                if (re.match(r'^[A-Z0-9\-]{2,20}$', code)
                        and len(name) > 8
                        and any(k in name.lower() for k in ['fund', 'scheme', 'plan'])):
                    index_map[code] = name; found = True; break
            if not found:
                # Try Motilal pattern: [serial, name, code] — name before code
                for i in range(len(vals) - 1):
                    name = vals[i]; code = vals[i + 1] if i + 1 < len(vals) else ''
                    if (len(name) > 8
                            and any(k in name.lower() for k in ['fund', 'scheme', 'plan'])
                            and re.match(r'^[A-Z0-9\-]{2,20}$', code)):
                        index_map[code] = name; break

    for sname in wb.sheetnames:
        if sname.strip().lower() in skip_sheets: continue
        ws   = wb[sname]
        rows = list(ws.iter_rows(values_only=True))
        if len(rows) < 4: continue

        fund_name = index_map.get(sname, '')
        holdings, cash_pct, detected = parse_sheet_universal(rows, fund_name)
        if not fund_name: fund_name = detected or sname

        if len(holdings) >= 2:
            total = sum(h['pct'] for h in holdings)
            if total < 5 or total > 200: continue
            key = norm(fund_name)
            out[key] = {"fund_name": fund_name.strip(), "amc": amc_name,
                        "holdings": holdings, "count": len(holdings),
                        "cashPct": cash_pct,
                        "uploaded_at": datetime.utcnow().isoformat(),
                        "format": "multi_index"}

    log.info(f"Multi-index: {len(out)} funds from {len(wb.sheetnames)} sheets")
    return out


# =============================================================================
# FORMAT C: Kotak — multi-sheet, no Index
# =============================================================================
def parse_kotak_style(wb, amc_name: str) -> dict:
    out = {}
    skip_sheets = {'index', 'cover', 'summary', 'disclaimer', 'annexure',
                   'contents', 'readme', 'back'}

    for sname in wb.sheetnames:
        if sname.strip().lower() in skip_sheets: continue
        ws   = wb[sname]
        rows = list(ws.iter_rows(values_only=True))
        if len(rows) < 3: continue

        fund_name = ''
        if rows[0] and len(rows[0]) > 2:
            cell = str(rows[0][2] or '').strip()
            m = re.match(r'portfolio of (.+?)\s+as on', cell, re.I)
            if m: fund_name = m.group(1).strip()

        holdings, cash_pct, detected = parse_sheet_universal(rows, fund_name)
        if not fund_name: fund_name = detected or sname

        if len(holdings) >= 2:
            total = sum(h['pct'] for h in holdings)
            if total < 5 or total > 200: continue  # skip bad parses
            key = norm(fund_name)
            out[key] = {"fund_name": fund_name, "amc": amc_name,
                        "holdings": holdings, "count": len(holdings),
                        "cashPct": cash_pct,
                        "uploaded_at": datetime.utcnow().isoformat(),
                        "format": "kotak"}

    log.info(f"Kotak-style: {len(out)} funds")
    return out


# =============================================================================
# FORMAT D: Single fund per file (ICICI, HDFC)
# =============================================================================
def parse_single_fund(wb, amc_name: str, filename: str = "") -> dict:
    out = {}
    ws   = wb[wb.sheetnames[0]]
    rows = list(ws.iter_rows(values_only=True))
    if len(rows) < 3: return out

    fund_name = ''
    for row in rows[:4]:
        for c in (row or []):
            v = str(c or '').strip()
            if len(v) > 8 and 'fund' in v.lower() and 'mutual fund' not in v.lower():
                fund_name = v; break
        if fund_name: break
    if not fund_name:
        fund_name = re.sub(r'\.xlsx?$', '', filename, flags=re.I).strip()

    holdings, cash_pct, detected = parse_sheet_universal(rows, fund_name)
    if not fund_name: fund_name = detected or filename

    if len(holdings) >= 2:
        total = sum(h['pct'] for h in holdings)
        if total >= 5:
            key = norm(fund_name)
            out[key] = {"fund_name": fund_name, "amc": amc_name,
                        "holdings": holdings, "count": len(holdings),
                        "cashPct": cash_pct,
                        "uploaded_at": datetime.utcnow().isoformat(),
                        "format": "single_fund"}
    return out


# =============================================================================
# .XLS LOADER (xlrd wrapper — Nippon, ABSL)
# =============================================================================
def load_xls_as_wb(raw: bytes):
    if not HAS_XLRD:
        log.warning(".xls file but xlrd not installed")
        return None
    try:
        wb_xls = xlrd.open_workbook(file_contents=raw)
    except Exception as e:
        log.warning(f"xlrd failed: {e}"); return None

    class FakeSheet:
        def __init__(self, ws): self._ws = ws
        def iter_rows(self, values_only=True, max_row=None):
            nrows = self._ws.nrows
            if max_row: nrows = min(nrows, max_row)
            for r in range(nrows):
                yield tuple(self._ws.cell_value(r, c) for c in range(self._ws.ncols))

    class FakeWB:
        def __init__(self, wb):
            self.sheetnames = wb.sheet_names()
            self._sheets = {n: FakeSheet(wb.sheet_by_name(n)) for n in self.sheetnames}
        def __getitem__(self, n): return self._sheets[n]
        def close(self): pass

    return FakeWB(wb_xls)


# =============================================================================
# ZIP EXTRACTOR
# =============================================================================
def extract_excels_from_zip(raw: bytes) -> list:
    results = []
    try:
        with zipfile.ZipFile(io.BytesIO(raw)) as zf:
            for name in zf.namelist():
                if name.startswith(('__', '.')) or name.endswith('/'): continue
                data  = zf.read(name)
                fname = Path(name).name
                if fname.lower().endswith(('.xlsx', '.xls')):
                    results.append((fname, data))
                elif fname.lower().endswith('.zip'):
                    results.extend(extract_excels_from_zip(data))
    except Exception as e:
        log.warning(f"ZIP extract failed: {e}")
    return results


# =============================================================================
# MAIN DISPATCH
# =============================================================================
def open_workbook(raw: bytes, filename: str):
    if filename.lower().endswith('.xls') and not filename.lower().endswith('.xlsx'):
        wb = load_xls_as_wb(raw)
        if wb: return wb, 'xls'
    try:
        wb = openpyxl.load_workbook(
            io.BytesIO(raw), read_only=True, data_only=True, keep_links=False)
        return wb, 'xlsx'
    except Exception as e:
        log.warning(f"Cannot open '{filename}': {e}")
        return None, None


def process_upload(raw: bytes, filename: str, amc_name: str) -> dict:
    if filename.lower().endswith('.zip'):
        excels = extract_excels_from_zip(raw)
        log.info(f"ZIP '{filename}': {len(excels)} files")
        combined = {}
        for xname, xbytes in excels:
            combined.update(process_upload(xbytes, xname, amc_name))
        return combined

    wb, ftype = open_workbook(raw, filename)
    if not wb: return {}

    n = len(wb.sheetnames)
    log.info(f"'{filename}': {n} sheets [{ftype}]")

    if _is_uti_format(wb):
        log.info("  -> Format E (UTI)")
        result = parse_uti(wb, amc_name)
    elif _has_index_sheet(wb) and n > 3:
        log.info("  -> Format A/B (multi-sheet + Index)")
        result = parse_multi_sheet_with_index(wb, amc_name)
    elif _is_kotak_format(wb):
        log.info("  -> Format C (Kotak-style)")
        result = parse_kotak_style(wb, amc_name)
    elif _is_single_fund(wb) or n == 1:
        log.info("  -> Format D (single fund)")
        result = parse_single_fund(wb, amc_name, filename)
    else:
        log.info("  -> Fallback (multi-sheet no Index)")
        result = parse_kotak_style(wb, amc_name)

    try: wb.close()
    except: pass

    log.info(f"  -> {len(result)} funds extracted")
    return result


# =============================================================================
# AUTH + ROUTES
# =============================================================================
def check_secret(secret: str):
    exp = os.environ.get("UPLOAD_SECRET", "")
    if exp and secret != exp: raise HTTPException(403, "Invalid secret")

@app.get("/")
async def root():
    amcs = sorted({v["amc"] for v in holdings_db.values()})
    return {"service": "MF Holdings API v6", "funds": len(holdings_db), "amcs": amcs,
            "last_updated": max(
                (v.get("uploaded_at", "") for v in holdings_db.values()), default=None)}

@app.get("/health")
async def health():
    amcs = sorted({v["amc"] for v in holdings_db.values()})
    return {"status": "ok", "funds": len(holdings_db), "amcs": amcs}

@app.get("/funds")
async def list_funds(amc: Optional[str] = None):
    out = [{"name": v["fund_name"], "amc": v["amc"], "key": k,
            "count": v["count"], "uploaded_at": v.get("uploaded_at", ""),
            "format": v.get("format", "")}
           for k, v in holdings_db.items()
           if not amc or amc.lower() in v.get("amc", "").lower()]
    out.sort(key=lambda x: (x["amc"], x["name"]))
    return {"total": len(out), "funds": out}

@app.get("/holdings")
async def get_holdings(fund: str = Query(..., min_length=2)):
    if not holdings_db: raise HTTPException(503, "No data yet")
    q = norm(fund)
    if q in holdings_db: return holdings_db[q]
    qw = set(q.split()); best_s, best_v = 0.0, None
    for k, v in holdings_db.items():
        kw = set(k.split())
        s  = len(qw & kw) / max(len(qw), len(kw), 1)
        if q in k: s += 0.6
        if k in q: s += 0.4
        if s > best_s: best_s, best_v = s, v
    if best_v and best_s >= 0.25: return best_v
    raise HTTPException(404, f"Not found: '{fund}'")

@app.get("/search")
async def search(q: str = Query(..., min_length=2)):
    qn = norm(q); qw = set(qn.split()); res = []
    for k, v in holdings_db.items():
        kw = set(k.split())
        s  = len(qw & kw) / max(len(qw), len(kw), 1)
        if qn in k: s += 0.6
        if k in qn: s += 0.4
        if s > 0:
            res.append({"score": round(s, 2), "name": v["fund_name"],
                        "amc": v["amc"], "key": k, "count": v["count"]})
    res.sort(key=lambda x: -x["score"])
    return {"query": q, "results": res[:15]}

@app.post("/upload")
async def upload(
    files:  List[UploadFile] = File(...),
    amc:    str              = Form(...),
    secret: str              = Form(default=""),
):
    """Upload AMC portfolio Excel/ZIP. Auto-detects format."""
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
            log.info(f"[{amc}] '{fname}' -> {len(parsed)} funds")

    if total_funds == 0:
        raise HTTPException(422, "No fund data found — check file format")
    save_db()
    return {"status": "ok", "amc": amc, "files": len(files),
            "funds_added": total_funds, "funds_total": len(holdings_db),
            "funds": fund_names}

@app.post("/preview")
async def preview_upload(
    files:  List[UploadFile] = File(...),
    amc:    str              = Form(...),
    secret: str              = Form(default=""),
):
    """Parse and preview without saving. Returns fund names + counts + total %."""
    check_secret(secret)
    preview = []
    for f in files:
        fname = f.filename or "upload"
        if not re.search(r'\.(xlsx|xls|zip)$', fname, re.I): continue
        raw    = await f.read()
        parsed = process_upload(raw, fname, amc.strip())
        for key, data in parsed.items():
            total_pct = sum(h['pct'] for h in data['holdings'])
            preview.append({
                "fund_name": data['fund_name'],
                "holdings":  data['count'],
                "total_pct": round(total_pct, 1),
                "format":    data.get('format', ''),
                "valid":     80 <= total_pct <= 115,
            })
    preview.sort(key=lambda x: x['fund_name'])
    valid = sum(1 for p in preview if p['valid'])
    return {"amc": amc, "total": len(preview),
            "valid": valid, "invalid": len(preview) - valid, "funds": preview}

@app.delete("/amc")
async def delete_amc(amc: str, secret: str = ""):
    check_secret(secret)
    keys = [k for k, v in holdings_db.items()
            if v.get("amc", "").lower() == amc.lower()]
    for k in keys: del holdings_db[k]
    save_db(); return {"deleted": len(keys), "funds_remaining": len(holdings_db)}

@app.delete("/fund")
async def delete_fund(key: str, secret: str = ""):
    check_secret(secret)
    if key not in holdings_db: raise HTTPException(404, f"Key not found: {key}")
    del holdings_db[key]; save_db()
    return {"deleted": key, "funds_remaining": len(holdings_db)}


# =============================================================================
# AMFI CAP PROXY
# =============================================================================
@app.get("/amfi-cap")
async def amfi_cap():
    import time, urllib.request
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
                'User-Agent': 'Mozilla/5.0', 'Referer': 'https://www.amfiindia.com/'})
            raw = urllib.request.urlopen(req, timeout=20).read()
            wb  = openpyxl.load_workbook(io.BytesIO(raw), read_only=True, data_only=True)
            ws  = wb.active
            rows = list(ws.iter_rows(values_only=True))
            name_col = cat_col = rank_col = hdr_row = -1
            for i, row in enumerate(rows[:10]):
                cells = [str(c or '').lower().strip() for c in row]
                nc = next((j for j, c in enumerate(cells) if 'company' in c or 'name' in c), -1)
                cc = next((j for j, c in enumerate(cells) if 'categor' in c or 'large' in c), -1)
                rc = next((j for j, c in enumerate(cells)
                           if c in ('rank', 'sr', 'sr.', 'sl', 'no', 'no.')), -1)
                if nc >= 0: name_col=nc; cat_col=cc; rank_col=rc; hdr_row=i; break
            if name_col < 0: continue
            large, mid, small = [], [], []
            for row in rows[hdr_row + 1:]:
                if not row or not row[name_col]: continue
                name = norm_name(row[name_col])
                if not name or len(name) < 3: continue
                if cat_col >= 0:
                    cat = str(row[cat_col] or '').lower()
                    if 'large' in cat: large.append(name)
                    elif 'mid' in cat: mid.append(name)
                    elif 'small' in cat: small.append(name)
                elif rank_col >= 0:
                    try:
                        rank = int(row[rank_col])
                        if rank <= 100: large.append(name)
                        elif rank <= 250: mid.append(name)
                        else: small.append(name)
                    except: pass
            if len(large) >= 90:
                result = {"large": large, "mid": mid, "small": small,
                          "updated": label, "total": len(large)+len(mid)+len(small)}
                _amfi_cap_cache = {"ts": time.time(), "data": result}
                return result
        except Exception as e:
            log.warning(f"AMFI fetch failed ({label}): {e}")

    raise HTTPException(503, "AMFI data temporarily unavailable")


# =============================================================================
# MARKET MONITOR
# =============================================================================
@app.get("/market-test")
async def market_test():
    import time
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key: return {"ok": False, "error": "ANTHROPIC_API_KEY not set"}
    t0 = time.time()
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={"x-api-key": api_key, "anthropic-version": "2023-06-01",
                         "content-type": "application/json"},
                json={"model": "claude-haiku-4-5-20251001", "max_tokens": 10,
                      "messages": [{"role": "user", "content": "Say OK"}]})
        ms = int((time.time()-t0)*1000)
        if resp.status_code == 200:
            return {"ok": True, "ms": ms, "model": "claude-haiku-4-5-20251001"}
        return {"ok": False, "status": resp.status_code, "error": resp.text[:100]}
    except Exception as e:
        return {"ok": False, "error": str(e)[:100]}

_market_cache: dict = {"data": None, "ts": 0.0, "status": "idle"}

async def _fetch_market_bg(api_key: str, stock_list: str):
    import json as _j, time
    from datetime import date
    _market_cache["status"] = "fetching"
    today  = date.today().strftime("%d %B %Y")
    prompt = (
        f"Today is {today}. Return ONLY valid JSON (no markdown) with:\n"
        '{"indices":[{"name":"NIFTY 50","value":0,"change":0,"changePct":0},'
        '{"name":"SENSEX","value":0,"change":0,"changePct":0},'
        '{"name":"NIFTY MIDCAP 150","value":0,"change":0,"changePct":0},'
        '{"name":"NIFTY SMALLCAP 250","value":0,"change":0,"changePct":0},'
        '{"name":"NIFTY BANK","value":0,"change":0,"changePct":0},'
        '{"name":"INDIA VIX","value":0,"change":0,"changePct":0}],'
        '"fii_dii":{"date":"","fii_net_crore":0,"dii_net_crore":0,"fii_buy":0,'
        '"fii_sell":0,"dii_buy":0,"dii_sell":0},'
        '"earnings":[{"company":"","result_date":"","revenue_growth_pct":0,'
        '"profit_growth_pct":0,"beat_miss":"Beat"}],'
        '"market_news":[{"headline":"","category":"","sentiment":"Positive"}],'
        f'"portfolio_news":[{{"stock":"","headline":"","sentiment":"Positive"}}]}}'
        f"\nFill with real recent data. Stocks: {stock_list}"
    )
    try:
        async with httpx.AsyncClient(timeout=28.0) as client:
            resp = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={"x-api-key": api_key, "anthropic-version": "2023-06-01",
                         "content-type": "application/json"},
                json={"model": "claude-haiku-4-5-20251001", "max_tokens": 1200,
                      "messages": [{"role": "user", "content": prompt}]})
        if resp.status_code == 200:
            text = resp.json()["content"][0]["text"].strip()
            text = re.sub(r"```[^\n]*\n?|```", "", text).strip()
            _market_cache["data"]   = _j.loads(text)
            _market_cache["ts"]     = time.time()
            _market_cache["status"] = "ready"
        else:
            _market_cache["status"] = "error"
    except Exception as e:
        log.error(f"_fetch_market_bg: {e}"); _market_cache["status"] = "error"

@app.get("/market-data")
async def market_data(stocks: str = ""):
    import asyncio, time
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key: raise HTTPException(503, "ANTHROPIC_API_KEY not configured")
    age = time.time() - _market_cache["ts"]
    if _market_cache["data"] and age < 1800: return _market_cache["data"]
    stock_list = stocks or "HDFC Bank, Reliance, Infosys, ICICI Bank, Axis Bank"
    if _market_cache["status"] != "fetching":
        asyncio.create_task(_fetch_market_bg(api_key, stock_list))
    if _market_cache["data"]: return _market_cache["data"]
    return {"status": "loading",
            "message": "Generating market data, please refresh in 20 seconds",
            "indices": [], "fii_dii": {}, "earnings": [],
            "market_news": [], "portfolio_news": []}


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0",
                port=int(os.environ.get("PORT", 8000)), reload=False)
