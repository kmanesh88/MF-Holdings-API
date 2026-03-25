"""
MF Holdings API — push-based store.
You upload AMC Excels once via POST /upload, server parses and stores them.
All clients then get holdings instantly via GET /holdings?fund=...

Deploy free on Render.com.
"""

import os, re, io, logging, json, hashlib
from datetime import datetime
from typing import Optional
from pathlib import Path

import openpyxl
from fastapi import FastAPI, Query, HTTPException, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import uvicorn

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

app = FastAPI(title="MF Holdings API", version="3.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["GET","POST","DELETE"], allow_headers=["*"])

# ── Persistent storage on disk (survives Render restarts if using a disk mount)
# Falls back to in-memory if no disk available
DATA_DIR = Path(os.environ.get("DATA_DIR", "/tmp/mf_data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_FILE  = DATA_DIR / "holdings.json"

holdings_db: dict = {}  # { norm(fund_name): {fund_name, amc, holdings, count, uploaded_at} }

def save_db():
    try:
        DB_FILE.write_text(json.dumps(holdings_db, ensure_ascii=False))
    except Exception as e:
        log.warning(f"Could not save DB: {e}")

def load_db():
    global holdings_db
    try:
        if DB_FILE.exists():
            holdings_db = json.loads(DB_FILE.read_text())
            log.info(f"Loaded {len(holdings_db)} funds from disk")
    except Exception as e:
        log.warning(f"Could not load DB: {e}")

# ── Name normalisation ─────────────────────────────────────────────────────
def norm(s: str) -> str:
    n = str(s).lower().strip()
    n = re.sub(r'\s*-?\s*(direct|regular)\s*plan.*$', '', n, flags=re.I)
    n = re.sub(r'\s*-?\s*(growth|idcw|dividend)\s*$', '', n, flags=re.I)
    n = re.sub(r'\s*\(g\)\s*$|\s*\(d\)\s*$', '', n, flags=re.I)
    return re.sub(r'\s+', ' ', n).strip()

# ── Excel parser ───────────────────────────────────────────────────────────
SKIP = re.compile(
    r'^(equity$|cash$|grand\s*total|no\.?\s*of\s*stocks|large\s*cap$|mid\s*cap$|'
    r'small\s*cap$|mf\s*/\s*etf|fixed\s*income|mutual\s*fund\s*units|derivatives|'
    r'total$|sub.?total|net\s*equity|net\s*receivable|cblo|repo|treps|scheme\s*name|'
    r'riskometer|past\s*performance|portfolio\s*as\s*on|name\s*of\s*instrument)',
    re.I
)

def parse_excel(raw: bytes, amc_name: str) -> dict:
    out = {}
    try:
        wb = openpyxl.load_workbook(io.BytesIO(raw), read_only=True, data_only=True)
    except Exception as e:
        log.warning(f"openpyxl failed: {e}"); return out

    for sname in wb.sheetnames:
        if re.search(r'^(index|cover|content|summary|disclaimer|back|readme)$', sname, re.I):
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

        # Detect header row (ISIN column)
        hrow = ncol = icol = scol = pcol = -1
        for i, row in enumerate(rows):
            cl = [str(c or '').lower().strip() for c in row]
            if 'isin' not in cl: continue
            hrow = i
            icol = cl.index('isin')
            ncol = next(
                (j for j,c in enumerate(cl) if c in ('name','instrument','security',
                 'company','name of instrument','issuer')),
                next((j for j,c in enumerate(cl) if 'name' in c or 'instrument' in c), -1)
            )
            scol = next((j for j,c in enumerate(cl) if 'sector' in c or 'industry' in c), -1)
            pcol = next(
                (j for j,c in enumerate(cl)
                 if re.search(r'%\s*(to|of)\s*(aum|nav)|nav\s*%|aum\s*%|weight\s*%', c)),
                next((j for j,c in enumerate(cl) if '%' in c and c != 'isin'), -1)
            )
            break

        if hrow < 0 or ncol < 0 or pcol < 0: continue

        holdings = []
        for row in rows[hrow+1:]:
            if not row or len(row) <= max(ncol, pcol): continue
            name   = str(row[ncol]  or '').strip()
            isin   = str(row[icol]  or '').strip() if icol >= 0 and icol < len(row) else ''
            sector = str(row[scol]  or '').strip() if scol >= 0 and scol < len(row) else ''
            rpct   = row[pcol] if pcol < len(row) else None
            if not name or len(name) < 2 or SKIP.match(name): continue
            try:
                pct = float(str(rpct).replace('%','').replace(',','').strip())
            except: pct = 0.0
            if pct <= 0 or pct > 100: continue
            holdings.append({"name": name, "isin": isin, "sector": sector, "pct": round(pct,4)})

        if len(holdings) >= 3:
            key = norm(fname)
            if key in out:
                out[key]["holdings"].extend(holdings)
                out[key]["count"] = len(out[key]["holdings"])
            else:
                out[key] = {
                    "fund_name":   fname.strip(),
                    "amc":         amc_name,
                    "holdings":    holdings,
                    "count":       len(holdings),
                    "uploaded_at": datetime.utcnow().isoformat(),
                }
    wb.close()
    return out

# ── Startup ────────────────────────────────────────────────────────────────
@app.on_event("startup")
async def startup():
    load_db()

# ── Auth helper ────────────────────────────────────────────────────────────
def check_secret(secret: str):
    expected = os.environ.get("UPLOAD_SECRET", "")
    if expected and secret != expected:
        raise HTTPException(403, "Invalid secret — set UPLOAD_SECRET env var on Render")

# ── Routes ─────────────────────────────────────────────────────────────────
@app.get("/")
async def root():
    return {
        "service":      "MF Holdings API",
        "funds":        len(holdings_db),
        "last_updated": max((v.get("uploaded_at","") for v in holdings_db.values()), default=None),
        "endpoints": {
            "GET  /health":                   "health check",
            "GET  /funds":                    "list all loaded funds",
            "GET  /holdings?fund=<name>":     "get holdings for a fund",
            "GET  /search?q=<query>":         "search fund names",
            "POST /upload":                   "upload AMC Excel (multipart: file + amc + secret)",
            "DELETE /fund?key=<key>&secret=": "remove a fund",
        }
    }

@app.get("/health")
async def health():
    return {
        "status": "ok",
        "funds":  len(holdings_db),
        "amcs":   list({v["amc"] for v in holdings_db.values()}),
    }

@app.get("/funds")
async def list_funds(amc: Optional[str] = None):
    out = [
        {"name": v["fund_name"], "amc": v["amc"], "key": k,
         "count": v["count"], "uploaded_at": v.get("uploaded_at","")}
        for k,v in holdings_db.items()
        if not amc or amc.lower() in v.get("amc","").lower()
    ]
    out.sort(key=lambda x: (x["amc"], x["name"]))
    return {"total": len(out), "funds": out}

@app.get("/holdings")
async def get_holdings(fund: str = Query(..., min_length=2)):
    if not holdings_db:
        raise HTTPException(503, "No data loaded yet — upload an AMC Excel via POST /upload")
    q = norm(fund)
    if q in holdings_db: return holdings_db[q]

    # Fuzzy match
    qw = set(q.split())
    best_s, best_v = 0.0, None
    for k, v in holdings_db.items():
        kw = set(k.split())
        s  = len(qw & kw) / max(len(qw), len(kw), 1)
        if q in k: s += 0.6
        if k in q: s += 0.4
        if s > best_s: best_s, best_v = s, v
    if best_v and best_s >= 0.25: return best_v
    raise HTTPException(404, f"Fund not found: '{fund}' — try /search?q=...")

@app.get("/search")
async def search(q: str = Query(..., min_length=2)):
    qn = norm(q); qw = set(qn.split()); res = []
    for k, v in holdings_db.items():
        kw = set(k.split())
        s  = len(qw & kw) / max(len(qw), len(kw), 1)
        if qn in k: s += 0.6
        if k in qn: s += 0.4
        if s > 0:
            res.append({"score": round(s,2), "name": v["fund_name"],
                        "amc": v["amc"], "key": k, "count": v["count"]})
    res.sort(key=lambda x: -x["score"])
    return {"query": q, "results": res[:15]}

@app.post("/upload")
async def upload_excel(
    file:   UploadFile = File(...),
    amc:    str        = Form(...),
    secret: str        = Form(default=""),
):
    """
    Upload an AMC portfolio Excel.
    Form fields: file (xlsx), amc (AMC name), secret (UPLOAD_SECRET env var).
    """
    check_secret(secret)
    if not file.filename.endswith(('.xlsx', '.xls', '.csv')):
        raise HTTPException(400, "Only .xlsx / .xls files accepted")

    raw     = await file.read()
    parsed  = parse_excel(raw, amc.strip())
    if not parsed:
        raise HTTPException(422, "No fund data found — check the Excel format")

    holdings_db.update(parsed)
    save_db()
    log.info(f"Uploaded {len(parsed)} funds for {amc}")
    return {
        "status":       "ok",
        "amc":          amc,
        "funds_added":  len(parsed),
        "funds_total":  len(holdings_db),
        "funds":        [{"name": v["fund_name"], "count": v["count"]} for v in parsed.values()],
    }

@app.delete("/fund")
async def delete_fund(key: str, secret: str = ""):
    check_secret(secret)
    if key not in holdings_db:
        raise HTTPException(404, f"Key not found: {key}")
    del holdings_db[key]
    save_db()
    return {"status": "deleted", "key": key, "funds_remaining": len(holdings_db)}

@app.delete("/amc")
async def delete_amc(amc: str, secret: str = ""):
    check_secret(secret)
    keys = [k for k,v in holdings_db.items() if v.get("amc","").lower() == amc.lower()]
    for k in keys: del holdings_db[k]
    save_db()
    return {"status": "deleted", "amc": amc, "removed": len(keys), "funds_remaining": len(holdings_db)}

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0",
                port=int(os.environ.get("PORT", 8000)), reload=False)
