"""
MF Holdings API — serves Indian mutual fund portfolio holdings as JSON.
Uses direct verified Excel URLs per AMC. Auto-refreshes monthly.
Deploy on Render.com (free tier).
"""

import os, re, io, logging, asyncio
from datetime import datetime, timedelta
from typing import Optional
import calendar

import httpx
import openpyxl
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

app = FastAPI(title="MF Holdings API", version="2.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["GET","POST"], allow_headers=["*"])

# ─────────────────────────────────────────────────────────────────────────────
# URL BUILDER — generates candidate URLs for last 3 months
# ─────────────────────────────────────────────────────────────────────────────
def last_day(year, month):
    return calendar.monthrange(year, month)[1]

def month_vars(dt: datetime):
    ld = last_day(dt.year, dt.month)
    return dict(
        YYYY=dt.strftime("%Y"), YY=dt.strftime("%y"),
        MM=dt.strftime("%m"), M=str(dt.month),
        Mon=dt.strftime("%b"), MON=dt.strftime("%b").upper(), mon=dt.strftime("%b").lower(),
        Month=dt.strftime("%B"), MONTH=dt.strftime("%B").upper(),
        MonYYYY=dt.strftime("%b%Y"), MMMYYYY=dt.strftime("%B%Y"),
        MonYY=dt.strftime("%b%y"), mon_YYYY=f"{dt.strftime('%b').lower()}{dt.year}",
        DD=f"{ld:02d}", D=str(ld),
        MonDD=f"{dt.strftime('%B')}_{ld}_{dt.year}",  # February_28_2026
    )

def candidates(patterns, months=3):
    urls = []
    dt = datetime.now()
    for _ in range(months):
        v = month_vars(dt)
        for p in patterns:
            try: urls.append(p.format(**v))
            except KeyError: pass
        dt = (dt.replace(day=1) - timedelta(days=1))
    return list(dict.fromkeys(urls))

# ─────────────────────────────────────────────────────────────────────────────
# AMC SOURCES — verified direct Excel URL patterns
# ─────────────────────────────────────────────────────────────────────────────
AMC_SOURCES = [
    # ── PPFAS (Parag Parikh) ─────────────────────────────────────────────────
    # Pattern: PPFAS_Monthly_Portfolio_Report_February_28_2026.xls
    {
        "name": "PPFAS",
        "patterns": [
            "https://amc.ppfas.com/downloads/portfolio-disclosure/{YYYY}/PPFAS_Monthly_Portfolio_Report_{MonDD}.xls",
            "https://amc.ppfas.com/downloads/portfolio-disclosure/{YYYY}/PPFAS_Monthly_Portfolio_Report_{MonDD}.xlsx",
        ],
    },
    # ── Canara Robeco ────────────────────────────────────────────────────────
    {
        "name": "Canara Robeco",
        "patterns": [
            "https://www.canararobeco.com/documents/statutory-disclosures/scheme-dashboard/scheme-monthly-portfolio/{YYYY}/{mon}-{YYYY}.xlsx",
            "https://www.canararobeco.com/documents/statutory-disclosures/scheme-dashboard/scheme-monthly-portfolio/{YYYY}/{Mon}-{YYYY}.xlsx",
            "https://www.canararobeco.com/documents/statutory-disclosures/scheme-dashboard/scheme-monthly-portfolio/{YYYY}/{MON}-{YYYY}.xlsx",
        ],
    },
    # ── UTI ─────────────────────────────────────────────────────────────────
    {
        "name": "UTI",
        "patterns": [
            "https://www.utimf.com/siteassets/downloads/statutory-disclosures/portfolio-disclosure/{YYYY}/{mon}-{YYYY}.xlsx",
            "https://www.utimf.com/siteassets/downloads/statutory-disclosures/portfolio-disclosure/{YYYY}/{Mon}-{YYYY}.xlsx",
        ],
    },
    # ── Kotak ────────────────────────────────────────────────────────────────
    {
        "name": "Kotak",
        "patterns": [
            "https://www.kotakmf.com/content/dam/kotakmf/downloads/portfolios/{YYYY}/{Mon}-{YYYY}-Portfolio.xlsx",
            "https://www.kotakmf.com/content/dam/kotakmf/downloads/portfolios/{YYYY}/Kotak-Portfolio-{Mon}-{YYYY}.xlsx",
            "https://www.kotakmf.com/content/dam/kotakmf/downloads/portfolios/Kotak-Portfolio-{Mon}-{YYYY}.xlsx",
        ],
    },
    # ── Mirae Asset ──────────────────────────────────────────────────────────
    {
        "name": "Mirae Asset",
        "patterns": [
            "https://www.miraeassetmf.co.in/Uploads/PortfolioDisclosure/Mirae_Asset_Portfolio_{MonYYYY}.xlsx",
            "https://www.miraeassetmf.co.in/Uploads/PortfolioDisclosure/Mirae_Asset_Portfolio_{Mon}_{YYYY}.xlsx",
        ],
    },
    # ── DSP ─────────────────────────────────────────────────────────────────
    {
        "name": "DSP",
        "patterns": [
            "https://www.dspim.com/content/dam/dsp/pdf/portfolio/{YYYY}/DSP_Portfolio_{Mon}_{YYYY}.xlsx",
            "https://www.dspim.com/content/dam/dsp/pdf/portfolio/DSP_Portfolio_{Mon}_{YYYY}.xlsx",
        ],
    },
    # ── Motilal Oswal ────────────────────────────────────────────────────────
    {
        "name": "Motilal Oswal",
        "patterns": [
            "https://www.motilaloswalmf.com/webresources/downloads/Portfolio_{Mon}_{YYYY}.xlsx",
            "https://www.motilaloswalmf.com/webresources/downloads/MOAMC_Portfolio_{Mon}_{YYYY}.xlsx",
        ],
    },
    # ── Axis ─────────────────────────────────────────────────────────────────
    {
        "name": "Axis",
        "patterns": [
            "https://www.axismf.com/media/axismf/downloads/portfolio/Axis_MF_Portfolio_{Mon}_{YYYY}.xlsx",
            "https://www.axismf.com/media/axismf/downloads/portfolio/Axis-MF-Portfolio-{Mon}-{YYYY}.xlsx",
        ],
    },
    # ── Nippon ───────────────────────────────────────────────────────────────
    {
        "name": "Nippon",
        "scrape": "https://mf.nipponindiaim.com/investor-service/downloads/factsheet-portfolio-and-other-disclosures",
        "patterns": [
            "https://mf.nipponindiaim.com/InvestorServices/FactsheetsDocuments/NIMF-Portfolio-{Mon}-{YYYY}.xlsx",
            "https://mf.nipponindiaim.com/InvestorServices/FactsheetsDocuments/Nippon-India-MF-Portfolio-{Mon}-{YYYY}.xlsx",
        ],
    },
    # ── SBI ──────────────────────────────────────────────────────────────────
    {
        "name": "SBI",
        "patterns": [
            "https://www.sbimf.com/sites/default/files/portfolio-disclosures/SBI_MF_Portfolio_{Mon}_{YYYY}.xlsx",
            "https://www.sbimf.com/sites/default/files/portfolio-disclosures/SBI-Portfolio-{Mon}-{YYYY}.xlsx",
        ],
        "scrape": "https://www.sbimf.com/portfolios",
    },
    # ── HDFC ─────────────────────────────────────────────────────────────────
    {
        "name": "HDFC",
        "patterns": [
            "https://www.hdfcfund.com/content/dam/hdfcmf/pdf/monthly-portfolio/{YYYY}/HDFC-MF-Monthly-Portfolio-{Mon}-{YYYY}.xlsx",
            "https://www.hdfcfund.com/content/dam/hdfcmf/pdf/monthly-portfolio/{YYYY}/HDFC-MF-Portfolio-{Mon}-{YYYY}.xlsx",
            "https://www.hdfcfund.com/content/dam/hdfcmf/pdf/monthly-portfolio/HDFC-MF-Portfolio-{MonYYYY}.xlsx",
        ],
        "scrape": "https://www.hdfcfund.com/statutory-disclosure/portfolio/monthly-portfolio",
    },
    # ── ICICI Prudential ─────────────────────────────────────────────────────
    {
        "name": "ICICI Prudential",
        "patterns": [
            "https://www.icicipruamc.com/docs/default-source/monthly-portfolio/icici-prudential-mf-portfolio-{mon}-{YYYY}.xlsx",
            "https://www.icicipruamc.com/docs/default-source/monthly-portfolio/icici-pru-portfolio-{mon}-{YYYY}.xlsx",
        ],
        "scrape": "https://www.icicipruamc.com/downloads/others/monthly-portfolio-disclosures",
    },
    # ── Aditya Birla Sun Life ────────────────────────────────────────────────
    {
        "name": "Aditya Birla Sun Life",
        "patterns": [
            "https://mutualfund.adityabirlacapital.com/content/dam/abcmf/pdf/portfolio/{YYYY}/ABSL_MF_Portfolio_{Mon}_{YYYY}.xlsx",
        ],
        "scrape": "https://mutualfund.adityabirlacapital.com/downloads/portfolios",
    },
    # ── Franklin Templeton ───────────────────────────────────────────────────
    {
        "name": "Franklin Templeton",
        "patterns": [
            "https://www.franklintempletonindia.com/content/dam/ftindia/pdf/downloads/portfolio/{YYYY}/{Mon}_{YYYY}_Portfolio.xlsx",
            "https://www.franklintempletonindia.com/content/dam/ftindia/pdf/downloads/portfolio/{YYYY}/Franklin_Portfolio_{Mon}_{YYYY}.xlsx",
        ],
    },
    # ── Tata ─────────────────────────────────────────────────────────────────
    {
        "name": "Tata",
        "patterns": [
            "https://www.tatamutualfund.com/content/dam/tata/pdf/portfolio-disclosure/Tata_MF_Portfolio_{Mon}_{YYYY}.xlsx",
        ],
        "scrape": "https://www.tatamutualfund.com/statutory-disclosures/portfolio-disclosures",
    },
    # ── Bandhan ──────────────────────────────────────────────────────────────
    {
        "name": "Bandhan",
        "patterns": [
            "https://www.bandhanmf.com/content/dam/bandhanmf/pdf/portfolio/Bandhan_MF_Portfolio_{Mon}_{YYYY}.xlsx",
        ],
        "scrape": "https://www.bandhanmf.com/downloads/monthly-portfolio",
    },
    # ── WhiteOak Capital ─────────────────────────────────────────────────────
    {
        "name": "WhiteOak Capital",
        "patterns": [
            "https://www.whiteoakcapital.com/wp-content/uploads/{YYYY}/{MM}/WhiteOak-Capital-MF-Portfolio-{Mon}-{YYYY}.xlsx",
        ],
        "scrape": "https://www.whiteoakcapital.com/mutual-fund/downloads",
    },
    # ── Groww ────────────────────────────────────────────────────────────────
    {
        "name": "Groww",
        "scrape": "https://www.growwmf.in/downloads",
    },
    # ── Quant ────────────────────────────────────────────────────────────────
    {
        "name": "Quant",
        "scrape": "https://www.quantmutual.com/statutory-disclosures",
    },
]

# ─────────────────────────────────────────────────────────────────────────────
# STATE
# ─────────────────────────────────────────────────────────────────────────────
holdings_db: dict = {}
last_refresh: Optional[datetime] = None
refresh_lock = asyncio.Lock()

# ─────────────────────────────────────────────────────────────────────────────
# NORMALISATION
# ─────────────────────────────────────────────────────────────────────────────
def norm(s: str) -> str:
    n = str(s).lower().strip()
    n = re.sub(r'\s*-?\s*(direct|regular)\s*plan.*$', '', n, flags=re.I)
    n = re.sub(r'\s*-?\s*(growth|idcw|dividend)\s*$', '', n, flags=re.I)
    n = re.sub(r'\s*\(g\)\s*$|\s*\(d\)\s*$', '', n, flags=re.I)
    return re.sub(r'\s+', ' ', n).strip()

# ─────────────────────────────────────────────────────────────────────────────
# EXCEL PARSER
# ─────────────────────────────────────────────────────────────────────────────
SKIP = re.compile(
    r'^(equity$|cash$|grand total|no\.?\s*of\s*stocks|large\s*cap$|mid\s*cap$|'
    r'small\s*cap$|mf\s*/\s*etf|fixed\s*income|mutual\s*fund\s*units|derivatives|'
    r'total$|sub.?total|net\s*equity|net\s*receivable|cblo|repo|treps|scheme\s*name|'
    r'riskometer|past\s*performance|portfolio\s*as\s*on)',
    re.I
)

def parse_excel(raw: bytes, amc: str) -> dict:
    out = {}
    try:
        wb = openpyxl.load_workbook(io.BytesIO(raw), read_only=True, data_only=True)
    except Exception as e:
        log.warning(f"[{amc}] openpyxl: {e}"); return out

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

        # Detect header row (has ISIN column)
        hrow = ncol = icol = scol = pcol = -1
        for i, row in enumerate(rows):
            cl = [str(c or '').lower().strip() for c in row]
            if 'isin' not in cl: continue
            hrow = i
            icol = cl.index('isin')
            ncol = next((j for j,c in enumerate(cl)
                         if c in ('name','instrument','security','company','name of instrument')),
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
            name   = str(row[ncol]  or '').strip()
            isin   = str(row[icol]  or '').strip() if icol >= 0 else ''
            sector = str(row[scol]  or '').strip() if scol >= 0 else ''
            rpct   = row[pcol]
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
                out[key] = {"fund_name": fname.strip(), "amc": amc,
                            "holdings": holdings, "count": len(holdings)}

    wb.close()
    return out

# ─────────────────────────────────────────────────────────────────────────────
# HTTP
# ─────────────────────────────────────────────────────────────────────────────
HDR = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "Chrome/122.0.0.0 Safari/537.36",
    "Accept": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,"
              "application/vnd.ms-excel,text/html,*/*",
}

async def get(url, timeout=40):
    for i in range(3):
        try:
            async with httpx.AsyncClient(follow_redirects=True, timeout=timeout,
                                          headers=HDR, verify=False) as c:
                r = await c.get(url)
                if r.status_code == 200 and len(r.content) > 2000:
                    return r.content
                log.debug(f"HTTP {r.status_code} {url}")
        except Exception as e:
            log.debug(f"Try {i+1} failed {url}: {e}")
        await asyncio.sleep(1.5 * (i+1))
    return None

async def scrape_xlsx(page_url):
    raw = await get(page_url, timeout=20)
    if not raw: return None
    html = raw.decode('utf-8', errors='ignore')
    links = re.findall(r'https?://[^\s"\'<>]+\.xlsx(?:\?[^\s"\'<>]*)?', html, re.I)
    rel   = re.findall(r'(?:href|src)=["\']([^"\']+\.xlsx[^"\']*)["\']', html, re.I)
    base  = '/'.join(page_url.split('/')[:3])
    links += [l if l.startswith('http') else base+('/'+l.lstrip('/')) for l in rel]
    if not links: return None
    yr, pm = str(datetime.now().year), (datetime.now().replace(day=1)-timedelta(days=1)).strftime('%b').lower()
    cm     = datetime.now().strftime('%b').lower()
    links  = list(dict.fromkeys(links))
    links.sort(key=lambda l:(10*(yr in l)+3*(cm in l.lower())+2*(pm in l.lower())), reverse=True)
    return links[0]

# ─────────────────────────────────────────────────────────────────────────────
# REFRESH
# ─────────────────────────────────────────────────────────────────────────────
async def fetch_amc(src):
    name = src["name"]
    raw  = None

    # Try direct URL patterns first
    for url in candidates(src.get("patterns", []), months=3):
        log.debug(f"[{name}] trying {url}")
        data = await get(url, timeout=45)
        if data:
            raw = data
            log.info(f"[{name}] ✓ direct {url}")
            break

    # Fall back to page scrape
    if raw is None and src.get("scrape"):
        xl_url = await scrape_xlsx(src["scrape"])
        if xl_url:
            data = await get(xl_url, timeout=60)
            if data:
                raw = data
                log.info(f"[{name}] ✓ scraped {xl_url}")

    if raw is None:
        log.warning(f"[{name}] ✗ not found"); return {}

    result = parse_excel(raw, name)
    log.info(f"[{name}] → {len(result)} funds")
    return result

async def refresh_all():
    global last_refresh
    async with refresh_lock:
        log.info("=== Refresh starting ===")
        results = await asyncio.gather(*[fetch_amc(s) for s in AMC_SOURCES],
                                        return_exceptions=True)
        new_db = {}
        for src, res in zip(AMC_SOURCES, results):
            if isinstance(res, Exception):
                log.error(f"[{src['name']}] {res}")
            elif res:
                new_db.update(res)
        holdings_db.clear(); holdings_db.update(new_db)
        last_refresh = datetime.utcnow()
        log.info(f"=== Done: {len(holdings_db)} funds ===")

async def scheduler():
    await refresh_all()
    while True:
        now = datetime.utcnow()
        nxt = now.replace(day=10, hour=3, minute=30, second=0, microsecond=0)
        if nxt.month == 12:
            nxt = nxt.replace(year=nxt.year+1, month=1)
        else:
            nxt = nxt.replace(month=nxt.month+1)
        if nxt <= now: nxt = nxt.replace(month=nxt.month+1 if nxt.month<12 else 1)
        await asyncio.sleep((nxt-now).total_seconds())
        await refresh_all()

@app.on_event("startup")
async def startup(): asyncio.create_task(scheduler())

# ─────────────────────────────────────────────────────────────────────────────
# ROUTES
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/")
async def root():
    return {"service":"MF Holdings API","funds":len(holdings_db),
            "last_refresh":last_refresh.isoformat() if last_refresh else None}

@app.get("/health")
async def health():
    return {"status":"ok","funds":len(holdings_db),
            "last_refresh":last_refresh.isoformat() if last_refresh else None}

@app.get("/funds")
async def list_funds(amc: Optional[str]=None):
    out = [{"name":v["fund_name"],"amc":v["amc"],"key":k,"count":v["count"]}
           for k,v in holdings_db.items()
           if not amc or amc.lower() in v.get("amc","").lower()]
    return {"total":len(out),"funds":sorted(out,key=lambda x:(x["amc"],x["name"]))}

@app.get("/holdings")
async def holdings(fund: str = Query(..., min_length=2)):
    if not holdings_db: raise HTTPException(503,"Loading, retry in 60s")
    q = norm(fund)
    if q in holdings_db: return holdings_db[q]
    qw = set(q.split())
    best_s, best_v = 0.0, None
    for k,v in holdings_db.items():
        ov = len(qw & set(k.split()))
        s  = ov / max(len(qw),len(k.split()),1)
        if q in k: s+=0.6
        if k in q: s+=0.4
        if s > best_s: best_s,best_v = s,v
    if best_v and best_s >= 0.3: return best_v
    raise HTTPException(404, f"Not found: '{fund}' — try /search?q=...")

@app.get("/search")
async def search(q: str = Query(..., min_length=2)):
    qn = norm(q); qw = set(qn.split()); res = []
    for k,v in holdings_db.items():
        ov = len(qw & set(k.split()))
        s  = ov/max(len(qw),len(k.split()),1)
        if qn in k: s+=0.6
        if k in qn: s+=0.4
        if s>0: res.append({"score":round(s,2),"name":v["fund_name"],"amc":v["amc"],"key":k,"count":v["count"]})
    res.sort(key=lambda x:-x["score"])
    return {"query":q,"results":res[:15]}

@app.post("/refresh")
async def manual_refresh(secret: str=""):
    exp = os.environ.get("REFRESH_SECRET","")
    if exp and secret!=exp: raise HTTPException(403,"Invalid secret")
    asyncio.create_task(refresh_all())
    return {"status":"refresh started","current_funds":len(holdings_db)}

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0",
                port=int(os.environ.get("PORT",8000)), reload=False)
