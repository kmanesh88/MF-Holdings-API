"""
MF Holdings API — serves parsed Indian mutual fund portfolio holdings as JSON.
Sources: AMFI-mandated monthly Excel disclosures from each AMC.
Auto-refreshes on the 10th of each month (after AMCs publish disclosures).
Deploy free on Render.com or Railway.app.
"""

import os, re, io, logging, asyncio, hashlib
from datetime import datetime, timedelta
from typing import Optional
from functools import lru_cache

import httpx
import openpyxl
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import uvicorn

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

app = FastAPI(title="MF Holdings API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# ── AMC Excel disclosure URLs ─────────────────────────────────────────────────
# SEBI mandates monthly disclosure by 10th of following month.
# Format: each Excel has sheets per fund, rows = holdings, % to AUM column.
# Updated as AMCs change their URL patterns.
AMC_SOURCES = [
    {
        "name": "SBI",
        "url": "https://www.sbimf.com/api/PortfolioDisclosure/GetPortfolioDisclosureList",
        "type": "api",          # SBI has an API endpoint
        "api_key": None,
    },
    {
        "name": "HDFC",
        "url": "https://www.hdfcfund.com/content/dam/abc/india/assets/statutory-disclosure/portfolio/monthly-portfolio/HDFC-MF-Portfolio-{month_year}.xlsx",
        "type": "excel_pattern",
    },
    {
        "name": "ICICI Prudential",
        "url": "https://www.icicipruamc.com/downloads/others/monthly-portfolio-disclosures",
        "type": "scrape_latest", # scrape page to find latest Excel link
    },
    {
        "name": "Nippon",
        "url": "https://mf.nipponindiaim.com/investor-service/downloads/factsheet-portfolio-and-other-disclosures",
        "type": "scrape_latest",
    },
    {
        "name": "Kotak",
        "url": "https://www.kotakmf.com/Information/portfolios",
        "type": "scrape_latest",
    },
    {
        "name": "Mirae Asset",
        "url": "https://www.miraeassetmf.co.in/downloads/monthly-portfolio",
        "type": "scrape_latest",
    },
    {
        "name": "Axis",
        "url": "https://www.axismf.com/downloads/monthly-portfolio",
        "type": "scrape_latest",
    },
    {
        "name": "UTI",
        "url": "https://www.utimf.com/downloads/portfolio-disclosure-scheme-wise",
        "type": "scrape_latest",
    },
    {
        "name": "Aditya Birla Sun Life",
        "url": "https://mutualfund.adityabirlacapital.com/downloads/portfolios",
        "type": "scrape_latest",
    },
    {
        "name": "DSP",
        "url": "https://www.dspim.com/downloads/portfolios",
        "type": "scrape_latest",
    },
    {
        "name": "Franklin Templeton",
        "url": "https://www.franklintempletonindia.com/investor/tools-and-resources/portfolio-of-all-schemes",
        "type": "scrape_latest",
    },
    {
        "name": "Tata",
        "url": "https://www.tatamutualfund.com/statutory-disclosures/portfolio-disclosures",
        "type": "scrape_latest",
    },
    {
        "name": "Motilal Oswal",
        "url": "https://www.motilaloswalmf.com/mf/downloads/monthly-portfolio",
        "type": "scrape_latest",
    },
    {
        "name": "Canara Robeco",
        "url": "https://www.canararobeco.com/documents/statutory-disclosures/scheme-dashboard/scheme-monthly-portfolio/",
        "type": "scrape_latest",
    },
    {
        "name": "PPFAS",
        "url": "https://amc.ppfas.com/downloads/portfolio-disclosure/",
        "type": "scrape_latest",
    },
    {
        "name": "Quant",
        "url": "https://www.quantmutual.com/downloads",
        "type": "scrape_latest",
    },
    {
        "name": "WhiteOak Capital",
        "url": "https://www.whiteoakcapital.com/mutual-fund/downloads",
        "type": "scrape_latest",
    },
    # Sundaram: no consolidated Excel — uses PDF only, excluded from auto-fetch
]

# ── In-memory store ────────────────────────────────────────────────────────────
# { normalized_fund_name: { holdings: [...], amc: str, refreshed_at: str } }
holdings_db: dict = {}
last_refresh: Optional[datetime] = None
refresh_lock = asyncio.Lock()

# ── Fund name normalisation ────────────────────────────────────────────────────
def norm(name: str) -> str:
    """Lowercase, strip suffixes, collapse spaces. Mirrors client-side normStockKey."""
    n = name.lower().strip()
    n = re.sub(r'\s*-?\s*(direct|regular)\s*plan.*$', '', n, flags=re.I)
    n = re.sub(r'\s*-?\s*(growth|idcw|dividend)\s*$', '', n, flags=re.I)
    n = re.sub(r'\s*\(g\)\s*$', '', n, flags=re.I)
    n = re.sub(r'\s*\(d\)\s*$', '', n, flags=re.I)
    n = re.sub(r'\s+', ' ', n).strip()
    return n

# ── Excel parser ───────────────────────────────────────────────────────────────
def parse_excel_bytes(raw: bytes, amc_name: str) -> dict:
    """
    Parse SEBI-format monthly portfolio Excel.
    Returns { fund_name: { holdings: [{name, isin, sector, pct}], large, mid, small, ... } }
    """
    results = {}
    try:
        wb = openpyxl.load_workbook(io.BytesIO(raw), read_only=True, data_only=True)
    except Exception as e:
        log.warning(f"[{amc_name}] openpyxl load failed: {e}")
        return results

    for sheet_name in wb.sheetnames:
        if re.search(r'index|cover|content|summary', sheet_name, re.I):
            continue
        ws = wb[sheet_name]
        rows = list(ws.iter_rows(values_only=True))
        if len(rows) < 5:
            continue

        # Detect fund name (row with "SCHEME NAME :" prefix or long fund name)
        fund_name = sheet_name
        for r in rows[:10]:
            c0 = str(r[0] or '').strip()
            c1 = str(r[1] or '').strip() if len(r) > 1 else ''
            if re.search(r'scheme\s*name', c0, re.I) and len(c1) > 3:
                fund_name = c1; break
            if len(c1) > 10 and re.search(r'fund|scheme', c1, re.I) and not re.search(r'mutual fund$', c1, re.I):
                fund_name = c1; break
            c0_full = c0
            if re.search(r'scheme\s*name\s*:', c0_full, re.I):
                name_part = re.sub(r'scheme\s*name\s*:\s*', '', c0_full, flags=re.I).strip()
                if len(name_part) > 3:
                    fund_name = name_part; break

        # Find header row (has ISIN column)
        header_row = -1
        name_col = isin_col = sector_col = pct_col = -1
        for i, row in enumerate(rows):
            cells = [str(c or '').lower().strip() for c in row]
            if 'isin' in cells:
                header_row = i
                name_col   = next((j for j, c in enumerate(cells) if c in ('name', 'instrument', 'security', 'company')), -1)
                if name_col < 0:
                    name_col = next((j for j, c in enumerate(cells) if 'name' in c or 'instrument' in c), -1)
                isin_col   = cells.index('isin')
                sector_col = next((j for j, c in enumerate(cells) if 'sector' in c or 'industry' in c), -1)
                pct_col    = next((j for j, c in enumerate(cells) if '% to' in c or '% of' in c or 'nav%' in c or '% aum' in c), -1)
                if pct_col < 0:
                    # Try last numeric-looking column
                    for j in range(len(cells)-1, -1, -1):
                        if re.search(r'%|aum|nav|weight', cells[j]):
                            pct_col = j; break
                break

        if header_row < 0 or name_col < 0:
            continue

        holdings = []
        skip = re.compile(
            r'^(equity|cash|grand total|no\.?\s*of|large cap|mid cap|small cap|'
            r'mf/etf|fixed income|mutual fund units|derivatives|government|corporate bond|'
            r'unlisted|total|sub.?total|net equity|net receivable|cblo|repo|treps)',
            re.I
        )

        for row in rows[header_row + 1:]:
            if len(row) <= max(name_col, isin_col if isin_col >= 0 else 0):
                continue
            name   = str(row[name_col] or '').strip()
            isin   = str(row[isin_col] or '').strip() if isin_col >= 0 else ''
            sector = str(row[sector_col] or '').strip() if sector_col >= 0 else ''
            pct    = row[pct_col] if pct_col >= 0 and pct_col < len(row) else None

            if not name or len(name) < 3 or skip.match(name):
                continue
            # Must have a valid ISIN or a numeric percentage
            if not re.match(r'INE[A-Z0-9]{9}\d', isin) and pct is None:
                continue
            try:
                pct_val = float(str(pct).replace('%','').replace(',','').strip()) if pct else 0.0
            except (ValueError, TypeError):
                pct_val = 0.0
            if pct_val <= 0:
                continue

            holdings.append({
                "name":   name,
                "isin":   isin,
                "sector": sector,
                "pct":    round(pct_val, 4),
            })

        if holdings:
            clean_name = norm(fund_name)
            results[clean_name] = {
                "fund_name": fund_name,
                "amc":       amc_name,
                "holdings":  holdings,
                "count":     len(holdings),
            }

    wb.close()
    return results

# ── HTTP fetch with retry ──────────────────────────────────────────────────────
async def fetch_url(url: str, timeout: int = 30) -> Optional[bytes]:
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; MF-Holdings-API/1.0; +https://github.com/kmanesh88/MF-Analyser)",
        "Accept": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,*/*",
    }
    for attempt in range(3):
        try:
            async with httpx.AsyncClient(follow_redirects=True, timeout=timeout) as client:
                r = await client.get(url, headers=headers)
                if r.status_code == 200:
                    return r.content
                log.warning(f"HTTP {r.status_code} for {url}")
        except Exception as e:
            log.warning(f"Attempt {attempt+1} failed for {url}: {e}")
            await asyncio.sleep(2 ** attempt)
    return None

# ── Scrape page for latest Excel link ─────────────────────────────────────────
async def scrape_latest_excel_url(page_url: str) -> Optional[str]:
    """Find the most recent .xlsx link on a disclosure page."""
    raw = await fetch_url(page_url, timeout=20)
    if not raw:
        return None
    html = raw.decode('utf-8', errors='ignore')
    # Find all xlsx links
    links = re.findall(r'https?://[^\s"\'<>]+\.xlsx', html, re.I)
    if not links:
        # Try relative links
        links = re.findall(r'href=["\']([^"\']+\.xlsx)["\']', html, re.I)
        base = '/'.join(page_url.split('/')[:3])
        links = [l if l.startswith('http') else base + l for l in links]
    if not links:
        return None
    # Prefer links with current/recent year
    year = str(datetime.now().year)
    prev_year = str(datetime.now().year - 1)
    month_names = ['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']
    current_month = month_names[datetime.now().month - 1]
    prev_month    = month_names[(datetime.now().month - 2) % 12]

    def score(link):
        l = link.lower()
        s = 0
        if year in l: s += 10
        if prev_year in l: s += 5
        if current_month in l: s += 3
        if prev_month in l: s += 2
        if 'portfolio' in l: s += 1
        return s

    links.sort(key=score, reverse=True)
    return links[0]

# ── Refresh all AMC data ──────────────────────────────────────────────────────
async def refresh_all():
    global last_refresh
    async with refresh_lock:
        log.info("Starting holdings refresh for all AMCs...")
        new_db = {}
        tasks = [fetch_and_parse(src) for src in AMC_SOURCES]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for src, result in zip(AMC_SOURCES, results):
            if isinstance(result, Exception):
                log.error(f"[{src['name']}] refresh failed: {result}")
                continue
            if result:
                new_db.update(result)
                log.info(f"[{src['name']}] loaded {len(result)} funds")
        holdings_db.clear()
        holdings_db.update(new_db)
        last_refresh = datetime.utcnow()
        log.info(f"Refresh complete. Total funds: {len(holdings_db)}")

async def fetch_and_parse(src: dict) -> dict:
    amc_name = src["name"]
    src_type = src.get("type", "scrape_latest")

    if src_type == "scrape_latest":
        excel_url = await scrape_latest_excel_url(src["url"])
        if not excel_url:
            log.warning(f"[{amc_name}] could not find Excel URL on {src['url']}")
            return {}
        log.info(f"[{amc_name}] found Excel: {excel_url}")
        raw = await fetch_url(excel_url, timeout=60)
        if not raw:
            log.warning(f"[{amc_name}] Excel download failed")
            return {}
        return parse_excel_bytes(raw, amc_name)

    elif src_type == "excel_pattern":
        # Build URL from pattern
        now = datetime.now()
        month_year = now.strftime("%b%Y")  # e.g. "Mar2025"
        url = src["url"].format(month_year=month_year)
        raw = await fetch_url(url, timeout=60)
        if not raw:
            # Try previous month
            prev = now.replace(day=1) - timedelta(days=1)
            url = src["url"].format(month_year=prev.strftime("%b%Y"))
            raw = await fetch_url(url, timeout=60)
        if not raw:
            return {}
        return parse_excel_bytes(raw, amc_name)

    return {}

# ── Scheduled auto-refresh ─────────────────────────────────────────────────────
async def scheduler():
    """Refresh on startup, then on 10th of each month at 09:00 IST."""
    await refresh_all()
    while True:
        now = datetime.utcnow()
        # Next 10th at 03:30 UTC (= 09:00 IST)
        next_10th = now.replace(day=10, hour=3, minute=30, second=0, microsecond=0)
        if now >= next_10th:
            if now.month == 12:
                next_10th = next_10th.replace(year=now.year+1, month=1)
            else:
                next_10th = next_10th.replace(month=now.month+1)
        wait_secs = (next_10th - now).total_seconds()
        log.info(f"Next refresh scheduled in {wait_secs/3600:.1f} hours")
        await asyncio.sleep(wait_secs)
        await refresh_all()

@app.on_event("startup")
async def startup():
    asyncio.create_task(scheduler())

# ── API Endpoints ──────────────────────────────────────────────────────────────
@app.get("/")
async def root():
    return {
        "service": "MF Holdings API",
        "funds_loaded": len(holdings_db),
        "last_refresh": last_refresh.isoformat() if last_refresh else None,
        "endpoints": {
            "/holdings": "GET ?fund=<name> → stock holdings for a fund",
            "/search":   "GET ?q=<query> → fuzzy search fund names",
            "/funds":    "GET → list all loaded fund names",
            "/refresh":  "POST → trigger manual refresh (admin)",
            "/health":   "GET → service health check",
        }
    }

@app.get("/health")
async def health():
    return {"status": "ok", "funds": len(holdings_db), "last_refresh": last_refresh.isoformat() if last_refresh else None}

@app.get("/funds")
async def list_funds(amc: Optional[str] = None):
    """List all loaded fund names, optionally filtered by AMC."""
    funds = []
    for key, val in holdings_db.items():
        if amc and amc.lower() not in val.get("amc", "").lower():
            continue
        funds.append({"name": val["fund_name"], "amc": val["amc"], "key": key, "holdings_count": val["count"]})
    funds.sort(key=lambda x: x["amc"])
    return {"total": len(funds), "funds": funds}

@app.get("/holdings")
async def get_holdings(fund: str = Query(..., description="Fund name (partial match OK)")):
    """
    Get stock holdings for a mutual fund by name.
    Returns: { fund_name, amc, holdings: [{name, isin, sector, pct}] }
    Fuzzy match — partial names work e.g. 'Sundaram Mid Cap' or 'HDFC Top 100'
    """
    if not holdings_db:
        raise HTTPException(503, "Holdings data not yet loaded. Try again in 60s.")

    query = norm(fund)

    # 1. Exact key match
    if query in holdings_db:
        return holdings_db[query]

    # 2. Substring match (query is substring of key or vice versa)
    candidates = []
    for key, val in holdings_db.items():
        # Score: how much of the query matches
        if query in key:
            candidates.append((len(query) / len(key), key, val))
        elif key in query:
            candidates.append((len(key) / len(query), key, val))
        else:
            # Word overlap
            q_words = set(query.split())
            k_words = set(key.split())
            overlap = len(q_words & k_words)
            if overlap >= min(2, len(q_words)):
                candidates.append((overlap / max(len(q_words), len(k_words)), key, val))

    if candidates:
        candidates.sort(key=lambda x: -x[0])
        _, best_key, best_val = candidates[0]
        return best_val

    raise HTTPException(404, f"Fund not found: '{fund}'. Use /search?q=<query> to find available funds.")

@app.get("/search")
async def search_funds(q: str = Query(..., min_length=3)):
    """Fuzzy search for fund names. Returns top 10 matches."""
    query = norm(q)
    q_words = set(query.split())

    results = []
    for key, val in holdings_db.items():
        k_words = set(key.split())
        overlap  = len(q_words & k_words)
        if overlap > 0 or query in key or key in query:
            score = overlap / max(len(q_words), len(k_words))
            if query in key:
                score += 0.5
            results.append({
                "score":   round(score, 3),
                "name":    val["fund_name"],
                "amc":     val["amc"],
                "key":     key,
                "count":   val["count"],
            })

    results.sort(key=lambda x: -x["score"])
    return {"query": q, "results": results[:10]}

@app.post("/refresh")
async def manual_refresh(secret: str = Query(default="")):
    """Trigger a manual data refresh. Requires REFRESH_SECRET env var to match."""
    expected = os.environ.get("REFRESH_SECRET", "")
    if expected and secret != expected:
        raise HTTPException(403, "Invalid secret")
    asyncio.create_task(refresh_all())
    return {"status": "refresh started", "funds_before": len(holdings_db)}

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.environ.get("PORT", 8000)), reload=False)
