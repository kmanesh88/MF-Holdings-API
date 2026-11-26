"""
Vantage · Alerts — advisor-facing proactive alerts. Nothing here contacts clients.

Phase 1 (this file): price-move alerts.
  1. Exposure:  client_holdings (₹ per fund) × holdings_db factsheets, looked through FoFs
                → client_exposure/{clientId}   (stock / country / geo / Indian cap, ₹ and %)
  2. Moves:     daily close-to-close moves (Yahoo chart API) for every security where at least one
                client has material exposure, plus indices and USD/INR
  3. Alerts:    advisor_alerts/{id}  (one per event × client; idempotent ids, status new|called|dismissed)
Phase 2 (next): event news (SEBI / RBI / BSE filings / SEC EDGAR / fixed RSS) on the same alert schema.

Schedule (GitHub Actions, .github/workflows/alerts-scan.yml):
  07:30 IST Tue–Sat  POST /alerts/scan?market=us   (after the US close)
  16:00 IST Mon–Fri  POST /alerts/scan?market=in   (after the Indian close, includes USD/INR)

Auth: advisor = Firebase ID token (Authorization: Bearer); scheduler = X-Agent-Secret
      (ALERTS_AGENT_SECRET → TAX_AGENT_SECRET → UPLOAD_SECRET).
"""
from __future__ import annotations

import asyncio
import datetime as dt
import importlib
import re
import logging
import os
import time
import uuid
from typing import Dict, List, Optional
from urllib.parse import quote

import httpx
from fastapi import APIRouter, BackgroundTasks, Depends, Header, HTTPException, Query
from pydantic import BaseModel

import alerts_engine as E

log = logging.getLogger("alerts")
router = APIRouter(prefix="/alerts", tags=["alerts"])

DEFAULT_CONFIG = {
    "stock_move_pct": 5.0,       # single stock, close-to-close
    "index_move_pct": 2.0,
    "fx_move_pct": 1.0,          # USD/INR
    "min_exposure_pct": 1.0,     # material if ≥ this % of the client's portfolio …
    "min_exposure_inr": 200000,  # … or ≥ this many rupees
}
INDICES = {
    "in": [
        {"key": "NIFTY50", "symbol": "^NSEI", "name": "Nifty 50", "scope": {"geo": "IN"}},
        {"key": "NIFTYMID150", "symbol": "NIFTY_MIDCAP_150.NS", "name": "Nifty Midcap 150", "scope": {"cap": "mid"}},
        {"key": "NIFTYSML250", "symbol": "NIFTY_SMLCAP_250.NS", "name": "Nifty Smallcap 250", "scope": {"cap": "small"}},
    ],
    "us": [
        {"key": "SP500", "symbol": "^GSPC", "name": "S&P 500", "scope": {"country": "US"}},
        {"key": "NDX", "symbol": "^NDX", "name": "Nasdaq 100", "scope": {"country": "US"}},
    ],
}
FX = {"key": "USDINR", "symbol": "USDINR=X", "name": "USD/INR"}
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"}
US_EXCH = {"NMS", "NYQ", "NGM", "NCM", "ASE", "PCX", "BTS", "NAS", "NYS"}


# ------------------------------------------------------------------ plumbing
def _main():
    return importlib.import_module("main")  # already loaded by uvicorn; lazy to avoid a circular import


def _db():
    db = _main()._get_firestore_client()
    if db is None:
        raise HTTPException(503, "Firestore not configured")
    return db


def _now():
    return dt.datetime.now(dt.timezone.utc)


def _advisor(authorization: Optional[str] = Header(None)) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "Sign in required")
    from firebase_admin import auth as fb_auth
    _db()
    try:
        return fb_auth.verify_id_token(authorization[7:])["uid"]
    except Exception:
        raise HTTPException(401, "Invalid token")


def _agent_or_advisor(authorization: Optional[str] = Header(None), x_agent_secret: Optional[str] = Header(None)) -> str:
    secret = os.environ.get("ALERTS_AGENT_SECRET") or os.environ.get("TAX_AGENT_SECRET") or os.environ.get("UPLOAD_SECRET")
    if x_agent_secret and secret and x_agent_secret == secret:
        return "scheduler"
    return _advisor(authorization)


def _config() -> dict:
    try:
        snap = _db().collection("alert_meta").document("config").get()
        return {**DEFAULT_CONFIG, **((snap.to_dict() or {}).get("thresholds") or {})} if snap.exists else dict(DEFAULT_CONFIG)
    except HTTPException:
        raise
    except Exception as e:
        log.warning("alert config unavailable: %s", e)
        return dict(DEFAULT_CONFIG)


def _meta(doc: str) -> dict:
    snap = _db().collection("alert_meta").document(doc).get()
    return (snap.to_dict() or {}) if snap.exists else {}


# ------------------------------------------------------------------ fund resolution + look-through
def _score(qn: str, k: str) -> float:
    qw, kw = set(qn.split()), set(k.split())
    s = len(qw & kw) / max(min(len(qw), len(kw)), 1)
    if qn in k:
        s += 0.6
    if k in qn:
        s += 0.4
    return s


def resolve_fund(name: str, min_score: float = 0.4):
    """Same matching as GET /holdings: exact normalised key, else best word-overlap score."""
    M = _main()
    q = M.norm(name)
    if q in M.holdings_db:
        return q, 1.0
    best_k, best_s = None, 0.0
    for k in M.holdings_db:
        s = _score(q, k)
        if s > best_s:
            best_k, best_s = k, s
    return (best_k, best_s) if best_s >= min_score else (None, best_s)


_ETF_RE = re.compile(r"\betf\b|\bbees\b", re.I)


def _compatible(scheme: str, fund_name: str) -> bool:
    """Guards against the loose word-overlap matcher pairing the wrong vehicle: same AMC brand
    (first word), FoF only with FoF, ETF only with ETF."""
    a, b = E.norm_name(scheme).split(), E.norm_name(fund_name).split()
    if not a or not b or a[0] != b[0]:
        return False
    if bool(E.FOF_NAME_RE.search(scheme)) != bool(E.FOF_NAME_RE.search(fund_name)):
        return False
    return bool(_ETF_RE.search(scheme)) == bool(_ETF_RE.search(fund_name))


def resolve_client_fund(scheme: str):
    """Stricter than resolve_fund: a wrong match would put wrong ₹ figures in front of the advisor."""
    M = _main()
    q = M.norm(scheme)
    cands = sorted(((_score(q, k), k) for k in M.holdings_db), reverse=True)[:8]
    for s, k in cands:
        if s < 0.6:
            break
        if _compatible(scheme, M.holdings_db[k].get("fund_name", k)):
            return k, s
    return None, cands[0][0] if cands else 0.0


def _cap_of(isin: str) -> str:
    return _main().AMFI_ISIN_CAP.get(isin) or "small"


def _unit_resolver(links: Dict[str, Dict[str, str]]):
    """Manual links (alert_meta/fof_links) first, else a strict auto-match against holdings_db."""
    M = _main()

    def resolve(parent_key: str, unit_name: str) -> Optional[str]:
        manual = (links.get(parent_key) or {}).get(E.norm_name(unit_name))
        if manual:
            return manual if manual in M.holdings_db else None
        k, s = resolve_fund(unit_name, min_score=0.75)
        return k if k and k != parent_key else None
    return resolve


def _links() -> Dict[str, Dict[str, str]]:
    """alert_meta/fof_links.items = [{fof_key, unit, target}] → {fof_key: {norm(unit): target}}"""
    out: Dict[str, Dict[str, str]] = {}
    for r in _meta("fof_links").get("items", []):
        out.setdefault(r["fof_key"], {})[E.norm_name(r["unit"])] = r["target"]
    return out


def _profiles_and_lookthrough(keys, links):
    M = _main()
    res = _unit_resolver(links)
    profiles = {k: E.fund_profile(M.holdings_db[k]) for k in keys if k in M.holdings_db}
    lt = {k: E.look_through(k, M.holdings_db, res, _cap_of) for k in keys if k in M.holdings_db}
    return profiles, lt


# ------------------------------------------------------------------ exposure
def rebuild_exposures() -> dict:
    M = _main()
    db = _db()
    links = _links()
    clients = [d.to_dict() for d in db.collection("client_holdings").stream()]
    resolved_all, keys = [], set()
    for c in clients:
        rows = []
        for f in c.get("funds", []):
            k, s = resolve_client_fund(f.get("schemeName", ""))
            rows.append((f, k, s))
            if k:
                keys.add(k)
        resolved_all.append((c, rows))
    profiles, lt = _profiles_and_lookthrough(keys, links)
    built_at = _now().isoformat()
    n = 0
    for c, rows in resolved_all:
        cid = c.get("clientId")
        if not cid:
            continue
        ex = E.build_client_exposure(c, rows, M.holdings_db, lt, profiles)
        ex["built_at"] = built_at
        db.collection("client_exposure").document(cid).set(ex)
        n += 1
    db.collection("alert_meta").document("exposure").set({"built_at": built_at, "clients": n, "funds": len(keys)})
    return {"clients": n, "funds_resolved": len(keys), "built_at": built_at}


def _load_exposures(max_age_h: float = 20) -> List[dict]:
    meta = _meta("exposure")
    fresh = False
    if meta.get("built_at"):
        age = (_now() - dt.datetime.fromisoformat(meta["built_at"])).total_seconds() / 3600
        fresh = age < max_age_h
    if not fresh:
        rebuild_exposures()
    return [d.to_dict() for d in _db().collection("client_exposure").stream()]


# ------------------------------------------------------------------ securities → tickers
_amfi_sym: Dict[str, str] = {}


def _amfi_symbols() -> Dict[str, str]:
    """ISIN → NSE symbol from the bundled AMFI market-cap file (col C ISIN, col F NSE symbol)."""
    if _amfi_sym:
        return _amfi_sym
    try:
        import openpyxl
        wb = openpyxl.load_workbook("amfi_market_cap.xlsx", read_only=True, data_only=True)
        for row in wb.active.iter_rows(min_row=3, values_only=True):
            isin, nse = (row[2] or ""), (row[5] or "")
            if isin and nse:
                _amfi_sym[str(isin).strip()] = str(nse).strip()
        wb.close()
    except Exception as e:
        log.warning("AMFI symbol map load failed: %s", e)
    return _amfi_sym


async def _yahoo_search(client: httpx.AsyncClient, q: str, want_us: bool) -> Optional[dict]:
    try:
        r = await client.get("https://query2.finance.yahoo.com/v1/finance/search",
                             params={"q": q, "quotesCount": 6, "newsCount": 0}, headers=UA, timeout=10)
        if r.status_code != 200:
            return None
        quotes = [x for x in r.json().get("quotes", []) if x.get("quoteType") in ("EQUITY", "ETF") and x.get("symbol")]
        if want_us:
            quotes.sort(key=lambda x: 0 if x.get("exchange") in US_EXCH else 1)
        return quotes[0] if quotes else None
    except Exception:
        return None


async def _resolve_tickers(client, secs: Dict[str, dict]) -> Dict[str, str]:
    """sec_key → Yahoo symbol. Indian: AMFI NSE symbol + .NS. Foreign: cached Yahoo ISIN/name search.
    Manual overrides: alert_meta/tickers.map."""
    db = _db()
    tmeta = _meta("tickers")
    manual, cache = tmeta.get("map", {}), tmeta.get("auto", {})
    amfi = _amfi_symbols()
    out, changed = {}, False
    for key, s in secs.items():
        if key in manual:
            out[key] = manual[key]
            continue
        if s["geo"] == "IN" and s.get("isin") in amfi:
            out[key] = amfi[s["isin"]] + ".NS"
            continue
        if key in cache:
            if cache[key]:
                out[key] = cache[key]
            continue
        hit = None
        if s.get("isin"):
            hit = await _yahoo_search(client, s["isin"], s["geo"] == "INTL")
        if not hit:
            hit = await _yahoo_search(client, s["name"], s["geo"] == "INTL")
        cache[key] = hit["symbol"] if hit else ""   # "" = tried, not found (fix via POST /alerts/ticker)
        changed = True
        if hit:
            out[key] = hit["symbol"]
    if changed:
        db.collection("alert_meta").document("tickers").set({"auto": cache}, merge=True)
    return out


async def _daily_move(client: httpx.AsyncClient, symbol: str) -> Optional[dict]:
    try:
        r = await client.get(f"https://query1.finance.yahoo.com/v8/finance/chart/{quote(symbol)}",
                             params={"range": "5d", "interval": "1d"}, headers=UA, timeout=10)
        if r.status_code != 200:
            return None
        res = (r.json().get("chart", {}).get("result") or [None])[0]
        if not res:
            return None
        meta = res.get("meta", {})
        closes = (res.get("indicators", {}).get("quote") or [{}])[0].get("close") or []
        pairs = [(t, c) for t, c in zip(res.get("timestamp") or [], closes) if c is not None]
        if len(pairs) < 2:
            return None
        (_, c0), (t1, c1) = pairs[-2], pairs[-1]
        session = dt.datetime.utcfromtimestamp(t1 + int(meta.get("gmtoffset") or 0)).date()
        if (dt.date.today() - session).days > 4:
            return None  # stale — exchange holiday streak or dead ticker
        return {"symbol": symbol, "close": round(c1, 4), "prev": round(c0, 4), "move_pct": (c1 / c0 - 1) * 100,
                "session": session.isoformat(), "currency": meta.get("currency", "")}
    except Exception:
        return None


async def _moves(client, symbols: List[str]) -> Dict[str, dict]:
    sem = asyncio.Semaphore(6)

    async def one(sym):
        async with sem:
            return sym, await _daily_move(client, sym)
    return {s: m for s, m in await asyncio.gather(*(one(s) for s in symbols)) if m}


# ------------------------------------------------------------------ scan
async def run_scan(market: str, run_id: str):
    db = _db()
    rep = {"id": run_id, "market": market, "started_at": _now().isoformat(), "status": "running"}
    runs = db.collection("alert_runs")
    runs.document(run_id).set(rep)
    try:
        cfg = _config()
        exposures = [e for e in _load_exposures() if e.get("total_inr")]
        geo = "IN" if market == "in" else "INTL"
        # Watch set: securities in this market where ≥1 client is materially exposed
        secs: Dict[str, dict] = {}
        for ex in exposures:
            for k, s in ex.get("stocks", {}).items():
                if s["geo"] == geo and E.material(s["inr"], s["pct"], cfg):
                    secs.setdefault(k, s)
        alerts: List[dict] = []
        async with httpx.AsyncClient(follow_redirects=True) as client:
            tickers = await _resolve_tickers(client, secs)
            idx_defs = INDICES[market]
            fx = [FX] if market == "in" else []
            syms = list(set(tickers.values()) | {i["symbol"] for i in idx_defs + fx})
            moves = await _moves(client, syms)
        for key, sym in tickers.items():
            if sym in moves:
                alerts += E.stock_move_alerts({**moves[sym], "name": secs[key]["name"]}, key, exposures, cfg)
        idx_moves = [(i, moves[i["symbol"]]) for i in idx_defs
                     if i["symbol"] in moves and abs(moves[i["symbol"]]["move_pct"]) >= cfg["index_move_pct"]]
        if idx_moves:
            alerts += E.index_move_alerts(idx_moves, exposures, cfg, market)
        if fx and FX["symbol"] in moves:
            alerts += E.fx_move_alerts(moves[FX["symbol"]], exposures, cfg)
        created = 0
        now = _now()
        for a in alerts:
            a["created_at"] = now
            try:
                db.collection("advisor_alerts").document(a["id"]).create(a)  # create() keeps status on re-runs
                created += 1
            except Exception as e:
                if "exist" not in str(e).lower():
                    log.warning("alert write failed: %s", e)
        rep.update(status="ok", finished_at=_now().isoformat(), clients=len(exposures), watched=len(secs),
                   tickers=len(tickers), no_ticker=sorted(k for k in secs if k not in tickers)[:50],
                   prices=len(moves), price_failures=len(syms) - len(moves), alerts=len(alerts), created=created)
    except Exception as e:
        log.exception("alerts scan failed")
        rep.update(status="failed", error=str(e)[:300], finished_at=_now().isoformat())
    runs.document(run_id).set(rep)
    return rep


# ------------------------------------------------------------------ endpoints
@router.post("/scan")
async def scan(bg: BackgroundTasks, market: str = Query("in", pattern="^(in|us)$"), wait: bool = False,
               who: str = Depends(_agent_or_advisor)):
    run_id = f"{dt.date.today().isoformat()}-{market}-{uuid.uuid4().hex[:6]}"
    if wait:
        return await run_scan(market, run_id)
    bg.add_task(run_scan, market, run_id)
    return {"started": run_id, "by": who}


@router.get("/runs")
def runs(limit: int = 10, _: str = Depends(_advisor)):
    docs = [d.to_dict() for d in _db().collection("alert_runs").stream()]
    return {"runs": sorted(docs, key=lambda r: r.get("started_at", ""), reverse=True)[:limit]}


@router.get("/health")
async def health():
    """No auth: is Yahoo reachable from this server, and is exposure data present?"""
    out = {"router": "alerts", "phase": 1}
    async with httpx.AsyncClient(follow_redirects=True) as client:
        out["price_probe"] = {s: bool(m) for s, m in (await _moves(client, ["^NSEI", "^GSPC", "USDINR=X", "NVDA"])).items()}
        hit = await _yahoo_search(client, "US67066G1040", True)
        out["isin_search_probe"] = hit.get("symbol") if hit else None
    try:
        out["exposure"] = _meta("exposure")
    except Exception as e:
        out["exposure"] = {"error": str(e)[:120]}
    return out


@router.get("/feed")
def feed(days: int = 7, include_closed: bool = False, _: str = Depends(_advisor)):
    """Alerts grouped by event (kind + entity + session), clients sorted by estimated impact."""
    cutoff = _now() - dt.timedelta(days=days)
    docs = [d.to_dict() for d in _db().collection("advisor_alerts").where("created_at", ">=", cutoff).stream()]
    if not include_closed:
        docs = [a for a in docs if a.get("status") == "new"]
    groups: Dict[str, dict] = {}
    for a in docs:
        a.pop("created_at", None)
        g = groups.setdefault(f"{a['kind']}|{a['entity']['key']}|{a['session_date']}", {
            "kind": a["kind"], "entity": a["entity"], "session_date": a["session_date"], "move_pct": a["move_pct"],
            "headline": a["headline"], "severity": a["severity"], "clients": [], "total_exposure_inr": 0,
            "total_impact_inr": 0})
        g["clients"].append(a)
        g["total_exposure_inr"] += a.get("exposure_inr", 0)
        g["total_impact_inr"] += a.get("est_impact_inr", 0)
        if a["severity"] == "high":
            g["severity"] = "high"
    out = list(groups.values())
    for g in out:
        g["clients"].sort(key=lambda a: -abs(a.get("est_impact_inr", 0)))
    out.sort(key=lambda g: (g["session_date"], g["severity"] == "high", abs(g["total_impact_inr"])), reverse=True)
    return {"events": out, "count": len(docs)}


class StatusIn(BaseModel):
    status: str
    note: Optional[str] = None


@router.post("/item/{alert_id}/status")
def set_status(alert_id: str, body: StatusIn, uid: str = Depends(_advisor)):
    if body.status not in ("new", "called", "dismissed"):
        raise HTTPException(422, "status must be new | called | dismissed")
    ref = _db().collection("advisor_alerts").document(alert_id)
    if not ref.get().exists:
        raise HTTPException(404, "alert not found")
    ref.set({"status": body.status, "status_note": body.note, "status_by": uid, "status_at": _now()}, merge=True)
    return {"ok": True}


@router.post("/exposure/rebuild")
def exposure_rebuild(_: str = Depends(_agent_or_advisor)):
    return rebuild_exposures()


@router.get("/exposure")
def exposure_list(_: str = Depends(_advisor)):
    rows = []
    for d in _db().collection("client_exposure").stream():
        e = d.to_dict()
        rows.append({"client_id": e.get("client_id"), "client_name": e.get("client_name"), "total_inr": e.get("total_inr"),
                     "geo": e.get("geo"), "countries": e.get("countries"), "unresolved_funds": e.get("unresolved_funds"),
                     "built_at": e.get("built_at")})
    return {"clients": sorted(rows, key=lambda r: -(r["total_inr"] or 0))}


@router.get("/exposure/{client_id}")
def exposure_one(client_id: str, _: str = Depends(_advisor)):
    snap = _db().collection("client_exposure").document(client_id).get()
    if not snap.exists:
        raise HTTPException(404, "no exposure for this client — run /alerts/exposure/rebuild")
    return snap.to_dict()


@router.get("/fund-profile")
def fund_profile(fund: str):
    """Vehicle + composition + look-through geography for one fund (no auth — factsheet data only)."""
    M = _main()
    k, s = resolve_fund(fund)
    if not k:
        raise HTTPException(404, f"not found: {fund}")
    links = _links() if M._get_firestore_client() else {}
    profiles, lt = _profiles_and_lookthrough([k], links)
    rows = lt[k]
    geo = {}
    for r in rows:
        g = f"{r['kind']}_{(r.get('geo') or 'IN').lower()}"
        geo[g] = round(geo.get(g, 0) + r["pct"], 2)
    return {"key": k, "fund_name": M.holdings_db[k]["fund_name"], "match": round(s, 2), **profiles[k],
            "look_through": geo,
            "intl_holdings": sorted(({"name": r["name"], "isin": r["isin"], "country": r.get("country"),
                                      "pct": round(r["pct"], 3)} for r in rows if r.get("geo") == "INTL"),
                                    key=lambda r: -r["pct"])}


@router.get("/foreign-names")
def foreign_names():
    """Normalised names of every international equity across uploaded factsheets — lets the browser-side
    analyser keep foreign stocks out of the Indian large/mid/small split without ISINs."""
    M = _main()
    names = {}
    for f in M.holdings_db.values():
        for h in f.get("holdings", []):
            c = E.classify_holding(h)
            if c["kind"] == "equity" and c["geo"] == "INTL":
                names[str(h.get("name", "")).strip().upper()] = c["country"] or ""
    return {"count": len(names), "names": names}


@router.get("/fofs")
def fofs(_: str = Depends(_advisor)):
    """Every FoF in holdings_db with its units and look-through status, plus client-held funds that
    have no factsheet at all. 'missing' units are what to upload monthly."""
    M = _main()
    links = _links()
    res = _unit_resolver(links)
    held: Dict[str, dict] = {}
    unresolved_client = {}
    for d in _db().collection("client_exposure").stream():
        e = d.to_dict()
        for f in e.get("funds", []):
            h = held.setdefault(f["key"], {"clients": 0, "inr": 0})
            h["clients"] += 1
            h["inr"] += f["inr"]
        for u in e.get("unresolved_funds", []):
            x = unresolved_client.setdefault(u["scheme"], {**u, "clients": 0, "inr": 0})
            x["clients"] += 1
            x["inr"] += u["inr"]
    out = []
    for k, f in M.holdings_db.items():
        p = E.fund_profile(f)
        if not p["vehicle"].startswith("fof"):
            continue
        units = []
        for h in f.get("holdings", []):
            if E.classify_holding(h)["kind"] != "fund_unit":
                continue
            t = res(k, h.get("name", ""))
            units.append({"name": h.get("name"), "isin": h.get("isin"), "pct": h.get("pct"),
                          "linked_to": t, "linked_name": M.holdings_db[t]["fund_name"] if t else None,
                          "manual": bool((links.get(k) or {}).get(E.norm_name(h.get("name", "")))),
                          "status": "linked" if t else "missing"})
        out.append({"key": k, "fund_name": f["fund_name"], "amc": f.get("amc"), "vehicle": p["vehicle"],
                    "units": units, "missing": sum(u["status"] == "missing" for u in units),
                    "held_by_clients": held.get(k, {"clients": 0, "inr": 0})})
    out.sort(key=lambda r: (r["vehicle"] != "fof_intl", -r["held_by_clients"]["inr"], r["fund_name"]))
    return {"fofs": out,
            "client_funds_without_factsheet": sorted(unresolved_client.values(), key=lambda u: -u["inr"])}


class LinkIn(BaseModel):
    fof_key: str
    unit_name: str
    target_key: Optional[str] = None  # None = remove the manual link


@router.post("/fof-link")
def fof_link(body: LinkIn, _: str = Depends(_advisor)):
    M = _main()
    if body.fof_key not in M.holdings_db or (body.target_key and body.target_key not in M.holdings_db):
        raise HTTPException(404, "unknown fund key")
    ref = _db().collection("alert_meta").document("fof_links")
    u = E.norm_name(body.unit_name)
    items = [r for r in (ref.get().to_dict() or {}).get("items", [])
             if not (r["fof_key"] == body.fof_key and E.norm_name(r["unit"]) == u)]
    if body.target_key:
        items.append({"fof_key": body.fof_key, "unit": body.unit_name, "target": body.target_key})
    ref.set({"items": items})
    return {"ok": True, "note": "Run exposure rebuild to apply"}


class TickerIn(BaseModel):
    sec_key: str             # ISIN, or "N:<normalised name>" for holdings without one
    symbol: Optional[str]    # Yahoo symbol, e.g. NVDA, 2330.TW; None removes the override


@router.post("/ticker")
def ticker(body: TickerIn, _: str = Depends(_advisor)):
    ref = _db().collection("alert_meta").document("tickers")
    cur = (ref.get().to_dict() or {}).get("map", {})
    if body.symbol:
        cur[body.sec_key] = body.symbol.strip()
    else:
        cur.pop(body.sec_key, None)
    ref.set({"map": cur}, merge=True)
    return {"ok": True}


@router.get("/config")
def get_config():
    return {"thresholds": _config(), "defaults": DEFAULT_CONFIG}


@router.put("/config")
def put_config(body: dict, _: str = Depends(_advisor)):
    th = {k: float(v) for k, v in (body.get("thresholds") or {}).items() if k in DEFAULT_CONFIG}
    _db().collection("alert_meta").document("config").set({"thresholds": th, "updated_at": _now()}, merge=True)
    return {"thresholds": _config()}
