"""
Vantage · Alerts engine — pure logic, no I/O (unit-testable).

  classify_holding()   one factsheet line → equity / debt / fund_unit / cash, geo IN | INTL, country
  fund_profile()       vehicle: domestic | domestic_with_intl | intl_direct | fof_intl | fof_domestic
  look_through()       fund → flat list of underlying exposures (recurses into linked FoF units, depth 2)
  build_client_exposure()  client funds (₹) → stock / country / geo / Indian-cap exposure in ₹ and %
  move_alerts()        price moves × exposures → advisor alerts with templated talking points

Geography is the ISIN's country prefix (issuer domicile). Indian cap applies to Indian equity only.
"""
from __future__ import annotations

import hashlib
import re
from typing import Callable, Dict, List, Optional, Tuple

ISIN_RE = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$")
INTL_NAME_RE = re.compile(
    r"overseas|international|global|world|foreign|\bus\b|u\.s\.|usa|america|nasdaq|s&p|nyse|fang|"
    r"china|japan|taiwan|hang seng|europe|emerging market|asia|greater china|developed market|msci",
    re.I)
US_NAME_RE = re.compile(r"\bus\b|u\.s\.|usa|america|nasdaq|s&p|nyse|fang|dow jones", re.I)
FOF_NAME_RE = re.compile(r"\bfof\b|fund\s*of\s*funds?|fund-of-funds?", re.I)
FUND_UNIT_NAME_RE = re.compile(r"\b(etf|fund|fof|bees|ucits|sicav|units?|trust|portfolio)\b", re.I)
CASH_RE = re.compile(r"treps|cblo|reverse repo|net current|net receivable|cash|margin|tri-?party", re.I)
DEBT_RE = re.compile(
    r"crisil|care|icra|fitch|ind-ra|aaa|aa\+|aa-|\baa\b|sovereign|tbill|t-bill|treasury|gilt|g-sec|gsec|"
    r"\bsdl\b|state development|commercial paper|certificate of deposit|fixed deposit|\bbond\b|debenture|"
    r"\bncd\b|\bdebt\b|\bptc\b|securiti[sz]ation|\b\d+(\.\d+)?%", re.I)
EQUITY_SECTOR_RE = re.compile(
    r"^(banks|insurance|it |software|pharma|auto|fmcg|consumer|capital goods|industrial|financial services|"
    r"healthcare|telecom|cement|metals|energy|power|oil|chemicals|textiles|realty|media|retail|"
    r"information technology|semiconductors|communication|technology)", re.I)
# Used only when the ISIN is blank (some PDF-parsed factsheets). Deliberately narrow: "Corp"/"Corporation"
# are excluded because Indian names use them too (Indian Oil Corp, Power Finance Corporation).
FOREIGN_SUFFIX_RE = re.compile(r"\b(inc|plc|ag|n\.?v|s\.?a|se|asa|spa|adr|ads|class [abc])\.?$", re.I)
FOREIGN_SECTOR_RE = re.compile(r"overseas|foreign|international|global", re.I)

COUNTRY_NAMES = {
    "US": "United States", "GB": "United Kingdom", "IE": "Ireland", "LU": "Luxembourg", "KY": "Cayman Islands",
    "NL": "Netherlands", "DE": "Germany", "FR": "France", "CH": "Switzerland", "JP": "Japan", "CN": "China",
    "HK": "Hong Kong", "TW": "Taiwan", "KR": "South Korea", "CA": "Canada", "BM": "Bermuda", "SG": "Singapore",
    "DK": "Denmark", "SE": "Sweden", "IL": "Israel", "BR": "Brazil", "AU": "Australia", "JE": "Jersey", "IN": "India",
}


def norm_name(s: str) -> str:
    s = re.sub(r"[^a-z0-9& ]", " ", str(s or "").lower())
    s = re.sub(r"\b(ltd|limited|inc|corp|corporation|co|plc|the|class [abc])\b", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def stock_key(isin: str, name: str) -> str:
    return isin if isin and ISIN_RE.match(isin) else "N:" + norm_name(name)


def classify_holding(h: dict) -> dict:
    """→ {kind: equity|debt|cash|fund_unit, geo: IN|INTL|None, country: str|None}"""
    name = str(h.get("name", "")).strip()
    sector = str(h.get("sector", "")).strip()
    isin = str(h.get("isin", "")).strip().upper()
    eff_sector = "" if sector == name else sector
    combined = f"{name} {eff_sector}"
    valid = bool(ISIN_RE.match(isin))
    foreign_isin = valid and not isin.startswith("IN")

    if CASH_RE.search(combined) and not EQUITY_SECTOR_RE.match(eff_sector):
        return {"kind": "cash", "geo": "IN", "country": "IN"}
    # Fund units: Indian MF/ETF units carry INF ISINs; foreign fund units are foreign ISINs with fund words
    if isin.startswith("INF") or (FUND_UNIT_NAME_RE.search(name) and not EQUITY_SECTOR_RE.match(eff_sector)
                                  and (foreign_isin or not valid) and not DEBT_RE.search(combined)):
        geo = "INTL" if (foreign_isin or INTL_NAME_RE.search(name)) else "IN"
        return {"kind": "fund_unit", "geo": geo, "country": isin[:2] if foreign_isin else None}
    is_debt = (bool(DEBT_RE.search(eff_sector)) and not EQUITY_SECTOR_RE.match(eff_sector)) \
        or bool(re.match(r"^\d+\.?\d*%", name)) or isin.startswith(("IN0", "IN1", "IN2", "IN3", "IN9"))
    if is_debt:
        return {"kind": "debt", "geo": "INTL" if foreign_isin else "IN", "country": isin[:2] if foreign_isin else "IN"}
    if foreign_isin:
        return {"kind": "equity", "geo": "INTL", "country": isin[:2]}
    if not valid and (FOREIGN_SUFFIX_RE.search(name) or FOREIGN_SECTOR_RE.search(eff_sector)):
        return {"kind": "equity", "geo": "INTL", "country": None}
    return {"kind": "equity", "geo": "IN", "country": "IN"}


def fund_profile(fund: dict) -> dict:
    """Vehicle + composition of one fund (as a % of its own NAV)."""
    name = fund.get("fund_name", "")
    comp = {"equity_in": 0.0, "equity_intl": 0.0, "debt": 0.0, "cash": 0.0, "units_in": 0.0, "units_intl": 0.0}
    for h in fund.get("holdings", []):
        c = classify_holding(h)
        p = float(h.get("pct") or 0)
        if c["kind"] == "equity":
            comp["equity_intl" if c["geo"] == "INTL" else "equity_in"] += p
        elif c["kind"] == "fund_unit":
            comp["units_intl" if c["geo"] == "INTL" else "units_in"] += p
        else:
            comp[c["kind"]] += p
    units = comp["units_in"] + comp["units_intl"]
    eq = comp["equity_in"] + comp["equity_intl"]
    if FOF_NAME_RE.search(name) or units >= 50:
        intl = INTL_NAME_RE.search(name) or comp["units_intl"] > comp["units_in"]
        vehicle = "fof_intl" if intl else "fof_domestic"
    elif eq > 0 and comp["equity_intl"] / eq >= 0.65:
        vehicle = "intl_direct"          # India-listed international ETF / index fund
    elif comp["equity_intl"] > 0.5:      # ignore rounding-level noise
        vehicle = "domestic_with_intl"   # e.g. Parag Parikh Flexi Cap
    else:
        vehicle = "domestic"
    return {"vehicle": vehicle, "composition": {k: round(v, 2) for k, v in comp.items()}}


def look_through(key: str, db: dict, resolve_unit: Callable[[str, str], Optional[str]],
                 cap_of: Callable[[str], str], depth: int = 0, _seen: Optional[set] = None) -> List[dict]:
    """Flatten a fund into underlying exposures, pct = % of this fund's NAV.
    resolve_unit(parent_key, unit_name) → holdings_db key of the underlying fund, or None."""
    seen = (_seen or set()) | {key}
    out: List[dict] = []
    for h in (db.get(key) or {}).get("holdings", []):
        p = float(h.get("pct") or 0)
        if p <= 0:
            continue
        c = classify_holding(h)
        isin = str(h.get("isin", "")).strip().upper()
        name = str(h.get("name", "")).strip()
        if c["kind"] == "fund_unit":
            target = resolve_unit(key, name)
            if target and target in db and target not in seen and depth < 2:
                for s in look_through(target, db, resolve_unit, cap_of, depth + 1, seen):
                    s = dict(s)
                    s["pct"] = p * s["pct"] / 100
                    s["via_unit"] = s.get("via_unit") or db[target].get("fund_name", target)
                    out.append(s)
            else:
                # A fund unit's ISIN prefix is the fund's domicile (LU/IE), not where it invests: infer from name
                hint = "US" if (c["geo"] == "INTL" and US_NAME_RE.search(name)) else None
                out.append({"kind": "unresolved_unit", "geo": c["geo"], "country": hint,
                            "name": name, "isin": isin, "pct": p, "key": "U:" + norm_name(name)})
            continue
        row = {"kind": c["kind"], "geo": c["geo"], "country": c["country"], "name": name, "isin": isin,
               "sector": str(h.get("sector", "")).strip(), "pct": p, "key": stock_key(isin, name)}
        if c["kind"] == "equity":
            row["cap"] = cap_of(isin) if c["geo"] == "IN" else None
        out.append(row)
    return out


def build_client_exposure(client: dict, resolved: List[Tuple[dict, Optional[str], float]], db: dict,
                          lt_cache: Dict[str, List[dict]], profiles: Dict[str, dict]) -> dict:
    """resolved: [(client_fund_row, holdings_db_key|None, match_score)]; lt_cache: key → look_through rows."""
    total = sum(float(f.get("currentValue") or 0) for f, _, _ in resolved) or 0.0
    geo = {"equity_in": 0.0, "equity_intl": 0.0, "debt": 0.0, "cash": 0.0,
           "unresolved_units_in": 0.0, "unresolved_units_intl": 0.0, "no_factsheet": 0.0}
    cap_in = {"large": 0.0, "mid": 0.0, "small": 0.0}
    countries: Dict[str, float] = {}
    countries_units: Dict[str, float] = {}   # FoF units without look-through, country inferred from name
    stocks: Dict[str, dict] = {}
    funds, unresolved_funds = [], []
    for f, key, score in resolved:
        inr = float(f.get("currentValue") or 0)
        if inr <= 0:
            continue
        scheme = f.get("schemeName", "")
        if not key:
            geo["no_factsheet"] += inr
            unresolved_funds.append({"scheme": scheme, "inr": round(inr), "looks_international": bool(INTL_NAME_RE.search(scheme)),
                                     "looks_fof": bool(FOF_NAME_RE.search(scheme))})
            continue
        fund_name = db[key].get("fund_name", key)
        funds.append({"scheme": scheme, "key": key, "fund_name": fund_name, "inr": round(inr),
                      "vehicle": profiles.get(key, {}).get("vehicle"), "match": round(score, 2)})
        for r in lt_cache.get(key, []):
            v = inr * r["pct"] / 100
            if r["kind"] == "equity":
                geo["equity_intl" if r["geo"] == "INTL" else "equity_in"] += v
                if r["geo"] == "IN":
                    cap_in[r.get("cap") or "small"] += v
                else:
                    cc = r.get("country") or "XX"
                    countries[cc] = countries.get(cc, 0.0) + v
                s = stocks.setdefault(r["key"], {"name": r["name"], "isin": r["isin"], "geo": r["geo"],
                                                 "country": r.get("country"), "sector": r.get("sector", ""),
                                                 "cap": r.get("cap"), "inr": 0.0, "via": {}})
                s["inr"] += v
                via = s["via"].setdefault(fund_name, {"inr": 0.0, "pct": 0.0})
                via["inr"] += v
                via["pct"] += r["pct"]
            elif r["kind"] == "unresolved_unit":
                geo["unresolved_units_intl" if r["geo"] == "INTL" else "unresolved_units_in"] += v
                if r["geo"] == "INTL" and r.get("country"):
                    countries_units[r["country"]] = countries_units.get(r["country"], 0.0) + v
            else:
                geo[r["kind"]] += v
    # Drop dust (< ₹5,000) to keep the document small; round for storage
    kept = {}
    for k, s in stocks.items():
        if s["inr"] < 5000:
            continue
        s["inr"] = round(s["inr"])
        s["pct"] = round(100 * s["inr"] / total, 3) if total else 0
        # list, not map: fund names can contain "." or "/" which are unsafe as Firestore map keys
        s["via"] = sorted(({"fund": fn, "inr": round(v["inr"]), "pct": round(v["pct"], 3)} for fn, v in s["via"].items()),
                          key=lambda x: -x["inr"])
        kept[k] = s
    pct = lambda v: round(100 * v / total, 2) if total else 0
    return {
        "client_id": client.get("clientId"), "client_name": client.get("clientName") or client.get("clientId"),
        "total_inr": round(total),
        "geo": {k: {"inr": round(v), "pct": pct(v)} for k, v in geo.items()},
        "cap_in": {k: {"inr": round(v), "pct": pct(v)} for k, v in cap_in.items()},
        "countries": {k: {"inr": round(v), "pct": pct(v)} for k, v in sorted(countries.items(), key=lambda x: -x[1])},
        "countries_units": {k: {"inr": round(v), "pct": pct(v)} for k, v in countries_units.items()},
        "stocks": kept, "funds": funds, "unresolved_funds": unresolved_funds,
    }


# ------------------------------------------------------------------ alerts
def inr_fmt(v: float) -> str:
    a = abs(v)
    s = f"₹{a / 1e7:.2f}Cr" if a >= 1e7 else f"₹{a / 1e5:.2f}L" if a >= 1e5 else f"₹{a:,.0f}"
    return ("−" if v < 0 else "") + s


def material(inr: float, pct: float, cfg: dict) -> bool:
    return inr >= cfg["min_exposure_inr"] or pct >= cfg["min_exposure_pct"]


def alert_id(kind: str, entity: str, session: str, client_id: str) -> str:
    return hashlib.sha1(f"{kind}|{entity}|{session}|{client_id}".encode()).hexdigest()[:24]


def _via_text(via: List[dict], n=2) -> str:
    top = sorted(via, key=lambda x: -x["inr"])[:n]
    more = len(via) - len(top)
    t = ", ".join(f"{v['fund']} ({inr_fmt(v['inr'])})" for v in top)
    return t + (f" +{more} more" if more > 0 else "")


def stock_move_alerts(move: dict, sec_key: str, exposures: List[dict], cfg: dict) -> List[dict]:
    """move: {symbol, name, move_pct, close, prev, session, currency}"""
    out = []
    mv = move["move_pct"]
    if abs(mv) < cfg["stock_move_pct"]:
        return out
    verb = "rose" if mv > 0 else "fell"
    for ex in exposures:
        s = ex.get("stocks", {}).get(sec_key)
        if not s or not material(s["inr"], s["pct"], cfg):
            continue
        impact = s["inr"] * mv / 100
        top = max(s["via"], key=lambda x: x["pct"])
        cur = move.get("currency") or ""
        tps = [
            f"{s['name']} {verb} {abs(mv):.1f}% on {move['session']} (close {cur} {move['close']:,.2f} vs {move['prev']:,.2f}).",
            f"{ex['client_name']} holds {inr_fmt(s['inr'])} ({s['pct']:.2f}% of portfolio) through {_via_text(s['via'])}. "
            f"Estimated impact {inr_fmt(impact)} ({100 * impact / ex['total_inr']:+.2f}% of portfolio).",
            f"Largest weight: {top['pct']:.2f}% of {top['fund']}.",
        ]
        if s["geo"] == "INTL":
            tps.append("International holding: the rupee's move against the dollar also changes its INR value.")
        out.append({
            "id": alert_id("stock_move", sec_key, move["session"], ex["client_id"]),
            "kind": "stock_move", "session_date": move["session"], "status": "new",
            "severity": "high" if abs(mv) >= 2 * cfg["stock_move_pct"] else "medium",
            "entity": {"key": sec_key, "name": s["name"], "symbol": move["symbol"], "geo": s["geo"],
                       "country": s.get("country")},
            "move_pct": round(mv, 2), "client_id": ex["client_id"], "client_name": ex["client_name"],
            "exposure_inr": s["inr"], "exposure_pct": s["pct"], "est_impact_inr": round(impact),
            "est_impact_pct": round(100 * impact / ex["total_inr"], 3) if ex["total_inr"] else 0,
            "via": s["via"],
            "headline": f"{s['name']} {verb} {abs(mv):.1f}%", "talking_points": tps,
            "exposure_as_of": ex.get("built_at"),
        })
    return out


def _scope_exposure(ex: dict, scope: dict) -> Tuple[float, str]:
    if scope.get("cap"):
        return ex["cap_in"][scope["cap"]]["inr"], f"Indian {scope['cap']}-cap equity"
    if scope.get("country"):
        cc = scope["country"]
        direct = ex["countries"].get(cc, {"inr": 0})["inr"]
        units = (ex.get("countries_units") or {}).get(cc, {"inr": 0})["inr"]
        label = f"{COUNTRY_NAMES.get(cc, cc)} equity" + (" (incl. FoFs without look-through)" if units else "")
        return direct + units, label
    if scope.get("geo") == "IN":
        return ex["geo"]["equity_in"]["inr"], "Indian equity"
    return ex["geo"]["equity_intl"]["inr"] + ex["geo"]["unresolved_units_intl"]["inr"], "international equity"


def index_move_alerts(moves: List[Tuple[dict, dict]], exposures: List[dict], cfg: dict, market: str) -> List[dict]:
    """moves: [(index_def, move)] already filtered to |move| ≥ threshold. One alert per client per market:
    the index with the largest estimated ₹ impact; the others are mentioned."""
    out = []
    for ex in exposures:
        best, others = None, []
        for idx, mv in moves:
            inr, label = _scope_exposure(ex, idx["scope"])
            pct = 100 * inr / ex["total_inr"] if ex["total_inr"] else 0
            if inr <= 0 or not material(inr, pct, cfg):
                continue
            impact = inr * mv["move_pct"] / 100
            cand = (abs(impact), idx, mv, inr, pct, label, impact)
            if not best or cand[0] > best[0]:
                if best:
                    others.append(best)
                best = cand
            else:
                others.append(cand)
        if not best:
            continue
        _, idx, mv, inr, pct, label, impact = best
        verb = "rose" if mv["move_pct"] > 0 else "fell"
        tps = [f"{idx['name']} {verb} {abs(mv['move_pct']):.1f}% on {mv['session']}.",
               f"{ex['client_name']}'s {label}: {inr_fmt(inr)} ({pct:.1f}% of portfolio). "
               f"Estimated impact ≈ {inr_fmt(impact)} ({100 * impact / ex['total_inr']:+.2f}% of portfolio), "
               f"assuming the holdings move with the index."]
        if others:
            tps.append("Also moved: " + ", ".join(f"{o[1]['name']} {o[2]['move_pct']:+.1f}%" for o in others) + ".")
        out.append({
            "id": alert_id("index_move", f"{market}:{idx['key']}", mv["session"], ex["client_id"]),
            "kind": "index_move", "session_date": mv["session"], "status": "new",
            "severity": "high" if abs(mv["move_pct"]) >= 2 * cfg["index_move_pct"] else "medium",
            "entity": {"key": idx["key"], "name": idx["name"], "symbol": idx["symbol"], "market": market},
            "move_pct": round(mv["move_pct"], 2), "client_id": ex["client_id"], "client_name": ex["client_name"],
            "exposure_inr": round(inr), "exposure_pct": round(pct, 2), "est_impact_inr": round(impact),
            "est_impact_pct": round(100 * impact / ex["total_inr"], 3), "via": [],
            "headline": f"{idx['name']} {verb} {abs(mv['move_pct']):.1f}%", "talking_points": tps,
            "exposure_as_of": ex.get("built_at"),
        })
    return out


def fx_move_alerts(mv: dict, exposures: List[dict], cfg: dict) -> List[dict]:
    """USD/INR. A rise = rupee weaker = INR value of international holdings up."""
    out = []
    if abs(mv["move_pct"]) < cfg["fx_move_pct"]:
        return out
    rupee = "weakened" if mv["move_pct"] > 0 else "strengthened"
    for ex in exposures:
        inr, _ = _scope_exposure(ex, {"geo": "INTL"})
        pct = 100 * inr / ex["total_inr"] if ex["total_inr"] else 0
        if inr <= 0 or not material(inr, pct, cfg):
            continue
        impact = inr * mv["move_pct"] / 100
        out.append({
            "id": alert_id("fx_move", "USDINR", mv["session"], ex["client_id"]),
            "kind": "fx_move", "session_date": mv["session"], "status": "new",
            "severity": "high" if abs(mv["move_pct"]) >= 2 * cfg["fx_move_pct"] else "medium",
            "entity": {"key": "USDINR", "name": "USD/INR", "symbol": mv["symbol"]},
            "move_pct": round(mv["move_pct"], 2), "client_id": ex["client_id"], "client_name": ex["client_name"],
            "exposure_inr": round(inr), "exposure_pct": round(pct, 2), "est_impact_inr": round(impact),
            "est_impact_pct": round(100 * impact / ex["total_inr"], 3), "via": [],
            "headline": f"Rupee {rupee} {abs(mv['move_pct']):.1f}% vs USD",
            "talking_points": [
                f"The rupee {rupee} {abs(mv['move_pct']):.1f}% against the dollar on {mv['session']} "
                f"(USD/INR {mv['prev']:.2f} → {mv['close']:.2f}).",
                f"{ex['client_name']}'s international equity: {inr_fmt(inr)} ({pct:.1f}% of portfolio). "
                f"Currency effect ≈ {inr_fmt(impact)} in INR terms, before any move in the stocks themselves."],
            "exposure_as_of": ex.get("built_at"),
        })
    return out
