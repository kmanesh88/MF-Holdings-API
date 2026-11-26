"""
Vantage · Fluid Tax — computation engine.

Reads every rate / threshold from the merged ruleset (base + approved overlay), so an approved
rule update changes the maths without a code change.

Main entry points:
    classify_gain(rules, asset_id, purchase_date, sale_date, **opts) -> dict
    compute_tax(rules, payload) -> dict
    harvest_headroom(rules, payload) -> dict
"""
from __future__ import annotations

import copy
import datetime as dt
import re
from typing import Any, Dict, List, Optional

# ------------------------------------------------------------------ overlay helpers
_SEL = re.compile(r"^(?P<key>[^\[]+)(?:\[(?P<sel>[^\]]+)\])?$")


def _step(node, seg, create=False):
    m = _SEL.match(seg)
    if not m:
        raise KeyError(seg)
    key, sel = m.group("key"), m.group("sel")
    if isinstance(node, list) and key.isdigit():
        return node[int(key)]
    if key not in node:
        if not create:
            raise KeyError(seg)
        node[key] = {}
    child = node[key]
    if sel is None:
        return child
    if not isinstance(child, list):
        raise KeyError(seg)
    if sel.isdigit():
        return child[int(sel)]
    k, v = sel.split("=", 1)
    for item in child:
        if isinstance(item, dict) and str(item.get(k)) == v:
            return item
    raise KeyError(seg)


def get_path(obj: dict, path: str):
    node = obj
    for seg in path.split("."):
        node = _step(node, seg)
    return node


def set_path(obj: dict, path: str, value):
    """Path syntax: 'capital_gains.assets[id=sgb].ltcg' or 'slabs.new_regime.rebate.max_rebate'.
    A final segment of the form 'list[+]' appends value to that list."""
    segs = path.split(".")
    node = obj
    for seg in segs[:-1]:
        node = _step(node, seg, create=True)
    last = segs[-1]
    if last.endswith("[+]"):
        node.setdefault(last[:-3], []).append(value)
        return
    m = _SEL.match(last)
    key, sel = m.group("key"), m.group("sel")
    if sel is None:
        node[key] = value
    else:
        target = _step(node, last)
        lst = node[key]
        lst[lst.index(target)] = value


def merge_overlay(base: dict, overlay: Optional[dict]) -> dict:
    rules = copy.deepcopy(base)
    if not overlay:
        return rules
    for p in overlay.get("patches", []):
        try:
            set_path(rules, p["path"], p["value"])
        except Exception:  # a stale patch must never break the engine
            rules.setdefault("_overlay_errors", []).append(p.get("path"))
    rules["version"] = overlay.get("version", rules["version"])
    rules["overlay_applied"] = len(overlay.get("patches", []))
    return rules


# ------------------------------------------------------------------ basics
def _d(x) -> dt.date:
    if isinstance(x, dt.date):
        return x
    return dt.date.fromisoformat(str(x)[:10])


def _add_months(d: dt.date, months: int) -> dt.date:
    y, m = divmod(d.month - 1 + months, 12)
    y += d.year
    m += 1
    day = min(d.day, [31, 29 if (y % 4 == 0 and (y % 100 or y % 400 == 0)) else 28, 31, 30, 31, 30,
                      31, 31, 30, 31, 30, 31][m - 1])
    return dt.date(y, m, day)


def months_held(purchase, sale) -> float:
    p, s = _d(purchase), _d(sale)
    return round((s - p).days / 30.4375, 1)


def slab_tax(income: float, slabs: List[list]) -> float:
    tax = 0.0
    for lo, hi, rate in slabs:
        if income <= lo:
            break
        top = income if hi is None else min(income, hi)
        tax += (top - lo) * rate / 100
    return tax


def marginal_slab_rate(income: float, slabs: List[list]) -> float:
    for lo, hi, rate in slabs:
        if hi is None or income < hi:
            if income >= lo:
                return rate
    return slabs[-1][2]


def _asset(rules, asset_id):
    for a in rules["capital_gains"]["assets"]:
        if a["id"] == asset_id:
            return a
    raise ValueError(f"Unknown asset '{asset_id}'")


# ------------------------------------------------------------------ classification
MF_UNIT_ASSETS = {"equity_mf", "debt_mf_post_apr2023", "debt_mf_pre_apr2023", "other_mf", "commodity_intl_etf"}

SIF_MAP = {"equity": "equity_mf", "debt": "debt_mf_post_apr2023", "hybrid": "other_mf"}


def classify_gain(rules: dict, asset_id: str, purchase_date, sale_date, *, listed: Optional[bool] = None,
                  original_subscriber: bool = False, held_to_maturity: bool = False, sif_class: str = "",
                  ulip_equity: bool = True, promoter: str = "", entity: str = "individual_resident") -> dict:
    """Returns {term, rate, pool_112A, setoff, exempt, asset_used, months, lt_threshold, reason}.
    rate is a number (special rate %) or 'slab' (normal rate)."""
    p, s = _d(purchase_date), _d(sale_date)
    held = months_held(p, s)
    aid = asset_id

    # routing
    if aid == "sif":
        aid = SIF_MAP.get(sif_class or "hybrid", "other_mf")
    if aid == "ulip_high_premium":
        aid = "equity_mf" if ulip_equity else "other_mf"
    if aid == "buyback":
        if promoter in ("corporate", "non_corporate"):
            eff = _asset(rules, "buyback")["promoter_effective"]
            rate = eff["corporate_promoter" if promoter == "corporate" else "non_corporate_promoter"]
            return {"term": "flat", "rate": rate, "pool_112A": False, "setoff": True, "exempt": False,
                    "asset_used": "buyback", "months": held, "lt_threshold": None,
                    "reason": f"Promoter buyback: effective {rate}% (Budget 2026)."}
        aid = "listed_equity"
    if aid.startswith("debt_mf") and p < dt.date(2023, 4, 1):
        aid = "debt_mf_pre_apr2023"
    elif aid.startswith("debt_mf"):
        aid = "debt_mf_post_apr2023"

    a = _asset(rules, aid)

    if a.get("flat") is not None:
        return {"term": "flat", "rate": a["flat"], "pool_112A": False, "setoff": False, "exempt": False,
                "asset_used": aid, "months": held, "lt_threshold": None, "vda": True,
                "reason": f"{a['label']}: flat {a['flat']}%, no set-off of losses."}
    if a.get("exempt_all"):
        return {"term": "LT", "rate": 0, "pool_112A": False, "setoff": False, "exempt": True,
                "asset_used": aid, "months": held, "lt_threshold": None, "reason": f"{a['label']}: exempt."}
    if "stcg" not in a:
        raise ValueError(f"'{a['label']}' is taxed by its underlying holdings — pick the underlying asset instead.")

    if aid == "sgb" and original_subscriber and held_to_maturity and entity.startswith("individual"):
        return {"term": "LT", "rate": 0, "pool_112A": False, "setoff": False, "exempt": True,
                "asset_used": aid, "months": held, "lt_threshold": a.get("lt_months"),
                "reason": "SGB held from original issue to maturity by an individual: exempt."}

    if a.get("deemed_stcg"):
        return {"term": "ST", "rate": "slab", "pool_112A": False, "setoff": True, "exempt": False,
                "asset_used": aid, "months": held, "lt_threshold": None,
                "reason": "Deemed short-term: taxed at slab/normal rate regardless of holding."}

    lt = a.get("lt_months")
    if listed and a.get("lt_months_listed"):
        lt = a["lt_months_listed"]
    is_lt = lt is not None and s > _add_months(p, lt)
    if is_lt:
        return {"term": "LT", "rate": a["ltcg"], "pool_112A": bool(a.get("exemption_112A")), "setoff": True,
                "exempt": False, "asset_used": aid, "months": held, "lt_threshold": lt,
                "reason": f"Held {held} months (> {lt}): long-term at {a['ltcg']}%"
                          + (" with ₹1.25 lakh 112A exemption pool." if a.get("exemption_112A") else ".")}
    return {"term": "ST", "rate": a["stcg"], "pool_112A": False, "setoff": True, "exempt": False,
            "asset_used": aid, "months": held, "lt_threshold": lt,
            "reason": f"Held {held} months (≤ {lt}): short-term at "
                      + (f"{a['stcg']}%." if a["stcg"] != "slab" else "slab/normal rate.")
                      + (f" Turns long-term after {_add_months(p, lt).isoformat()}." if lt else "")}


# ------------------------------------------------------------------ entity profile
SLAB_ENTITIES = {"individual_resident", "individual_nri", "huf", "aop_boi"}
MMR_ENTITIES = {"trust_private_discretionary", "aop_mmr"}
COMPANY_OPTIONS = {"company_115BAA": "115BAA", "company_115BAB": "115BAB",
                   "company_normal_small": "normal_small", "company_normal": "normal"}


def _resolve_engine(rules, entity):
    e = rules["entities"].get(entity)
    if entity in SLAB_ENTITIES | MMR_ENTITIES | {"firm_llp", "company_foreign"} | set(COMPANY_OPTIONS):
        return entity, (e or {}).get("label", entity)
    if e is None:
        raise ValueError(f"Unknown entity '{entity}'")
    eng = e.get("engine")
    if eng is None:
        raise ValueError(f"{e['label']}: not taxed directly — tax arises in the hands of its investors / beneficiaries.")
    return eng, e["label"]


def _profile(rules, entity, regime, age_band, company_regime="115BAA", coop_regime="normal"):
    ent = rules["entities"]
    eng, label = _resolve_engine(rules, entity)
    if eng == "aop_determinate_like":
        eng = "aop_boi"
    if eng in SLAB_ENTITIES or eng in ("huf_nonresident", "ajp"):
        reg = regime if regime in ("new_regime", "old_regime") else "new_regime"
        if reg == "new_regime":
            slabs = rules["slabs"]["new_regime"]["slabs"]
        else:
            band = age_band if (eng == "individual_resident" and age_band in ("senior_60_79", "super_senior_80")) else "below_60"
            slabs = rules["slabs"]["old_regime"]["slabs"][band]
        return {"kind": "slab", "slabs": slabs, "regime": reg, "surcharge": rules["surcharge"]["individual_like"],
                "rebate": rules["slabs"][reg]["rebate"] if eng == "individual_resident" else None,
                "shortfall": eng in ("individual_resident", "huf"), "label": label}
    if eng in MMR_ENTITIES:
        return {"kind": "flat", "rate": 30, "regime": "mmr", "surcharge": rules["surcharge"]["individual_like"],
                "rebate": None, "shortfall": False, "label": label}
    if eng == "firm_llp":
        return {"kind": "flat", "rate": ent["firm_llp"]["rate"], "regime": "flat",
                "surcharge": rules["surcharge"]["firm_llp"], "rebate": None, "shortfall": False,
                "label": label, "no_special_cap": True}
    if eng in COMPANY_OPTIONS or eng == "company":
        oid = COMPANY_OPTIONS.get(eng) or {"115BAA": "115BAA", "115BAB": "115BAB", "normal_small": "normal_small",
                                           "normal": "normal"}.get(company_regime, "115BAA")
        opt = next(o for o in ent["company_domestic"]["options"] if o["id"] == oid)
        return {"kind": "flat", "rate": opt["rate"], "regime": opt["id"],
                "surcharge": rules["surcharge"][opt["surcharge"]], "rebate": None, "shortfall": False,
                "label": f"{label} — {opt['id']}" if eng == "company" else f"Domestic company ({opt['id']})", "no_special_cap": True}
    if eng == "company_foreign":
        return {"kind": "flat", "rate": ent["company_foreign"]["rate"], "regime": "flat",
                "surcharge": rules["surcharge"]["foreign_company"], "rebate": None, "shortfall": False,
                "label": label, "no_special_cap": True}
    if eng in ("fpi_corporate", "fpi_non_corporate"):
        return {"kind": "fpi", "rate": 20, "slab_gain_rate": 30, "regime": "115AD",
                "surcharge": rules["surcharge"]["foreign_company" if eng == "fpi_corporate" else "individual_like"],
                "rebate": None, "shortfall": False, "label": label, "no_special_cap": eng == "fpi_corporate"}
    if eng == "cooperative":
        opts = ent["cooperative_society"]["options"]
        if coop_regime in ("115BAD", "115BAE"):
            opt = next(o for o in opts if o["id"] == coop_regime)
            return {"kind": "flat", "rate": opt["rate"], "regime": coop_regime, "surcharge": rules["surcharge"][opt["surcharge"]],
                    "rebate": None, "shortfall": False, "label": label, "no_special_cap": True}
        opt = next(o for o in opts if o["id"] == "normal")
        return {"kind": "slab", "slabs": opt["slabs"], "regime": "normal", "surcharge": rules["surcharge"][opt["surcharge"]],
                "rebate": None, "shortfall": False, "label": label, "no_special_cap": True}
    if eng == "charitable":
        return {"kind": "exempt", "regime": "exempt", "label": label}
    raise ValueError(f"Unknown entity '{entity}'")


def _surcharge_rate(sc: dict, total_income: float, regime: str) -> float:
    if "flat" in sc:
        return sc["flat"]
    rate = 0
    for threshold, r in sc.get("bands", []):
        if total_income > threshold:
            rate = r
    if regime == "new_regime" and sc.get("new_regime_cap") is not None:
        rate = min(rate, sc["new_regime_cap"])
    return rate


def _threshold_below(sc: dict, total_income: float) -> Optional[float]:
    t = None
    for threshold, _ in sc.get("bands", []):
        if total_income > threshold:
            t = threshold
    return t


# ------------------------------------------------------------------ main computation
def compute_tax(rules: dict, payload: dict) -> dict:
    """
    payload = {
      entity: individual_resident | individual_nri | huf | aop_boi | aop_mmr | trust_private_discretionary |
              firm_llp | company_115BAA | company_115BAB | company_normal_small | company_normal | company_foreign
      regime: new_regime | old_regime          (slab entities)
      age_band: below_60 | senior_60_79 | super_senior_80   (old regime, resident individuals)
      normal_income: taxable income excluding the gains below, after deductions
      dividend_income: part of normal_income that is dividend (for the 15% surcharge cap)
      brought_forward: {stcl: x, ltcl: y}      optional carried-forward losses
      gains: [{asset_id, purchase_date, sale_date, gain, listed?, original_subscriber?, held_to_maturity?,
               sif_class?, promoter?, label?}]
    }
    """
    entity = payload.get("entity", "individual_resident")
    prof = _profile(rules, entity, payload.get("regime", "new_regime"), payload.get("age_band", "below_60"),
                    payload.get("company_regime", "115BAA"), payload.get("coop_regime", "normal"))
    if prof["kind"] == "exempt":
        return {"entity": entity, "entity_label": prof["label"], "regime": "exempt", "lines": [], "set_off": [],
                "carry_forward": {"stcl": 0, "ltcl": 0}, "exemption_112A_used": 0, "basic_exemption_used_vs_cg": 0,
                "exempt_gains": 0, "income": {"normal_incl_slab_gains": 0, "special_rate_gains": 0, "total_taxable": 0},
                "tax": {"normal": 0, "special_cg": 0, "vda": 0, "rebate": 0, "surcharge_rate": 0, "surcharge": 0,
                        "marginal_relief": 0, "cess": 0, "total": 0},
                "tax_on_gains_only": 0, "effective_rate_pct": 0.0,
                "notes": ["Exempt entity: income is exempt if registered and applied to objects / per its exemption; "
                          "violations or non-specified investments can make income taxable at MMR."],
                "ruleset_version": rules.get("version")}
    normal = float(payload.get("normal_income") or 0)
    dividend = min(float(payload.get("dividend_income") or 0), max(normal, 0))
    eng_id = _resolve_engine(rules, entity)[0]
    nonresident = eng_id in ("individual_nri", "huf_nonresident")
    country = None
    if payload.get("dtaa_country") and (nonresident or prof["kind"] == "fpi"):
        country = next((c for c in rules.get("dtaa", {}).get("countries", []) if c["id"] == payload["dtaa_country"]), None)
    nr_dividend = 0.0
    nr_div_rate = 0.0
    if nonresident and dividend > 0:
        act = float(rules.get("dtaa", {}).get("act_rates_nonresident", {}).get("dividend", 20))
        treaty = float(country["dividend"]["individual_effective"]) if country else act
        nr_div_rate = min(act, treaty)
        nr_dividend, normal, dividend = dividend, normal - dividend, 0.0
    exemption_112A = float(rules["capital_gains"]["common"].get("ltcg_112A_exemption", 125000))
    notes: List[str] = []

    # 1. classify
    lines = []
    for g in payload.get("gains", []):
        c = classify_gain(rules, g["asset_id"], g["purchase_date"], g["sale_date"], listed=g.get("listed"),
                          original_subscriber=g.get("original_subscriber", False),
                          held_to_maturity=g.get("held_to_maturity", False), sif_class=g.get("sif_class", ""),
                          ulip_equity=g.get("ulip_equity", True), promoter=g.get("promoter", ""),
                          entity="individual" if _resolve_engine(rules, entity)[0] in ("individual_resident", "individual_nri") else entity)
        if country and country.get("mf_units_residence_only") and payload.get("apply_mf_treaty") \
                and c["asset_used"] in MF_UNIT_ASSETS and not c["exempt"]:
            c = {**c, "exempt": True, "setoff": False,
                 "reason": f"{country['name']} DTAA residuary clause (tribunal view): MF-unit gains taxable only in the country of residence."}
        lines.append({**c, "gain": float(g.get("gain") or 0), "label": g.get("label") or g["asset_id"]})

    # 2. bucket: key = (term, rate, pool)
    def key(l):
        return (l["term"], str(l["rate"]), l["pool_112A"], "vda" if l.get("vda") else "cg")
    buckets: Dict[tuple, float] = {}
    exempt_total = 0.0
    vda_losses_ignored = 0.0
    for l in lines:
        if l["exempt"]:
            exempt_total += max(l["gain"], 0)
            continue
        if not l["setoff"] and l["gain"] < 0:
            vda_losses_ignored += -l["gain"]
            continue
        buckets[key(l)] = buckets.get(key(l), 0.0) + l["gain"]

    # 3. set-off within and across buckets
    slabs = prof.get("slabs")
    marg = (marginal_slab_rate(normal, slabs) if slabs else prof.get("slab_gain_rate", prof.get("rate", 30)))

    def eff_rate(k):
        r = k[1]
        return marg if r == "slab" else float(r)

    losses_st = -sum(v for k, v in buckets.items() if k[0] == "ST" and v < 0)
    losses_lt = -sum(v for k, v in buckets.items() if k[0] == "LT" and v < 0)
    bf = payload.get("brought_forward") or {}
    losses_st += float(bf.get("stcl") or 0)
    losses_lt += float(bf.get("ltcl") or 0)
    gains = {k: v for k, v in buckets.items() if v > 0}
    setoff_log = []

    def absorb(loss, keys):
        for k in keys:
            if loss <= 0:
                break
            take = min(loss, gains.get(k, 0))
            if take > 0:
                gains[k] -= take
                loss -= take
                setoff_log.append({"against": f"{k[0]} @ {k[1]}{' (112A pool)' if k[2] else ''}", "amount": round(take)})
        return loss

    st_keys = sorted([k for k in gains if k[0] == "ST"], key=eff_rate, reverse=True)
    lt_keys = sorted([k for k in gains if k[0] in ("LT",)], key=lambda k: (k[2], -eff_rate(k)))  # non-pool first
    flat_keys = [k for k in gains if k[0] == "flat"]
    losses_st = absorb(losses_st, st_keys + lt_keys + flat_keys)
    losses_lt = absorb(losses_lt, lt_keys)
    carry_forward = {"stcl": round(losses_st), "ltcl": round(losses_lt)}

    # 4. 112A exemption
    pool_keys = [k for k in gains if k[2]]
    pool_total = sum(gains[k] for k in pool_keys)
    exemption_used = min(exemption_112A, pool_total)
    remaining_ex = exemption_used
    for k in pool_keys:
        take = min(remaining_ex, gains[k])
        gains[k] -= take
        remaining_ex -= take

    # 5. split slab-rate gains into normal income
    slab_gains = sum(v for k, v in gains.items() if k[1] == "slab")
    special = {k: v for k, v in gains.items() if k[1] != "slab" and v > 0}
    normal_total = normal + slab_gains

    # 6. basic exemption shortfall (resident individual / HUF)
    shortfall_used = 0.0
    if prof["kind"] == "slab" and prof["shortfall"]:
        basic = slabs[0][1] if slabs[0][2] == 0 else 0
        shortfall = max(0.0, basic - normal_total)
        for k in sorted([k for k in special if k[3] != "vda"], key=lambda k: -float(k[1])):
            if shortfall <= 0:
                break
            take = min(shortfall, special[k])
            special[k] -= take
            shortfall -= take
            shortfall_used += take

    # 7. tax components
    if prof["kind"] == "slab":
        tax_normal = slab_tax(normal_total, slabs)
    elif prof["kind"] == "fpi":
        tax_normal = normal * prof["rate"] / 100 + slab_gains * prof["slab_gain_rate"] / 100
        notes.append("FPI: interest/dividend at 20%, non-equity short-term gains at 30% (115AD); DTAA may lower.")
    else:
        tax_normal = normal_total * prof["rate"] / 100
    tax_special_cg = sum(v * float(k[1]) / 100 for k, v in special.items() if k[3] != "vda") + nr_dividend * nr_div_rate / 100
    tax_vda = sum(v * float(k[1]) / 100 for k, v in special.items() if k[3] == "vda")
    special_income = sum(v for k, v in special.items()) + nr_dividend
    total_income = normal_total + special_income

    # 8. rebate (resident individual)
    rebate = 0.0
    if prof.get("rebate") and total_income <= prof["rebate"]["income_limit"]:
        if prof["regime"] == "new_regime":
            rebate = min(prof["rebate"]["max_rebate"], tax_normal)          # not on special-rate income
        else:
            stcg_part = sum(v * float(k[1]) / 100 for k, v in special.items() if k[0] == "ST" and k[3] != "vda")
            rebate = min(prof["rebate"]["max_rebate"], tax_normal + stcg_part)  # not on 112A / other LTCG
    elif prof.get("rebate") and prof["regime"] == "new_regime" and prof["rebate"].get("marginal_relief") \
            and special_income == 0 and total_income <= prof["rebate"]["income_limit"] + 100000:
        excess = total_income - prof["rebate"]["income_limit"]
        if tax_normal > excess:
            rebate = tax_normal - excess
            notes.append("Marginal relief on the ₹12 lakh rebate applied.")

    tax_before_sc = max(0.0, tax_normal + tax_special_cg + tax_vda - rebate)

    # 9. surcharge (with 15% cap on CG + dividend portion for individual-like)
    sc = prof["surcharge"]
    sc_rate = _surcharge_rate(sc, total_income, prof["regime"])
    cap = None if prof.get("no_special_cap") else sc.get("special_income_cap")
    tax_normal_net = max(0.0, tax_normal - rebate)
    div_share = (tax_normal_net * dividend / normal_total) if normal_total > 0 else 0
    if cap is not None:
        capped_part = tax_special_cg + div_share
        surcharge = (tax_normal_net - div_share + tax_vda) * sc_rate / 100 + capped_part * min(sc_rate, cap) / 100
    else:
        surcharge = tax_before_sc * sc_rate / 100

    # marginal relief on surcharge (approximation: excess reduced from normal income first)
    marginal_relief = 0.0
    t = _threshold_below(sc, total_income) if "bands" in sc else None
    if t and sc_rate > 0:
        lower_rate = 0
        for threshold, r in sc["bands"]:
            if t > threshold:
                lower_rate = r
        if prof["regime"] == "new_regime" and sc.get("new_regime_cap") is not None:
            lower_rate = min(lower_rate, sc["new_regime_cap"])
        excess = total_income - t
        if prof["kind"] == "slab":
            tax_at_t = slab_tax(max(0.0, normal_total - excess), slabs) + tax_special_cg + tax_vda
        elif prof["kind"] == "fpi":
            tax_at_t = max(0.0, tax_normal - excess * prof["rate"] / 100) + tax_special_cg + tax_vda
        else:
            tax_at_t = max(0.0, normal_total - excess) * prof["rate"] / 100 + tax_special_cg + tax_vda
        ceiling = tax_at_t * (1 + lower_rate / 100) + excess
        if tax_before_sc + surcharge > ceiling:
            marginal_relief = tax_before_sc + surcharge - ceiling
            surcharge -= marginal_relief
            notes.append("Marginal relief on surcharge applied (approximate).")

    cess = (tax_before_sc + surcharge) * rules["meta"]["cess_pct"] / 100
    total = tax_before_sc + surcharge + cess

    # notes
    if entity == "trust_private_discretionary":
        notes.append(rules["entities"]["trust_private_discretionary"]["open_question"])
    if prof["regime"] in ("normal", "normal_small") and "ompany" in prof["label"]:
        notes.append("MAT at 14% of book profit (final tax) may apply — not computed here.")
    if dividend:
        notes.append("Surcharge cap on dividends computed on a pro-rata share of normal tax.")
    if nr_dividend:
        notes.append(f"Non-resident dividends taxed at {nr_div_rate:g}%" + (f" ({country['name']} treaty vs Act 20%, lower applies)." if country else " under the Act."))
    if country:
        notes.append(f"Treaty country: {country['name']}. Needs TRC + Form 10F. Capital gains on shares: {country['cg_shares']}")
        if country.get("mf_units_residence_only") and not payload.get("apply_mf_treaty"):
            notes.append(f"{country['name']} residents may claim MF-unit gains as not taxable in India (tribunal view) — switch on to see the effect.")
    if vda_losses_ignored:
        notes.append(f"VDA losses of ₹{vda_losses_ignored:,.0f} ignored (no set-off allowed).")
    if any(l["asset_used"] == "real_estate" for l in lines) and entity in ("individual_resident", "huf"):
        notes.append("Land/building bought before 23 Jul 2024: compare with 20%-with-indexation option.")

    return {
        "entity": entity, "entity_label": prof["label"], "regime": prof["regime"],
        "lines": lines,
        "set_off": setoff_log, "carry_forward": carry_forward,
        "exemption_112A_used": round(exemption_used), "basic_exemption_used_vs_cg": round(shortfall_used),
        "exempt_gains": round(exempt_total),
        "income": {"normal_incl_slab_gains": round(normal_total), "special_rate_gains": round(special_income),
                   "total_taxable": round(total_income)},
        "tax": {"normal": round(tax_normal), "special_cg": round(tax_special_cg), "vda": round(tax_vda),
                "rebate": round(rebate), "surcharge_rate": sc_rate, "surcharge": round(surcharge),
                "marginal_relief": round(marginal_relief), "cess": round(cess), "total": round(total)},
        "tax_on_gains_only": None,
        "effective_rate_pct": round(total / total_income * 100, 2) if total_income > 0 else 0.0,
        "notes": notes,
        "ruleset_version": rules.get("version"),
    }


def tax_on_gains(rules: dict, payload: dict) -> dict:
    """Incremental tax caused by the gains = tax(with gains) - tax(without gains)."""
    with_g = compute_tax(rules, payload)
    without = compute_tax(rules, {**payload, "gains": []})
    with_g["tax_on_gains_only"] = with_g["tax"]["total"] - without["tax"]["total"]
    return with_g


def harvest_headroom(rules: dict, payload: dict) -> dict:
    """payload: {realised_112A_net: x, lots: [{label, purchase_date, unrealised_gain, value}], as_of?}
    Returns how much equity LTCG can still be booked tax-free this tax year, using LT lots first."""
    ex = float(rules["capital_gains"]["common"].get("ltcg_112A_exemption", 125000))
    used = max(0.0, float(payload.get("realised_112A_net") or 0))
    room = max(0.0, ex - used)
    as_of = _d(payload.get("as_of") or dt.date.today())
    plan, left = [], room
    for lot in sorted(payload.get("lots", []), key=lambda l: _d(l["purchase_date"])):
        lt = as_of > _add_months(_d(lot["purchase_date"]), 12)
        g = float(lot.get("unrealised_gain") or 0)
        if not lt or g <= 0 or left <= 0:
            continue
        book = min(g, left)
        frac = book / g
        plan.append({"label": lot.get("label"), "book_gain": round(book),
                     "redeem_value": round(float(lot.get("value") or 0) * frac)})
        left -= book
    saving = (room - left) * 12.5 / 100 * (1 + rules["meta"]["cess_pct"] / 100)
    return {"exemption": ex, "already_used": round(used), "headroom": round(room),
            "plan": plan, "unused_after_plan": round(left),
            "tax_saved_now": round(saving),
            "note": "Sell and buy back to reset cost; gains booked within the ₹1.25 lakh exemption are tax-free."}
