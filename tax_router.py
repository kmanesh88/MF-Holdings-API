"""
Vantage · Fluid Tax — API + update agent.

Layers:
  base     tax_rules.py (hardcoded, versioned in git)
  overlay  Firestore tax_rules/live  {version, patches:[{path, value, update_id, approved_by, approved_at, summary}]}
  pending  Firestore tax_updates/{id} proposals from the agent, applied only when an advisor approves

Agent: Claude + web search checks official sources for changes / verifies flagged items, and files proposals.
Triggered weekly by GitHub Actions (POST /tax/agent/run with X-Agent-Secret) or from the app.

Mount in main.py:
    from tax_router import router as tax_router
    app.include_router(tax_router)

Env:
    ANTHROPIC_API_KEY
    FIREBASE_SERVICE_ACCOUNT_JSON
    TAX_AGENT_SECRET      (optional; falls back to UPLOAD_SECRET)
    TAX_AGENT_MODEL       (optional; default claude-sonnet-5-5)
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import os
import re
import time
import uuid
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, Header, HTTPException
from pydantic import BaseModel

import tax_engine as E
from tax_rules import BASE_VERSION, base_ruleset

log = logging.getLogger("tax")
router = APIRouter(prefix="/tax", tags=["tax"])

AGENT_MODEL = os.environ.get("TAX_AGENT_MODEL", "claude-sonnet-5-5")
OFFICIAL_DOMAINS = ("incometaxindia.gov.in", "incometax.gov.in", "indiabudget.gov.in", "egazette.gov.in",
                    "pib.gov.in", "sebi.gov.in", "rbi.org.in", "pfrda.org.in", "npstrust.org.in",
                    "finmin.gov.in", "cbdt.gov.in", "dea.gov.in", "sansad.in", "prsindia.org")
TRUSTED_SECONDARY = ("taxmann.com", "taxguru.in", "business-standard.com", "economictimes.indiatimes.com",
                     "livemint.com", "thehindubusinessline.com", "ey.com", "kpmg.com", "pwc.in", "deloitte.com",
                     "grantthornton.in", "cleartax.in", "valueresearchonline.com", "amfiindia.com",
                     "icicipruamc.com", "edelweissmf.com", "nism.ac.in", "moneycontrol.com", "businesstoday.in")


# ------------------------------------------------------------------ firebase
def _db():
    import firebase_admin
    from firebase_admin import credentials, firestore
    if not firebase_admin._apps:
        firebase_admin.initialize_app(credentials.Certificate(json.loads(os.environ["FIREBASE_SERVICE_ACCOUNT_JSON"])))
    return firestore.client()


def _now():
    return dt.datetime.now(dt.timezone.utc)


_overlay_cache: Dict[str, Any] = {"data": None, "ts": 0.0}


def _overlay(force=False) -> Optional[dict]:
    if not force and _overlay_cache["data"] is not None and time.time() - _overlay_cache["ts"] < 300:
        return _overlay_cache["data"]
    try:
        snap = _db().collection("tax_rules").document("live").get()
        data = snap.to_dict() if snap.exists else {"patches": []}
    except Exception as e:  # Firestore down → base rules still work
        log.warning("tax overlay unavailable: %s", e)
        data = _overlay_cache["data"] or {"patches": []}
    _overlay_cache.update(data=data, ts=time.time())
    return data


def rules(force=False) -> dict:
    return E.merge_overlay(base_ruleset(), _overlay(force))


# ------------------------------------------------------------------ auth
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
    secret = os.environ.get("TAX_AGENT_SECRET") or os.environ.get("UPLOAD_SECRET")
    if x_agent_secret and secret and x_agent_secret == secret:
        return "scheduler"
    return _advisor(authorization)


# ------------------------------------------------------------------ read endpoints
@router.get("/rules")
def get_rules():
    r = rules()
    pending = 0
    try:
        pending = sum(1 for _ in _db().collection("tax_updates").where("status", "==", "pending").stream())
    except Exception:
        pass
    last_run = None
    try:
        runs = sorted((d.to_dict() for d in _db().collection("tax_agent_runs").stream()),
                      key=lambda x: x.get("started_at") or "", reverse=True)
        last_run = runs[0] if runs else None
        if last_run:
            last_run = {k: (v.isoformat() if isinstance(v, dt.datetime) else v) for k, v in last_run.items()}
    except Exception:
        pass
    r["status"] = {"base_version": BASE_VERSION, "pending_updates": pending, "last_agent_run": last_run,
                   "unverified": len(_unverified_paths(r))}
    return r


@router.get("/rules/{section}")
def get_section(section: str):
    r = rules()
    if section not in r:
        raise HTTPException(404, f"Unknown section. Try one of: {', '.join(k for k in r if not k.startswith('_'))}")
    return {section: r[section], "version": r["version"]}


class ClassifyIn(BaseModel):
    asset_id: str
    purchase_date: str
    sale_date: str
    listed: Optional[bool] = None
    original_subscriber: bool = False
    held_to_maturity: bool = False
    sif_class: str = ""
    promoter: str = ""
    entity: str = "individual_resident"


@router.post("/classify")
def classify(body: ClassifyIn):
    try:
        return E.classify_gain(rules(), body.asset_id, body.purchase_date, body.sale_date,
                               listed=body.listed, original_subscriber=body.original_subscriber,
                               held_to_maturity=body.held_to_maturity, sif_class=body.sif_class,
                               promoter=body.promoter, entity=body.entity)
    except ValueError as e:
        raise HTTPException(400, str(e))


@router.post("/compute")
def compute(payload: Dict[str, Any]):
    try:
        return E.tax_on_gains(rules(), payload)
    except (ValueError, KeyError) as e:
        raise HTTPException(400, str(e))


@router.post("/compare")
def compare(payload: Dict[str, Any]):
    """Same gains across several entities/regimes: payload {base:{...compute payload}, variants:[{entity, regime, ...}]}"""
    base = payload.get("base") or {}
    out = []
    for v in payload.get("variants") or []:
        try:
            r = E.tax_on_gains(rules(), {**base, **v})
            out.append({"variant": v, "total": r["tax"]["total"], "on_gains": r["tax_on_gains_only"],
                        "effective_rate_pct": r["effective_rate_pct"], "label": r["entity_label"], "regime": r["regime"]})
        except (ValueError, KeyError) as e:
            out.append({"variant": v, "error": str(e)})
    return {"results": out}


@router.post("/harvest")
def harvest(payload: Dict[str, Any]):
    return E.harvest_headroom(rules(), payload)


# ------------------------------------------------------------------ updates (human in the loop)
def _ser(d: dict) -> dict:
    return {k: (v.isoformat() if isinstance(v, dt.datetime) else v) for k, v in d.items()}


@router.get("/updates")
def list_updates(status: str = "pending", uid: str = Depends(_advisor)):
    q = _db().collection("tax_updates")
    if status != "all":
        q = q.where("status", "==", status)
    rows = [{"id": d.id, **_ser(d.to_dict())} for d in q.stream()]
    rows.sort(key=lambda r: r.get("proposed_at") or "", reverse=True)
    return rows


class Decision(BaseModel):
    value: Any = None      # optional edited value
    note: str = ""


@router.post("/updates/{uid_}/approve")
def approve(uid_: str, body: Decision, uid: str = Depends(_advisor)):
    db = _db()
    ref = db.collection("tax_updates").document(uid_)
    snap = ref.get()
    if not snap.exists:
        raise HTTPException(404, "Not found")
    u = snap.to_dict()
    if u.get("status") != "pending":
        raise HTTPException(409, f"Already {u.get('status')}")
    value = body.value if body.value is not None else u["proposed_value"]
    # dry-run the patch against current rules
    test = E.merge_overlay(base_ruleset(), {"patches": (_overlay(True).get("patches") or []) +
                                            [{"path": u["path"], "value": value}]})
    if u["path"] in (test.get("_overlay_errors") or []):
        raise HTTPException(400, f"Path does not resolve: {u['path']}")
    live_ref = db.collection("tax_rules").document("live")
    live = live_ref.get().to_dict() if live_ref.get().exists else {"patches": []}
    patches = live.get("patches") or []
    patches.append({"path": u["path"], "value": value, "update_id": uid_, "summary": u.get("summary"),
                    "approved_by": uid, "approved_at": _now().isoformat(), "sources": u.get("sources", [])})
    version = f"{BASE_VERSION}+{len(patches)}@{_now():%Y%m%d}"
    live_ref.set({"version": version, "patches": patches, "updated_at": _now()})
    ref.update({"status": "approved", "decided_by": uid, "decided_at": _now(), "applied_value": value,
                "decision_note": body.note or None})
    _overlay(force=True)
    return {"ok": True, "version": version}


@router.post("/updates/{uid_}/reject")
def reject(uid_: str, body: Decision, uid: str = Depends(_advisor)):
    ref = _db().collection("tax_updates").document(uid_)
    if not ref.get().exists:
        raise HTTPException(404, "Not found")
    ref.update({"status": "rejected", "decided_by": uid, "decided_at": _now(), "decision_note": body.note or None})
    return {"ok": True}


@router.get("/overlay")
def get_overlay(uid: str = Depends(_advisor)):
    return _overlay(True)


@router.delete("/overlay/{index}")
def revert_patch(index: int, uid: str = Depends(_advisor)):
    db = _db()
    live_ref = db.collection("tax_rules").document("live")
    live = live_ref.get().to_dict() or {"patches": []}
    patches = live.get("patches") or []
    if not 0 <= index < len(patches):
        raise HTTPException(404, "No such patch")
    removed = patches.pop(index)
    live_ref.set({"version": f"{BASE_VERSION}+{len(patches)}@{_now():%Y%m%d}", "patches": patches, "updated_at": _now()})
    if removed.get("update_id"):
        db.collection("tax_updates").document(removed["update_id"]).update({"status": "reverted", "reverted_by": uid,
                                                                             "reverted_at": _now()})
    _overlay(force=True)
    return {"ok": True, "removed": removed}


# ------------------------------------------------------------------ agent
def _flatten(node, prefix="", out=None):
    """Flat 'path = value' list with id/item selectors for lists of dicts."""
    out = [] if out is None else out
    if isinstance(node, dict):
        for k, v in node.items():
            if k in ("sources", "status", "_overlay_errors", "overlay_applied", "src"):
                continue
            _flatten(v, f"{prefix}.{k}" if prefix else k, out)
    elif isinstance(node, list) and node and all(isinstance(x, dict) for x in node):
        for i, item in enumerate(node):
            sel = None
            for key in ("id", "item", "what"):
                if key in item and "=" not in str(item[key]) and "]" not in str(item[key]) and "." not in str(item[key]):
                    sel = f"[{key}={item[key]}]"
                    break
            _flatten(item, f"{prefix}{sel or f'[{i}]'}", out)
    else:
        out.append((prefix, node))
    return out


def _unverified_paths(r: dict) -> List[str]:
    return [p[:-len(".verified")] for p, v in _flatten(r) if p.endswith(".verified") and v is False]


AGENT_SYSTEM = """You are the tax-law update agent for an Indian wealth-advisory app used by advisors in client meetings.
You maintain a ruleset of Indian direct-tax rules for investments and taxpayer entities (individual, NRI, HUF, AOP/BOI,
trusts, firms/LLPs, companies). Accuracy matters more than coverage: a wrong rate shown to a client is worse than a missing one.

Your job on each run:
1. CHANGES: search for changes in Indian direct tax that affect the ruleset since the date given — Finance Acts/Bills,
   Taxation Laws amendments, CBDT circulars and notifications, Income-tax Rules, SEBI changes that alter MF/SIF/AIF tax
   classification, RBI changes (SGB, bonds), PFRDA changes (NPS), and binding clarifications.
2. VERIFY: for items flagged unverified, confirm or correct them from authoritative sources.

Source rules:
- Prefer official sources (incometaxindia.gov.in, incometax.gov.in, indiabudget.gov.in, egazette.gov.in, pib.gov.in,
  sebi.gov.in, rbi.org.in, pfrda.org.in). Reputable secondary sources (Big-4, Taxmann, TaxGuru, Business Standard,
  Economic Times, Mint, AMC tax reckoners) are acceptable only when they agree with each other.
- A proposal (Budget speech / Finance Bill) is NOT law until enacted. Mark change_type "proposed" if not yet enacted.
- Quote the exact supporting sentence (≤ 30 words) for every proposal.

Output: ONLY a JSON object, no prose, no code fences:
{"checked_through": "YYYY-MM-DD",
 "proposals": [
   {"path": "<exact path from the ruleset listing, or a parent list path ending in [+] to add an item>",
    "current_value": <value as in listing or null>,
    "proposed_value": <new JSON value>,
    "change_type": "update" | "verify" | "proposed" | "add",
    "summary": "<one line, plain English, what changes for investors>",
    "effective_from": "YYYY-MM-DD or null",
    "sources": [{"url": "...", "quote": "..."}],
    "confidence": "high" | "medium" | "low"}],
 "no_change_checked": ["<areas you checked and found unchanged>"]}

For a VERIFY that confirms the current value, propose path "<block>.verified" with proposed_value true, and include the
source. If a verified value is wrong, propose the corrected value at its exact path AND "<block>.verified": true.
Use exact paths only. Never invent a path. If nothing changed and nothing could be verified, return an empty proposals list."""


def _call_agent(prompt: str) -> str:
    import anthropic
    client = anthropic.Anthropic()
    messages = [{"role": "user", "content": prompt}]
    tools = [{"type": "web_search_20250305", "name": "web_search", "max_uses": 30}]
    text = ""
    for _ in range(6):   # continue through pause_turn
        resp = client.messages.create(model=AGENT_MODEL, max_tokens=16000, system=AGENT_SYSTEM,
                                      tools=tools, messages=messages)
        text = "".join(getattr(b, "text", "") for b in resp.content if getattr(b, "type", "") == "text")
        if resp.stop_reason != "pause_turn":
            break
        messages.append({"role": "assistant", "content": resp.content})
    return text


def _parse(text: str) -> dict:
    t = re.sub(r"^```(?:json)?\s*|\s*```$", "", text.strip())
    try:
        return json.loads(t)
    except json.JSONDecodeError:
        a, b = t.find("{"), t.rfind("}")
        if a >= 0 and b > a:
            return json.loads(t[a:b + 1])
        raise


def _tier(url: str) -> str:
    host = re.sub(r"^https?://", "", url or "").split("/")[0].lower()
    if any(host.endswith(d) for d in OFFICIAL_DOMAINS):
        return "official"
    if any(host.endswith(d) for d in TRUSTED_SECONDARY):
        return "secondary"
    return "other"


def run_agent(run_id: str, mode: str = "all"):
    db = _db()
    run_ref = db.collection("tax_agent_runs").document(run_id)
    try:
        r = rules(force=True)
        flat = _flatten(r)
        prev = [d.to_dict() for d in db.collection("tax_agent_runs").where("status", "==", "done").stream()]
        since = max((p.get("checked_through") or "" for p in prev), default="") or r["meta"]["period"]["from"]
        unverified = _unverified_paths(r)
        listing = "\n".join(f"{p} = {json.dumps(v, ensure_ascii=False)}" for p, v in flat)
        task = []
        if mode in ("all", "changes"):
            task.append(f"CHANGES: find enacted or proposed changes since {since} (today is {dt.date.today()}).")
        if mode in ("all", "verify"):
            task.append("VERIFY these unverified blocks (up to 25 this run, most investor-relevant first; skip ones verified in earlier runs):\n"
                        + "\n".join(f"- {u}" for u in unverified))
        prompt = (f"Ruleset version {r['version']} for tax year {r['meta']['tax_year']}.\n\n"
                  + "\n\n".join(task) + "\n\nRULESET LISTING (path = value):\n" + listing)
        text = _call_agent(prompt)
        out = _parse(text)
        valid_paths = {p for p, _ in flat}
        existing = {(d.get("path"), json.dumps(d.get("proposed_value"), sort_keys=True))
                    for d in (x.to_dict() for x in db.collection("tax_updates").where("status", "==", "pending").stream())}
        filed, skipped = 0, []
        for p in out.get("proposals", []):
            path = p.get("path", "")
            ok_path = path in valid_paths or path.endswith("[+]") or path.endswith(".verified") or \
                any(vp.startswith(path + ".") for vp in valid_paths)
            srcs = [s for s in (p.get("sources") or []) if s.get("url")]
            if not ok_path or not srcs:
                skipped.append({"path": path, "why": "bad path" if not ok_path else "no source"})
                continue
            sig = (path, json.dumps(p.get("proposed_value"), sort_keys=True))
            if sig in existing:
                continue
            tiers = [_tier(s["url"]) for s in srcs]
            conf = p.get("confidence", "medium")
            if "official" not in tiers and conf == "high":
                conf = "medium"
            if all(t == "other" for t in tiers):
                conf = "low"
            uid_ = uuid.uuid4().hex[:16]
            db.collection("tax_updates").document(uid_).set({
                "status": "pending", "run_id": run_id, "proposed_at": _now(),
                "path": path, "current_value": p.get("current_value"), "proposed_value": p.get("proposed_value"),
                "change_type": p.get("change_type", "update"), "summary": p.get("summary", ""),
                "effective_from": p.get("effective_from"), "confidence": conf,
                "sources": [{**s, "tier": _tier(s["url"])} for s in srcs],
            })
            existing.add(sig)
            filed += 1
        run_ref.update({"status": "done", "finished_at": _now(), "filed": filed, "skipped": skipped,
                        "checked_through": out.get("checked_through") or str(dt.date.today()),
                        "no_change_checked": out.get("no_change_checked", [])[:30]})
    except Exception as e:  # noqa: BLE001
        log.exception("tax agent run failed")
        run_ref.update({"status": "failed", "finished_at": _now(), "error": str(e)[:500]})


@router.post("/agent/run")
def agent_run(bg: BackgroundTasks, mode: str = "all", who: str = Depends(_agent_or_advisor)):
    if mode not in ("all", "changes", "verify"):
        raise HTTPException(400, "mode must be all | changes | verify")
    db = _db()
    running = [d for d in db.collection("tax_agent_runs").where("status", "==", "running").stream()]
    for d in running:   # treat runs older than 30 min as dead
        st = d.to_dict().get("started_at")
        if st and (_now() - st).total_seconds() < 1800:
            return {"ok": True, "run_id": d.id, "status": "already_running"}
        d.reference.update({"status": "failed", "error": "timed out"})
    run_id = f"{_now():%Y%m%d%H%M%S}-{uuid.uuid4().hex[:6]}"
    db.collection("tax_agent_runs").document(run_id).set({"status": "running", "started_at": _now(),
                                                          "triggered_by": who, "mode": mode})
    bg.add_task(run_agent, run_id, mode)
    return {"ok": True, "run_id": run_id, "status": "started"}


@router.get("/agent/runs")
def agent_runs(uid: str = Depends(_advisor)):
    rows = [{"id": d.id, **_ser(d.to_dict())} for d in _db().collection("tax_agent_runs").stream()]
    rows.sort(key=lambda r: r.get("started_at") or "", reverse=True)
    return rows[:20]
