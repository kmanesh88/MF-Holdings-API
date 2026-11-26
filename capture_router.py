"""
Vantage · Capture module
Recording engine that builds Fluid's real-world, multilingual conversation dataset.

Pipeline:
  upload (any audio) -> ffmpeg to 16 kHz mono mp3 -> Firebase Storage
  -> Sarvam batch STT (codemix + diarization + keyterms)
  -> Claude: English translation + intent / emotion / entity tags + advisor-speaker guess
  -> Firestore: capture_sessions/{sid} + capture_sessions/{sid}/utterances/{00000}
  -> advisor reviews / corrects tags in capture.html -> export JSONL dataset

Mount in main.py:
    from capture_router import router as capture_router
    app.include_router(capture_router)

Env vars:
    FIREBASE_SERVICE_ACCOUNT_JSON   (already on Render)
    FIREBASE_STORAGE_BUCKET         optional, defaults to <project_id>.firebasestorage.app
    SARVAM_API_KEY
    ANTHROPIC_API_KEY
    SARVAM_STT_MODEL                optional, default saaras:v4
    CAPTURE_TAG_MODEL               optional, default claude-sonnet-5-5
"""
import os
import re
import json
import logging
import tempfile
import subprocess
import datetime as dt
from typing import Optional, List, Dict

from fastapi import (APIRouter, UploadFile, File, Form, Header, HTTPException,
                     BackgroundTasks, Depends, Query)
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

import firebase_admin
from firebase_admin import credentials, firestore, auth as fb_auth, storage

log = logging.getLogger("capture")
router = APIRouter(prefix="/capture", tags=["capture"])

# ---------------------------------------------------------------- config
SCHEMA_VERSION = 1
COL = "capture_sessions"
STT_MODEL = os.environ.get("SARVAM_STT_MODEL", "saaras:v4")
TAG_MODEL = os.environ.get("CAPTURE_TAG_MODEL", "claude-sonnet-5-5")
MAX_SECONDS = 2 * 60 * 60          # Sarvam batch limit per file
TAG_CHUNK = 40                     # utterances per Claude call
CHANNELS = {"in_person", "phone", "video"}
ROLES = {"advisor", "client", "colleague", "other"}
CONVERSATION_TYPES = {"client", "team"}
PROCESSING = {"uploaded", "transcribing", "tagging"}

INTENTS: Dict[str, str] = {
    "stop_sip_pause":        "Wants to stop / pause SIP",
    "redeem_withdraw":       "Wants to redeem or withdraw money",
    "market_fear":           "Worried about market fall / volatility",
    "return_dissatisfaction": "Unhappy with returns",
    "fund_performance":      "Asks why a fund / category is doing well or badly",
    "compare_safe_assets":   "Compares with FD, gold, real estate, insurance, other 'safe' options",
    "new_investment":        "Has money to invest / wants to add",
    "product_inquiry":       "Asks about a specific product (PMS, AIF, SIF, NFO, stock, bond, NPS)",
    "tax_query":             "General tax question: how much tax, slab, regime, ELSS / 80C",
    "tax_harvesting":        "Booking gains/losses for tax: ₹1.25 lakh harvesting, set-off, timing a redemption",
    "tax_crossborder":       "NRI / OCI / returning NRI / DTAA / TDS / US-UK home-country tax (PFIC etc.)",
    "tax_structure":         "Which entity should hold it: HUF, trust, LLP, company, gifting, clubbing",
    "goal_planning":         "Talks about goals: education, retirement, house, marriage",
    "liquidity_need":        "Upcoming need for cash / emergency",
    "risk_appetite":         "States how much risk they can or can't take",
    "trust_service":         "Complaint or doubt about advisor, platform or service",
    "third_party_opinion":   "Cites friend, relative, YouTube, news or tip",
    "fees_costs":            "Expense ratio, commission, charges",
    "admin_request":         "KYC, nominee, statements, mandate, address change",
    "small_talk":            "Rapport / non-finance chat",
    "acknowledgement":       "Ok / yes / understood with no new content",
    "advisor_turn":          "Said by the advisor",
    "other":                 "Nothing above fits",
}
# Intent → Fluid cards that should appear (lets the dataset score and train Fluid's triggers)
FLUID_CARDS = {
    "stop_sip_pause": ["f_bear_market", "f_sip_lumpsum"], "market_fear": ["f_bear_market", "f_market_valuation"],
    "redeem_withdraw": ["f_bear_market", "f_tax_calc"], "return_dissatisfaction": ["f_bear_market"],
    "compare_safe_assets": ["f_inflation", "f_asset_class", "f_tax_debt"],
    "tax_query": ["f_tax_equity", "f_tax_changes"], "tax_harvesting": ["f_tax_harvest", "f_tax_equity"],
    "tax_crossborder": ["f_tax_nri", "f_tax_dtaa", "f_tax_home", "f_tax_residency"],
    "tax_structure": ["f_tax_entity", "f_tax_planning"], "fees_costs": [], "third_party_opinion": [],
}

EMOTIONS = ["calm", "curious", "anxious", "frustrated", "skeptical", "excited", "confused"]

# Up to 50 keyterms (Sarvam limit), client-specific fund names first.
BASE_KEYTERMS = [
    "Parag Parikh", "PPFAS", "HDFC", "ICICI Prudential", "SBI", "Kotak", "Axis", "Nippon",
    "Mirae", "Aditya Birla Sun Life", "DSP", "Sundaram", "Tata", "UTI", "Franklin Templeton",
    "Motilal Oswal", "Quant", "Canara Robeco", "Edelweiss", "Invesco", "Bandhan", "HSBC",
    "Baroda BNP Paribas", "Mahindra Manulife", "WhiteOak", "360 ONE", "Scripbox",
    "SIP", "SWP", "STP", "NAV", "XIRR", "NPS", "PMS", "AIF", "SIF", "ELSS", "NFO",
    "arbitrage", "flexi cap", "small cap", "mid cap", "large cap", "liquid fund",
    "gilt", "FD", "LTCG", "STCG",
]

# ---------------------------------------------------------------- clients
_db = None


def _init_firebase():
    if not firebase_admin._apps:
        sa = json.loads(os.environ["FIREBASE_SERVICE_ACCOUNT_JSON"])
        firebase_admin.initialize_app(credentials.Certificate(sa))


def db():
    global _db
    _init_firebase()
    if _db is None:
        _db = firestore.client()
    return _db


def bucket():
    _init_firebase()
    name = os.environ.get("FIREBASE_STORAGE_BUCKET")
    if not name:  # default bucket for projects created after Sept 2024
        name = json.loads(os.environ["FIREBASE_SERVICE_ACCOUNT_JSON"])["project_id"] + ".firebasestorage.app"
    return storage.bucket(name)


def sarvam():
    from sarvamai import SarvamAI
    return SarvamAI(api_subscription_key=os.environ["SARVAM_API_KEY"])


def now():
    return dt.datetime.now(dt.timezone.utc)


# ---------------------------------------------------------------- auth
def current_advisor(authorization: Optional[str] = Header(None)) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "Missing token")
    _init_firebase()
    try:
        return fb_auth.verify_id_token(authorization[7:])["uid"]
    except Exception:
        raise HTTPException(401, "Invalid token")


def _owned(sid: str, uid: str):
    ref = db().collection(COL).document(sid)
    snap = ref.get()
    if not snap.exists or snap.to_dict().get("advisor_id") != uid:
        raise HTTPException(404, "Session not found")
    return ref, snap.to_dict()


# ---------------------------------------------------------------- audio
_TIME_RE = re.compile(r"time=(\d+):(\d+):(\d+(?:\.\d+)?)")
_DUR_RE = re.compile(r"Duration: (\d+):(\d+):(\d+(?:\.\d+)?)")


def _hms(m):
    return int(m[0]) * 3600 + int(m[1]) * 60 + float(m[2])


def to_mp3(src: str, dst: str) -> float:
    """Normalise any input (webm/opus, m4a, amr, wav, mp4...) to 16 kHz mono mp3. Returns seconds."""
    import imageio_ffmpeg
    ff = imageio_ffmpeg.get_ffmpeg_exe()
    p = subprocess.run([ff, "-y", "-i", src, "-vn", "-ac", "1", "-ar", "16000", "-b:a", "48k", dst],
                       capture_output=True, text=True)
    if p.returncode != 0 or not os.path.exists(dst) or os.path.getsize(dst) == 0:
        raise HTTPException(400, "Could not read this audio file")
    times = _TIME_RE.findall(p.stderr)          # progress lines; last one = output length
    if times:
        return round(_hms(times[-1]), 1)
    d = _DUR_RE.search(p.stderr)
    return round(_hms(d.groups()), 1) if d else 0.0


# ---------------------------------------------------------------- keyterms
def keyterms(client_id: Optional[str]) -> List[str]:
    terms: List[str] = []
    if client_id:
        try:
            snap = db().collection("client_holdings").document(client_id).get()
            if snap.exists:
                d = snap.to_dict() or {}
                rows = d.get("holdings") or d.get("funds") or []
                for r in rows:
                    if isinstance(r, dict):
                        name = r.get("fund_name") or r.get("scheme") or r.get("name")
                        if name:
                            terms.append(str(name)[:64])
        except Exception:
            log.warning("keyterms: could not read client_holdings for %s", client_id)
    seen, out = set(), []
    for t in terms[:20] + BASE_KEYTERMS:
        k = t.lower()
        if k not in seen:
            seen.add(k)
            out.append(t)
    return out[:50]


# ---------------------------------------------------------------- pipeline
def process_session(sid: str, stage: str = "all"):
    ref = db().collection(COL).document(sid)
    snap = ref.get()
    if not snap.exists:
        return
    s = snap.to_dict()
    try:
        if stage == "all":
            ref.update({"status": "transcribing", "error": None, "updated_at": now()})
            entries, lang, job_id = transcribe(s)
            write_utterances(ref, entries)
            ref.update({
                "detected_language": lang,
                "sarvam_job_id": job_id,
                "utterance_count": len(entries),
                "speakers": sorted({str(e.get("speaker_id", "0")) for e in entries}),
            })
        ref.update({"status": "tagging", "updated_at": now()})
        tag_session(ref)
        ref.update({"status": "ready", "processed_at": now(), "updated_at": now()})
    except Exception as e:  # noqa: BLE001
        log.exception("capture: session %s failed", sid)
        ref.update({"status": "failed", "error": str(e)[:500], "updated_at": now()})


def transcribe(s: dict):
    folder = os.path.dirname(s["audio_path"])
    with tempfile.TemporaryDirectory() as td:
        local = os.path.join(td, "audio.mp3")
        bucket().blob(s["audio_path"]).download_to_filename(local)

        kwargs = dict(model=STT_MODEL, mode="codemix", with_diarization=True,
                      num_speakers=int(s.get("num_speakers") or 2))
        if s.get("language_hint"):
            kwargs["language_code"] = s["language_hint"]
        terms = keyterms(s.get("client_id"))
        if terms:
            kwargs["keyterms"] = terms

        job = sarvam().speech_to_text_job.create_job(**kwargs)
        job.upload_files(file_paths=[local])
        job.start()
        job.wait_until_complete(poll_interval=10, timeout=3600)

        res = job.get_file_results()
        if not res.get("successful"):
            err = (res.get("failed") or [{}])[0].get("error_message", "unknown error")
            raise RuntimeError(f"Sarvam: {err}")

        out = os.path.join(td, "out")
        os.makedirs(out)
        job.download_outputs(output_dir=out)
        files = [f for f in os.listdir(out) if f.endswith(".json")]
        if not files:
            raise RuntimeError("Sarvam returned no transcript")
        with open(os.path.join(out, files[0]), encoding="utf-8") as f:
            data = json.load(f)

    # keep raw output so we can re-process without paying for STT again
    bucket().blob(f"{folder}/sarvam.json").upload_from_string(
        json.dumps(data, ensure_ascii=False), content_type="application/json")

    entries = (data.get("diarized_transcript") or {}).get("entries") or []
    if not entries:   # no diarization returned: fall back to timestamp chunks, single speaker
        ts = data.get("timestamps") or {}
        entries = [{"transcript": c, "start_time_seconds": a, "end_time_seconds": b, "speaker_id": "0"}
                   for c, a, b in zip(ts.get("chunks", []), ts.get("start_time_seconds", []),
                                      ts.get("end_time_seconds", []))]
    entries = [e for e in entries if (e.get("transcript") or "").strip()]
    return entries, data.get("language_code"), getattr(job, "job_id", None)


def _delete_collection(col):
    while True:
        docs = list(col.limit(400).stream())
        if not docs:
            return
        b = db().batch()
        for d in docs:
            b.delete(d.reference)
        b.commit()


def write_utterances(ref, entries):
    col = ref.collection("utterances")
    _delete_collection(col)
    b, n = db().batch(), 0
    for i, e in enumerate(entries):
        b.set(col.document(f"{i:05d}"), {
            "i": i,
            "speaker_id": str(e.get("speaker_id", "0")),
            "start": float(e.get("start_time_seconds") or 0),
            "end": float(e.get("end_time_seconds") or 0),
            "text_codemix": e["transcript"].strip(),
            "text_en": None,
            "ai_tags": None,
            "human_tags": None,
            "reviewed": False,
        })
        n += 1
        if n == 400:
            b.commit()
            b, n = db().batch(), 0
    if n:
        b.commit()


TAG_SYSTEM = f"""You annotate transcripts of Indian wealth-management conversations between an advisor (RM) and a client, to build a training dataset.

Transcripts are code-mixed: English words in Latin script; Tamil, Malayalam, Hindi or other Indic words in native script. Speech-to-text can mangle fund or company names; correct obvious ones in the translation.

For EVERY utterance under "Annotate", return:
- i: the index shown after #
- en: faithful English translation. Keep tone and hedging ("I'm a little scared", not "I am concerned"). Keep finance terms as spoken (SIP, NAV, FD, small cap).
- intent: one code from the list below. Use "advisor_turn" for anything the advisor says. Use "other" only if nothing fits.
- emotion: one of {", ".join(EMOTIONS)}
- intensity: 1 (mild) to 3 (strong)
- entities: funds, AMCs, stocks, instruments or amounts mentioned, as short strings ([] if none)

Also return advisor_speaker: the speaker id most likely to be the advisor (explains, recommends, asks about goals and risk).

Intent codes:
""" + "\n".join(f"- {k}: {v}" for k, v in INTENTS.items()) + """

Respond with JSON only, no prose, no code fences:
{"advisor_speaker":"0","utterances":[{"i":0,"en":"...","intent":"...","emotion":"...","intensity":1,"entities":[]}]}"""


def _parse_json(text: str) -> dict:
    text = text.strip()
    text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text)
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        a, b = text.find("{"), text.rfind("}")
        if a >= 0 and b > a:
            return json.loads(text[a:b + 1])
        raise


def _tag_chunk(client, chunk, ctx, conversation_type="client") -> dict:
    lines = []
    if conversation_type == "team":
        lines.append("This is an internal TEAM conversation: a senior advisor with relationship managers discussing clients. "
                     "Treat the person explaining / answering as the advisor (intent advisor_turn); tag questions and client "
                     "situations raised by the other speakers with the intent they describe.\n")
    if ctx:
        lines.append("Context (do not annotate):")
        lines += [f"[speaker {u['speaker_id']}] {u['text_codemix']}" for u in ctx]
        lines.append("")
    lines.append("Annotate:")
    lines += [f"#{u['i']} [speaker {u['speaker_id']}] {u['text_codemix']}" for u in chunk]
    msg = client.messages.create(model=TAG_MODEL, max_tokens=8000, system=TAG_SYSTEM,
                                 messages=[{"role": "user", "content": "\n".join(lines)}])
    text = "".join(getattr(b, "text", "") for b in msg.content)
    return _parse_json(text)


def tag_session(ref):
    import anthropic
    client = anthropic.Anthropic()
    docs = sorted((d.to_dict() for d in ref.collection("utterances").stream()), key=lambda u: u["i"])
    if not docs:
        return
    ctype = (ref.get().to_dict() or {}).get("conversation_type", "client")
    votes: Dict[str, int] = {}
    for start in range(0, len(docs), TAG_CHUNK):
        chunk = docs[start:start + TAG_CHUNK]
        ctx = docs[max(0, start - 6):start]
        try:
            result = _tag_chunk(client, chunk, ctx, ctype)
        except Exception:  # one retry, then give up on this chunk only
            log.warning("tag chunk %s failed, retrying", start)
            result = _tag_chunk(client, chunk, ctx, ctype)
        adv = str(result.get("advisor_speaker", ""))
        if adv:
            votes[adv] = votes.get(adv, 0) + len(chunk)
        by_i = {int(u["i"]): u for u in result.get("utterances", []) if "i" in u}
        b = db().batch()
        for u in chunk:
            t = by_i.get(u["i"])
            if not t:
                continue
            intent = t.get("intent") if t.get("intent") in INTENTS else "other"
            emotion = t.get("emotion") if t.get("emotion") in EMOTIONS else "calm"
            try:
                intensity = max(1, min(3, int(t.get("intensity") or 1)))
            except (TypeError, ValueError):
                intensity = 1
            b.update(ref.collection("utterances").document(f"{u['i']:05d}"), {
                "text_en": t.get("en"),
                "ai_tags": {"intent": intent, "emotion": emotion, "intensity": intensity,
                            "entities": [str(x) for x in (t.get("entities") or [])][:10]},
            })
        b.commit()

    session = ref.get().to_dict()
    if session.get("speaker_map_source") != "human":
        speakers = sorted({u["speaker_id"] for u in docs})
        advisor = max(votes, key=votes.get) if votes else speakers[0]
        other = "colleague" if ctype == "team" else "client"
        ref.update({"speaker_map": {sp: ("advisor" if sp == advisor else other) for sp in speakers},
                    "speaker_map_source": "ai"})


# ---------------------------------------------------------------- endpoints
@router.get("/taxonomy")
def taxonomy():
    return {"intents": INTENTS, "emotions": EMOTIONS, "channels": sorted(CHANNELS), "roles": sorted(ROLES)}


@router.post("/sessions")
async def create_session(bg: BackgroundTasks,
                         file: UploadFile = File(...),
                         client_id: str = Form(""),
                         channel: str = Form("in_person"),
                         language_hint: str = Form(""),
                         num_speakers: int = Form(2),
                         recorded_at: str = Form(""),
                         source: str = Form("recorded"),
                         conversation_type: str = Form("client"),
                         uid: str = Depends(current_advisor)):
    if channel not in CHANNELS:
        raise HTTPException(400, f"channel must be one of {sorted(CHANNELS)}")
    ref = db().collection(COL).document()
    sid = ref.id
    ext = os.path.splitext(file.filename or "")[1][:8] or ".bin"
    with tempfile.TemporaryDirectory() as td:
        raw = os.path.join(td, "raw" + ext)
        with open(raw, "wb") as f:
            while chunk := await file.read(1 << 20):
                f.write(chunk)
        mp3 = os.path.join(td, "audio.mp3")
        secs = await run_in_threadpool(to_mp3, raw, mp3)
        if secs > MAX_SECONDS:
            raise HTTPException(400, "Recording is longer than 2 hours; split it and upload in parts")
        if 0 < secs < 3:
            raise HTTPException(400, "Recording is too short")
        path = f"capture/{uid}/{sid}/audio.mp3"
        await run_in_threadpool(lambda: bucket().blob(path).upload_from_filename(mp3, content_type="audio/mpeg"))

    ref.set({
        "schema_version": SCHEMA_VERSION,
        # commercial-readiness slots: unused for now, populated at launch
        "org_id": None,
        "consent": None,
        # core
        "advisor_id": uid,
        "client_id": client_id.strip() or None,
        "channel": channel,
        "conversation_type": conversation_type if conversation_type in CONVERSATION_TYPES else "client",
        "source": source if source in {"recorded", "uploaded"} else "recorded",
        "language_hint": language_hint or None,
        "detected_language": None,
        "num_speakers": max(1, min(6, num_speakers)),
        "audio_path": path,
        "duration_sec": secs,
        "recorded_at": recorded_at or now().isoformat(),
        "status": "uploaded",
        "error": None,
        "speaker_map": None,
        "speaker_map_source": None,
        "utterance_count": 0,
        "created_at": now(),
        "updated_at": now(),
    })
    bg.add_task(process_session, sid)
    return {"session_id": sid, "duration_sec": secs, "status": "uploaded"}


def _session_out(sid, d):
    d = dict(d)
    d["id"] = sid
    for k in ("created_at", "updated_at", "processed_at"):
        if isinstance(d.get(k), dt.datetime):
            d[k] = d[k].isoformat()
    return d


@router.get("/sessions")
def list_sessions(uid: str = Depends(current_advisor)):
    snaps = db().collection(COL).where("advisor_id", "==", uid).stream()
    rows = [_session_out(s.id, s.to_dict()) for s in snaps]
    rows.sort(key=lambda r: r.get("created_at") or "", reverse=True)
    for r in rows:   # reviewed counts are cheap enough at this scale
        if r["status"] == "ready":
            ref = db().collection(COL).document(r["id"]).collection("utterances")
            r["reviewed_count"] = sum(1 for _ in ref.where("reviewed", "==", True).stream())
    return rows


@router.get("/sessions/{sid}")
def get_session(sid: str, uid: str = Depends(current_advisor)):
    ref, d = _owned(sid, uid)
    utts = sorted((u.to_dict() for u in ref.collection("utterances").stream()), key=lambda u: u["i"])
    return {"session": _session_out(sid, d), "utterances": utts}


@router.get("/sessions/{sid}/audio")
def audio_url(sid: str, uid: str = Depends(current_advisor)):
    _, d = _owned(sid, uid)
    url = bucket().blob(d["audio_path"]).generate_signed_url(
        expiration=dt.timedelta(hours=2), version="v4", method="GET")
    return {"url": url}


class SpeakerMap(BaseModel):
    speaker_map: Dict[str, str]


@router.patch("/sessions/{sid}/speakers")
def set_speakers(sid: str, body: SpeakerMap, uid: str = Depends(current_advisor)):
    ref, _ = _owned(sid, uid)
    if any(v not in ROLES for v in body.speaker_map.values()):
        raise HTTPException(400, f"roles must be in {sorted(ROLES)}")
    ref.update({"speaker_map": body.speaker_map, "speaker_map_source": "human", "updated_at": now()})
    return {"ok": True}


class TagUpdate(BaseModel):
    intent: str
    emotion: Optional[str] = None
    intensity: Optional[int] = None
    entities: List[str] = []
    text_en: Optional[str] = None
    note: Optional[str] = None


@router.patch("/sessions/{sid}/utterances/{i}")
def review_utterance(sid: str, i: int, body: TagUpdate, uid: str = Depends(current_advisor)):
    ref, _ = _owned(sid, uid)
    uref = ref.collection("utterances").document(f"{i:05d}")
    if not uref.get().exists:
        raise HTTPException(404, "Utterance not found")
    intent = re.sub(r"[^a-z0-9_]", "_", body.intent.strip().lower())[:40] or "other"
    upd = {
        "human_tags": {
            "intent": intent,
            "custom_intent": intent not in INTENTS,
            "emotion": body.emotion if body.emotion in EMOTIONS else None,
            "intensity": max(1, min(3, body.intensity)) if body.intensity else None,
            "entities": [e.strip() for e in body.entities if e.strip()][:10],
            "note": (body.note or "").strip()[:500] or None,
            "reviewed_by": uid,
            "reviewed_at": now().isoformat(),
            "bulk": False,
        },
        "reviewed": True,
    }
    if body.text_en is not None:
        upd["text_en_human"] = body.text_en.strip()
    uref.update(upd)
    return {"ok": True}


@router.post("/sessions/{sid}/confirm-remaining")
def confirm_remaining(sid: str, uid: str = Depends(current_advisor)):
    """Accept AI tags as-is for every unreviewed client utterance (flagged bulk=True in export)."""
    ref, d = _owned(sid, uid)
    smap = d.get("speaker_map") or {}
    b, n, total = db().batch(), 0, 0
    for snap in ref.collection("utterances").where("reviewed", "==", False).stream():
        u = snap.to_dict()
        if smap.get(u["speaker_id"]) not in ("client", "colleague") or not u.get("ai_tags"):
            continue
        b.update(snap.reference, {"reviewed": True, "human_tags": {
            **u["ai_tags"], "custom_intent": False, "note": None, "reviewed_by": uid,
            "reviewed_at": now().isoformat(), "bulk": True}})
        n += 1
        total += 1
        if n == 400:
            b.commit()
            b, n = db().batch(), 0
    if n:
        b.commit()
    return {"confirmed": total}


@router.post("/sessions/{sid}/reprocess")
def reprocess(sid: str, bg: BackgroundTasks, stage: str = Query("all", pattern="^(all|tag)$"),
              uid: str = Depends(current_advisor)):
    ref, d = _owned(sid, uid)
    if d.get("status") in PROCESSING and d.get("status") != "uploaded":
        raise HTTPException(409, "Already processing")
    ref.update({"status": "uploaded", "error": None, "updated_at": now()})
    bg.add_task(process_session, sid, stage)
    return {"ok": True}


@router.delete("/sessions/{sid}")
def delete_session(sid: str, uid: str = Depends(current_advisor)):
    ref, d = _owned(sid, uid)
    _delete_collection(ref.collection("utterances"))
    for blob in bucket().list_blobs(prefix=os.path.dirname(d["audio_path"]) + "/"):
        blob.delete()
    ref.delete()
    return {"ok": True}


def _client_rows(uid: str, reviewed_only: bool):
    for s in db().collection(COL).where("advisor_id", "==", uid).stream():
        d = s.to_dict()
        if d.get("status") != "ready":
            continue
        smap = d.get("speaker_map") or {}
        utts = sorted((u.to_dict() for u in s.reference.collection("utterances").stream()),
                      key=lambda u: u["i"])
        last_advisor = None
        for u in utts:
            role = smap.get(u["speaker_id"], "other")
            if role == "advisor":
                last_advisor = u.get("text_en_human") or u.get("text_en") or u["text_codemix"]
                continue
            if role not in ("client", "colleague") or (reviewed_only and not u.get("reviewed")):
                continue
            tags = u.get("human_tags") or u.get("ai_tags") or {}
            yield {
                "session_id": s.id,
                "utterance": u["i"],
                "client_id": d.get("client_id"),
                "channel": d.get("channel"),
                "conversation_type": d.get("conversation_type", "client"),
                "speaker_role": role,
                "language": d.get("detected_language") or d.get("language_hint"),
                "text_codemix": u["text_codemix"],
                "text_en": u.get("text_en_human") or u.get("text_en"),
                "intent": tags.get("intent"),
                "expected_fluid_cards": FLUID_CARDS.get(tags.get("intent"), []),
                "emotion": tags.get("emotion"),
                "intensity": tags.get("intensity"),
                "entities": tags.get("entities") or [],
                "prev_advisor_line": last_advisor,
                "label_source": ("human_bulk" if (u.get("human_tags") or {}).get("bulk")
                                 else "human" if u.get("human_tags") else "ai"),
                "start": u.get("start"),
            }


@router.get("/stats")
def stats(uid: str = Depends(current_advisor)):
    sessions = [s.to_dict() for s in db().collection(COL).where("advisor_id", "==", uid).stream()]
    rows = list(_client_rows(uid, reviewed_only=False))
    by_intent: Dict[str, Dict[str, int]] = {}
    by_lang: Dict[str, int] = {}
    for r in rows:
        k = r["intent"] or "untagged"
        by_intent.setdefault(k, {"total": 0, "reviewed": 0})
        by_intent[k]["total"] += 1
        if r["label_source"] != "ai":
            by_intent[k]["reviewed"] += 1
        lang = r["language"] or "unknown"
        by_lang[lang] = by_lang.get(lang, 0) + 1
    return {
        "sessions": len(sessions),
        "ready_sessions": sum(1 for s in sessions if s.get("status") == "ready"),
        "hours": round(sum(s.get("duration_sec") or 0 for s in sessions) / 3600, 2),
        "client_utterances": len(rows),
        "reviewed": sum(1 for r in rows if r["label_source"] != "ai"),
        "by_intent": by_intent,
        "by_language": by_lang,
        "by_channel": {c: sum(1 for s in sessions if s.get("channel") == c) for c in CHANNELS},
    }


@router.get("/export")
def export(reviewed_only: bool = True, uid: str = Depends(current_advisor)):
    def gen():
        for r in _client_rows(uid, reviewed_only):
            yield json.dumps(r, ensure_ascii=False) + "\n"
    name = f"fluid_dataset_{now():%Y%m%d}.jsonl"
    return StreamingResponse(gen(), media_type="application/x-ndjson",
                             headers={"Content-Disposition": f'attachment; filename="{name}"'})
