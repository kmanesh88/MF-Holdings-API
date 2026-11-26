"""
Vantage · Backups — weekly full export of Firestore to Cloud Storage.

POST /backup/run   (X-Agent-Secret from GitHub Actions, or a signed-in advisor)
GET  /backup/runs  (signed-in advisor)

Writes  gs://<BACKUP_BUCKET>/backups/firestore/<YYYY-MM-DD_HHMM>.jsonl.gz
        one line per document: {"path": "col/doc/sub/doc", "data": {...}}
plus a manifest with per-collection counts. Keeps 12 weeks (BACKUP_KEEP_DAYS, default 84).
Also purges Capture recordings that have been in Trash for more than 30 days.

Restore with tools/restore_backup.py (dry-run by default).

Env:
    FIREBASE_SERVICE_ACCOUNT_JSON
    BACKUP_BUCKET        optional; defaults to the project's default bucket
    BACKUP_KEEP_DAYS     optional; default 84
"""
from __future__ import annotations

import base64
import datetime as dt
import gzip
import json
import logging
import os
import tempfile
import uuid

from fastapi import APIRouter, BackgroundTasks, Depends

from tax_router import _agent_or_advisor, _advisor

log = logging.getLogger("backup")
router = APIRouter(prefix="/backup", tags=["backup"])


def _fb():
    import firebase_admin
    from firebase_admin import credentials, firestore, storage
    sa = json.loads(os.environ["FIREBASE_SERVICE_ACCOUNT_JSON"])
    if not firebase_admin._apps:
        firebase_admin.initialize_app(credentials.Certificate(sa))
    name = os.environ.get("BACKUP_BUCKET") or os.environ.get("FIREBASE_STORAGE_BUCKET") or f"{sa['project_id']}.firebasestorage.app"
    return firestore.client(), storage.bucket(name)


def encode(v):
    """Firestore value -> JSON-safe value (reversible by tools/restore_backup.py)."""
    from google.cloud.firestore_v1 import DocumentReference, GeoPoint
    if isinstance(v, dt.datetime):
        return {"__dt__": v.isoformat()}
    if isinstance(v, DocumentReference):
        return {"__ref__": v.path}
    if isinstance(v, GeoPoint):
        return {"__geo__": [v.latitude, v.longitude]}
    if isinstance(v, (bytes, bytearray)):
        return {"__bytes__": base64.b64encode(bytes(v)).decode()}
    if isinstance(v, dict):
        return {k: encode(x) for k, x in v.items()}
    if isinstance(v, list):
        return [encode(x) for x in v]
    return v


def _walk(col, out, counts, root):
    # list_documents() also returns "missing" parent docs (e.g. users/{uid} with only subcollections),
    # which stream() skips — without this, everything under users/{uid}/... would be lost.
    for ref in col.list_documents():
        snap = ref.get()
        if snap.exists:
            out.write(json.dumps({"path": ref.path, "data": encode(snap.to_dict() or {})},
                                 ensure_ascii=False).encode() + b"\n")
            counts[root] = counts.get(root, 0) + 1
        for sub in ref.collections():
            _walk(sub, out, counts, root)


def run_backup(run_id: str):
    db, bucket = _fb()
    run_ref = db.collection("backup_runs").document(run_id)
    try:
        stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d_%H%M")
        counts = {}
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, "backup.jsonl.gz")
            with gzip.open(path, "wb") as out:
                for col in db.collections():
                    if col.id == "backup_runs":
                        continue
                    _walk(col, out, counts, col.id)
            size = os.path.getsize(path)
            key = f"backups/firestore/{stamp}.jsonl.gz"
            bucket.blob(key).upload_from_filename(path, content_type="application/gzip")
        bucket.blob(f"backups/firestore/{stamp}.manifest.json").upload_from_string(
            json.dumps({"run_id": run_id, "file": key, "bytes": size, "documents": counts,
                        "total": sum(counts.values())}, indent=1), content_type="application/json")

        # retention
        keep = int(os.environ.get("BACKUP_KEEP_DAYS", "84"))
        cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=keep)
        removed = 0
        for blob in bucket.list_blobs(prefix="backups/firestore/"):
            if blob.time_created and blob.time_created < cutoff:
                blob.delete()
                removed += 1

        # Capture trash older than 30 days
        purged = 0
        try:
            from capture_router import purge_trash
            purged = purge_trash()
        except Exception as e:  # noqa: BLE001
            log.warning("trash purge skipped: %s", e)

        run_ref.update({"status": "done", "finished_at": dt.datetime.now(dt.timezone.utc), "file": key,
                        "bytes": size, "documents": counts, "total": sum(counts.values()),
                        "old_backups_removed": removed, "trash_purged": purged})
    except Exception as e:  # noqa: BLE001
        log.exception("backup failed")
        run_ref.update({"status": "failed", "finished_at": dt.datetime.now(dt.timezone.utc), "error": str(e)[:500]})


@router.post("/run")
def backup_run(bg: BackgroundTasks, who: str = Depends(_agent_or_advisor)):
    db, _ = _fb()
    run_id = f"{dt.datetime.now(dt.timezone.utc):%Y%m%d%H%M%S}-{uuid.uuid4().hex[:6]}"
    db.collection("backup_runs").document(run_id).set({"status": "running", "triggered_by": who,
                                                       "started_at": dt.datetime.now(dt.timezone.utc)})
    bg.add_task(run_backup, run_id)
    return {"ok": True, "run_id": run_id}


@router.get("/runs")
def backup_runs(uid: str = Depends(_advisor)):
    db, _ = _fb()
    rows = []
    for d in db.collection("backup_runs").stream():
        x = d.to_dict()
        rows.append({"id": d.id, **{k: (v.isoformat() if isinstance(v, dt.datetime) else v) for k, v in x.items()}})
    rows.sort(key=lambda r: r.get("started_at") or "", reverse=True)
    return rows[:20]
