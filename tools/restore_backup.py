"""
Restore a Vantage Firestore backup.

Usage (from a machine with the service-account JSON):
    export FIREBASE_SERVICE_ACCOUNT_JSON="$(cat service-account.json)"
    python tools/restore_backup.py backups/firestore/2026-12-01_0300.jsonl.gz            # dry run: counts only
    python tools/restore_backup.py <file> --only clients,client_holdings --apply          # restore some collections
    python tools/restore_backup.py <file> --apply                                         # restore everything

<file> can be a local path or a path inside the backup bucket (downloaded automatically).
Restoring overwrites documents with the same path; it never deletes documents that exist now.
"""
import argparse
import base64
import datetime as dt
import gzip
import json
import os
import sys
import tempfile


def decode(v, db):
    from google.cloud.firestore_v1 import GeoPoint
    if isinstance(v, dict):
        if set(v) == {"__dt__"}:
            return dt.datetime.fromisoformat(v["__dt__"])
        if set(v) == {"__ref__"}:
            return db.document(v["__ref__"])
        if set(v) == {"__geo__"}:
            return GeoPoint(*v["__geo__"])
        if set(v) == {"__bytes__"}:
            return base64.b64decode(v["__bytes__"])
        return {k: decode(x, db) for k, x in v.items()}
    if isinstance(v, list):
        return [decode(x, db) for x in v]
    return v


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("file")
    ap.add_argument("--only", default="", help="comma-separated top-level collections")
    ap.add_argument("--apply", action="store_true", help="actually write (default is dry run)")
    a = ap.parse_args()

    import firebase_admin
    from firebase_admin import credentials, firestore, storage
    sa = json.loads(os.environ["FIREBASE_SERVICE_ACCOUNT_JSON"])
    firebase_admin.initialize_app(credentials.Certificate(sa))
    db = firestore.client()

    path = a.file
    if not os.path.exists(path):
        bucket = storage.bucket(os.environ.get("BACKUP_BUCKET") or f"{sa['project_id']}.firebasestorage.app")
        tmp = os.path.join(tempfile.mkdtemp(), "b.jsonl.gz")
        bucket.blob(path).download_to_filename(tmp)
        path = tmp

    only = {x.strip() for x in a.only.split(",") if x.strip()}
    counts, batch, n = {}, db.batch(), 0
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for line in f:
            rec = json.loads(line)
            root = rec["path"].split("/")[0]
            if only and root not in only:
                continue
            counts[root] = counts.get(root, 0) + 1
            if a.apply:
                batch.set(db.document(rec["path"]), decode(rec["data"], db))
                n += 1
                if n == 400:
                    batch.commit(); batch, n = db.batch(), 0
    if a.apply and n:
        batch.commit()
    print(("RESTORED" if a.apply else "DRY RUN — would restore"), json.dumps(counts, indent=1))


if __name__ == "__main__":
    sys.exit(main())
