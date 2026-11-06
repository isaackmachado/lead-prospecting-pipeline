#!/usr/bin/env python3
"""Step 4 - push scored leads into a CRM through an ingest API.

Configuration comes from the environment only:
  CRM_BACKEND_URL   e.g. https://your-backend.example.com
  CRM_INGEST_TOKEN  token the backend expects in the X-Ingest-Token header

Idempotent by design: the endpoint upserts on ext_id, so a re-run updates
records instead of duplicating them.

Usage:
  python3 pipeline/publish.py --file work/out/leads_1701794400.parquet --tier A
"""
import argparse
import json
import os
import subprocess
import sys
import urllib.request
from pathlib import Path

WORK = Path(os.environ.get("LEADS_WORK", "work"))
DUCKDB = os.environ.get("DUCKDB_BIN", "duckdb")
BACKEND = os.environ.get("CRM_BACKEND_URL")
TOKEN = os.environ.get("CRM_INGEST_TOKEN")
BATCH = 200


def require_env():
    missing = [k for k, v in (("CRM_BACKEND_URL", BACKEND),
                              ("CRM_INGEST_TOKEN", TOKEN)) if not v]
    if missing:
        sys.exit("missing environment variable(s): " + ", ".join(missing))


def fetch_rows(parquet: Path, tier):
    where = f"WHERE tier = '{tier}'" if tier else ""
    q = f"SELECT * FROM read_parquet('{parquet}') {where} ORDER BY score DESC;"
    out = subprocess.run([DUCKDB, "-json", "-c", q], capture_output=True, text=True)
    if out.returncode != 0:
        sys.exit(out.stderr[:800])
    return json.loads(out.stdout)


def post(leads):
    data = json.dumps({"leads": leads}).encode()
    req = urllib.request.Request(
        f"{BACKEND}/backend/v1/ingest/leads", data=data,
        headers={"Content-Type": "application/json", "X-Ingest-Token": TOKEN},
        method="POST")
    with urllib.request.urlopen(req, timeout=120) as r:
        return json.loads(r.read().decode())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", required=True, help="parquet of scored leads")
    ap.add_argument("--tier", help="only this tier (A/B/C)")
    args = ap.parse_args()
    require_env()

    rows = fetch_rows(Path(args.file), args.tier)
    print(f"{len(rows)} leads from {Path(args.file).name}")

    total = {"created": 0, "updated": 0, "skipped": 0}
    for i in range(0, len(rows), BATCH):
        chunk = rows[i:i + BATCH]
        clean = [{k: v for k, v in r.items() if v not in (None, "")} for r in chunk]
        try:
            res = post(clean)
        except Exception as e:
            sys.exit(f"batch {i // BATCH + 1} failed: {e}")
        for k in total:
            total[k] += res.get(k, 0)
        print(f"  batch {i // BATCH + 1}: +{res.get('created', 0)} new, "
              f"{res.get('updated', 0)} updated, {res.get('skipped', 0)} skipped", flush=True)

    print(f"\ntotal: {total}")


if __name__ == "__main__":
    main()
