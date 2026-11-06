#!/usr/bin/env python3
"""Step 1 - collect.

Reads the Overture Maps release through DuckDB and writes one parquet per city,
already filtered by eligible category and with usable contact information.

Usage:
  python3 pipeline/collect.py --city "Miami-Dade (US)"
  python3 pipeline/collect.py --all
"""
import argparse
import os
import json
import re
import subprocess
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent
WORK = Path(os.environ.get("LEADS_WORK", "work"))
DATA = WORK / "data"
DUCKDB = os.environ.get("DUCKDB_BIN", "duckdb")


def _load_config():
    """Real config if present, otherwise the committed example."""
    here = Path(__file__).resolve().parent
    for name in ("config.yaml", "config.example.yaml"):
        f = here / name
        if f.exists():
            return yaml.safe_load(f.read_text())
    raise SystemExit("config.yaml not found - copy config.example.yaml")


CFG = _load_config()


def sql_escape(s: str) -> str:
    return s.replace("'", "''")


def cat_list() -> str:
    cats = [c for c, v in CFG["categories"].items() if v["fit"] > 0]
    return ",".join(f"'{c}'" for c in cats)


def build_query(city: dict) -> str:
    xmin, ymin, xmax, ymax = city["bbox"]
    release = CFG["overture"]["release"]
    src = CFG["overture"]["bucket"].format(release=release)
    out = DATA / (city["slug"] + ".parquet")
    return f"""
INSTALL httpfs; LOAD httpfs;
SET http_timeout=300000;
SET threads=4;
COPY (
  SELECT
    id                                                    AS id,
    names.primary                                         AS name,
    basic_category                                        AS cat_basic,
    taxonomy.primary                                      AS cat_primary,
    addresses[1].country                                  AS country,
    addresses[1].region                                   AS region,
    addresses[1].locality                                 AS locality,
    addresses[1].postcode                                 AS postcode,
    addresses[1].freeform                                 AS address,
    geometry.x                                            AS lon,
    geometry.y                                            AS lat,
    phones, websites, socials, emails,
    operating_status, confidence,
    '{sql_escape(city["name"])}'                          AS city_label,
    '{release}'                                           AS release
  FROM read_parquet('{src}')
  WHERE bbox.xmin > {xmin} AND bbox.xmax < {xmax}
    AND bbox.ymin > {ymin} AND bbox.ymax < {ymax}
    AND (basic_category IN ({cat_list()}) OR taxonomy.primary IN ({cat_list()}))
    AND (operating_status IS NULL OR operating_status <> 'permanently_closed')
    AND (confidence IS NULL OR confidence >= 0.5)
    AND (
      (phones IS NOT NULL AND len(phones) > 0)
      OR (emails IS NOT NULL AND len(emails) > 0)
    )
) TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD);
"""


def run(city: dict) -> dict:
    slug = re.sub(r"[^a-z0-9]+", "-", city["name"].lower()).strip("-")
    city = {**city, "slug": slug}
    DATA.mkdir(parents=True, exist_ok=True)
    script = WORK / f"_collect_{slug}.sql"
    script.write_text(build_query(city))
    out = subprocess.run([DUCKDB, "-c", f".read {script}"],
                         capture_output=True, text=True, timeout=1800)
    if out.returncode != 0:
        return {"city": city["name"], "ok": False, "error": out.stderr[-500:]}
    n = subprocess.run(
        [DUCKDB, "-json", "-c",
         f"SELECT count(*) AS n FROM '{DATA / (slug + '.parquet')}'"],
        capture_output=True, text=True)
    try:
        total = json.loads(n.stdout)[0]["n"]
    except Exception:
        total = None
    return {"city": city["name"], "ok": True, "rows": total,
            "file": str(DATA / (slug + ".parquet"))}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--city", help="exact city name from config.yaml")
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--country", help="filter cities by ISO2 code")
    args = ap.parse_args()

    cities = CFG["cities"]
    if args.city:
        cities = [c for c in cities if c["name"] == args.city]
        if not cities:
            sys.exit(f"city not found: {args.city}")
    elif args.country:
        cities = [c for c in cities if c["country"] == args.country]
    elif not args.all:
        sys.exit("use --city, --country or --all")

    results = []
    for c in cities:
        print(f"-> {c['name']} ...", flush=True)
        r = run(c)
        print(f"   {r}", flush=True)
        results.append(r)

    (WORK / "collect_report.json").write_text(
        json.dumps(results, indent=2, ensure_ascii=False))
    ok = [r for r in results if r["ok"]]
    total = sum(r.get("rows") or 0 for r in ok)
    print(f"\n{len(ok)}/{len(results)} cities · {total} businesses collected")


if __name__ == "__main__":
    main()
