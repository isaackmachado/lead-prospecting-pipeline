#!/usr/bin/env python3
"""Step 3 - score, dedupe and assemble the lead list.

Joins places + site_checks, applies the config.yaml rules, computes the
explainable score (0-100) and writes the final list to parquet + CSV.

Usage:
  python3 pipeline/score.py --checks work/site_checks/xxx.json
  python3 pipeline/score.py --latest
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
CHECKS = WORK / "site_checks"
OUT = WORK / "out"
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

try:
    import phonenumbers
except ImportError:
    phonenumbers = None


def e164(raw: str, country: str):
    """Normalise a phone number to E.164. Without the lib, return digits with '+'."""
    raw = (raw or "").strip()
    if not raw:
        return None
    if phonenumbers:
        try:
            num = phonenumbers.parse(raw, country)
            if phonenumbers.is_valid_number(num):
                return phonenumbers.format_number(num, phonenumbers.PhoneNumberFormat.E164)
        except Exception:
            pass
        # try again assuming the digits already carry the country code
        digits = re.sub(r"\D", "", raw)
        if len(digits) > 10:
            try:
                num = phonenumbers.parse("+" + digits, None)
                if phonenumbers.is_valid_number(num):
                    return phonenumbers.format_number(num, phonenumbers.PhoneNumberFormat.E164)
            except Exception:
                pass
    digits = re.sub(r"\D", "", raw)
    return ("+" + digits) if digits else None


def classify_channel(email, phone, country_cfg):
    """Which channel to use with this lead, respecting the country's legal block."""
    has_email = bool(email)
    has_phone = bool(phone)
    if has_email and country_cfg.get("email_allowed", True):
        return "email"
    if has_phone:
        return "whatsapp" if country_cfg.get("lang") in ("es", "pt") else "phone"
    if has_email:
        return "email_blocked"
    return None


def contact_points(email, phone, socials):
    """0-25. Verified email > plain email > probable mobile > landline > nothing."""
    pts = 0
    if email:
        pts = 18
    if phone:
        # a mobile-length number is worth more than a landline
        digits = re.sub(r"\D", "", phone)
        mobile = len(digits) >= 12
        pts = max(pts, 16 if mobile else 11)
    if email and phone:
        pts = min(25, pts + 6)
    if not email and not phone and socials:
        pts = 4
    return pts


def fit_points(cat_primary, cat_basic):
    for c in (cat_primary, cat_basic):
        if c and c in CFG["categories"]:
            return CFG["categories"][c]["fit"]
    return 6  # unknown category: low fit, but not zero


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--checks", help="site_checks file")
    ap.add_argument("--latest", action="store_true")
    ap.add_argument("--min-score", type=int, default=30)
    args = ap.parse_args()

    if args.latest:
        files = sorted(CHECKS.glob("*.json"), key=lambda p: p.stat().st_mtime)
        if not files:
            sys.exit("no site_checks found")
        checks_file = files[-1]
    elif args.checks:
        checks_file = Path(args.checks)
    else:
        sys.exit("use --checks <file> or --latest")

    checks = json.loads(checks_file.read_text())
    print(f"checks: {checks_file.name} ({len(checks)} records)")

    # matching places
    ids = ",".join(f"'{c['id']}'" for c in checks if c.get("id"))
    q = f"""
    SELECT id, name, cat_basic, cat_primary, country, region, locality, postcode,
           address, lon, lat, phones, emails, websites, socials, city_label, confidence
    FROM read_parquet('{DATA}/*.parquet')
    WHERE id IN ({ids});
    """
    out = subprocess.run([DUCKDB, "-json", "-c", q], capture_output=True, text=True)
    if out.returncode != 0:
        sys.exit(out.stderr[:800])
    places = {p["id"]: p for p in json.loads(out.stdout)}
    print(f"places: {len(places)} matched")

    gap_points = CFG["site_gap_points"]
    lead_states = set(CFG["lead_states"])
    rows = []
    seen_keys = set()

    for c in checks:
        p = places.get(c["id"])
        if not p:
            continue
        st = c["status"]
        if st not in lead_states:
            continue

        country = p.get("country") or ""
        ccfg = CFG["countries"].get(country, {})
        raw_phone = (p.get("phones") or [None])[0]
        phone = e164(raw_phone, country) if raw_phone else None
        email = ((p.get("emails") or [None])[0] or "").lower() or None

        # dedupe: normalised phone OR name+city
        key_phone = re.sub(r"\D", "", phone or "")
        key_name = (re.sub(r"[^a-z0-9]", "", (p.get("name") or "").lower())[:28]
                    + "|" + re.sub(r"[^a-z0-9]", "", (p.get("locality") or "").lower())[:12])
        if (key_phone and key_phone in seen_keys) or (not key_phone and key_name in seen_keys):
            continue
        seen_keys.add(key_phone or key_name)

        site_gap = gap_points.get(st, 0)
        contact = contact_points(email, phone, p.get("socials"))
        fit = fit_points(p.get("cat_primary"), p.get("cat_basic"))
        conf = p.get("confidence") or 0.6
        freshness = min(15, round(conf * 10 + 5))

        # normalised to 0-100: theoretical maximum = 45+25+20+10 = 100
        score = round(45 * (site_gap / 40) + 25 * (contact / 25) + 20 * (fit / 20)
                      + 10 * (freshness / 15), 1)

        if score < args.min_score:
            continue

        channel = classify_channel(email, phone, ccfg)

        rows.append({
            "id": p["id"],
            "name": p.get("name"),
            "category": p.get("cat_primary") or p.get("cat_basic") or "",
            "cat_raw": p.get("cat_primary") or p.get("cat_basic"),
            "country": country,
            "region": p.get("region"),
            "locality": p.get("locality"),
            "postcode": p.get("postcode"),
            "address": p.get("address"),
            "city_label": p.get("city_label"),
            "site_status": st,
            "site_detail": c.get("detail"),
            "site_url": c.get("url"),
            "site_final_url": c.get("final_url"),
            "http_code": c.get("http_code"),
            "checked_at": c.get("checked_at"),
            "phone": phone,
            "email": email,
            "social": (p.get("socials") or [None])[0],
            "whatsapp_url": f"https://wa.me/{re.sub(r'[^0-9]', '', phone)}" if phone else None,
            "channel": channel,
            "score": score,
            "tier": "A",  # recomputed by percentile after sorting
            "score_site": round(45 * (site_gap / 40), 1),
            "score_contact": round(25 * (contact / 25), 1),
            "score_fit": round(20 * (fit / 20), 1),
            "score_fresh": round(10 * (freshness / 15), 1),
            "lang": ccfg.get("lang", "en"),
            "status": "new",
            "lat": p.get("lat"),
            "lon": p.get("lon"),
        })

    rows.sort(key=lambda r: (-r["score"], r["name"] or ""))

    # --- tiers by percentile: the queue recalibrates itself every run.
    # Score is relative by nature (everyone is "no site + contact"), so a fixed
    # cut saturates. A = top 15%, B = 15-40%, C = 40-70%, D = the rest.
    if rows:
        scores = sorted((r["score"] for r in rows), reverse=True)
        n = len(scores)

        def pct(p: float) -> float:
            i = min(n - 1, max(0, int(round(p * (n - 1)))))
            return scores[i]

        cut_a, cut_b, cut_c = pct(0.15), pct(0.40), pct(0.70)
        for r in rows:
            r["tier"] = ("A" if r["score"] >= cut_a else
                         "B" if r["score"] >= cut_b else
                         "C" if r["score"] >= cut_c else "D")
        print(f"\n  percentile cuts: A>={cut_a}  B>={cut_b}  C>={cut_c}")

    OUT.mkdir(parents=True, exist_ok=True)
    stamp = checks_file.stem.split("_")[-1]
    parquet = OUT / f"leads_{stamp}.parquet"
    json.dump(rows, open(OUT / f"leads_{stamp}.json", "w"), ensure_ascii=False, indent=1)
    subprocess.run([DUCKDB, "-c",
                    f"COPY (SELECT * FROM read_json_auto('{OUT / f'leads_{stamp}.json'}')) "
                    f"TO '{parquet}' (FORMAT PARQUET, COMPRESSION ZSTD)"],
                   capture_output=True, text=True)

    from collections import Counter
    print(f"\n{len(rows)} qualified leads (score >= {args.min_score})")
    print("  by tier:  ", dict(Counter(r["tier"] for r in rows)))
    print("  by state: ", dict(Counter(r["site_status"] for r in rows)))
    print("  by channel:", dict(Counter(r["channel"] for r in rows)))
    print("  by country:", dict(Counter(r["country"] for r in rows)))
    print(f"\nparquet: {parquet}")

    tk = OUT / f"leads_{stamp}_tierA.csv"
    subprocess.run([DUCKDB, "-c",
                    f"COPY (SELECT name, category, locality, region, country, site_status, "
                    f"site_detail, phone, email, whatsapp_url, score, channel, site_url "
                    f"FROM read_parquet('{parquet}') WHERE tier = 'A' ORDER BY score DESC) "
                    f"TO '{tk}' (HEADER, DELIMITER ',')"],
                   capture_output=True, text=True)
    print(f"tier A csv: {tk}")


if __name__ == "__main__":
    main()
