#!/usr/bin/env python3
"""Step 2 - site classifier.

Does not trust the `websites` field: it opens the URL for real, follows the
redirect, reads the HTML and classifies into one of 12 states. A site that is
"alive but ugly" is NOT classified as dead - a false positive costs the whole
conversation.

Usage:
  python3 pipeline/probe.py --city miami-dade-us --limit 200
  python3 pipeline/probe.py --all --limit 5000
"""
import argparse
import os
import asyncio
import json
import re
import sys
import time
import subprocess
from pathlib import Path
from urllib.parse import urlparse

import aiohttp
import yaml

ROOT = Path(__file__).resolve().parent
WORK = Path(os.environ.get("LEADS_WORK", "work"))
DATA = WORK / "data"
CHECKS = WORK / "site_checks"


def _load_config():
    """Real config if present, otherwise the committed example."""
    here = Path(__file__).resolve().parent
    for name in ("config.yaml", "config.example.yaml"):
        f = here / name
        if f.exists():
            return yaml.safe_load(f.read_text())
    raise SystemExit("config.yaml not found - copy config.example.yaml")


CFG = _load_config()
DUCKDB = os.environ.get("DUCKDB_BIN", "duckdb")

UA = CFG["probe"]["user_agent"]
TIMEOUT = CFG["probe"]["timeout_s"]
ATTEMPTS = CFG["probe"]["attempts"]
CONC = CFG["probe"]["concurrency"]
MAX_KB = CFG["probe"]["max_page_kb"]

SOCIAL = set(CFG["social_domains"])
AGGREG = set(CFG["aggregator_domains"])
BUILDER = set(CFG["builder_domains"])
PARKED = [m.lower() for m in CFG["parked_markers"]]
PLACEHOLDER = [m.lower() for m in CFG["placeholder_markers"]]
WAF = [m.lower() for m in CFG["waf_markers"]]
SHORTENERS = set(CFG["shortener_domains"])

COMPOUND = {"com.br", "com.mx", "com.ar", "com.co", "com.cl", "co.uk", "com.au",
            "co.nz", "com.pe", "com.ec", "com.ve", "com.uy", "com.py", "com.bo",
            "com.gt", "com.do", "co.cr", "com.pa"}


def registrable(host: str) -> str:
    host = host.lower().strip(".")
    parts = host.split(".")
    if len(parts) <= 2:
        return host
    if ".".join(parts[-2:]) in COMPOUND:
        return ".".join(parts[-3:])
    return ".".join(parts[-2:])


def domain_kind(url: str):
    """Classify the domain without opening anything: social / aggregator / builder / shortener / own."""
    try:
        host = urlparse(url if "//" in url else "http://" + url).hostname or ""
    except Exception:
        return "INVALID", ""
    host = host.lower().removeprefix("www.")
    if not host:
        return "INVALID", ""
    reg = registrable(host)
    for d in SOCIAL:
        if host == d or host.endswith("." + d) or reg == d:
            return "SOCIAL", reg
    for d in AGGREG:
        if host == d or host.endswith("." + d) or reg == d:
            return "AGGREGATOR", reg
    for d in BUILDER:
        if host == d or host.endswith("." + d) or reg == d:
            return "BUILDER", reg
    if reg in SHORTENERS:
        return "SHORTENER", reg
    if re.match(r"^\d+\.\d+\.\d+\.\d+$", host):
        return "INVALID", host
    return "OWN", reg


def normalise(url: str) -> str:
    url = (url or "").strip()
    if not url:
        return ""
    if not re.match(r"^https?://", url, re.I):
        url = "http://" + url
    return url


async def fetch(session, url):
    """GET with N attempts. Returns (status, html, final_url, elapsed_ms, err)."""
    last_err = None
    for attempt in range(ATTEMPTS):
        t0 = time.perf_counter()
        try:
            async with session.get(url, allow_redirects=True,
                                   timeout=aiohttp.ClientTimeout(total=TIMEOUT),
                                   ssl=False) as r:
                raw = await r.content.read(MAX_KB * 1024)
                ms = int((time.perf_counter() - t0) * 1000)
                charset = r.charset or "utf-8"
                try:
                    html = raw.decode(charset, errors="replace")
                except LookupError:
                    html = raw.decode("utf-8", errors="replace")
                return r.status, html, str(r.url), ms, None
        except asyncio.TimeoutError:
            last_err = "timeout"
        except aiohttp.ClientConnectorCertificateError as e:
            last_err = f"tls:{e}"
        except aiohttp.ClientSSLError as e:
            last_err = f"tls:{e}"
        except aiohttp.ClientConnectorError as e:
            last_err = f"conn:{e}"
        except aiohttp.ClientError as e:
            last_err = f"client:{e}"
        except Exception as e:  # noqa: BLE001
            last_err = f"other:{type(e).__name__}"
        await asyncio.sleep(0.5 * (attempt + 1))
    return None, None, None, 0, last_err


def analyse(html: str):
    low = (html or "").lower()
    title_m = re.search(r"<title[^>]*>(.*?)</title>", low, re.S)
    title = re.sub(r"\s+", " ", title_m.group(1)).strip()[:200] if title_m else ""
    body_len = len(re.sub(r"<[^>]+>", "", low))
    has_viewport = bool(re.search(r"<meta[^>]+name=[\"']viewport", low))
    text = title + " " + low[:40000]
    return {
        "title": title,
        "body_len": body_len,
        "has_viewport": has_viewport,
        "page_kb": round(len(html or "") / 1024, 1),
        "parked": next((m for m in PARKED if m in text), None),
        "placeholder": next((m for m in PLACEHOLDER if m in text), None),
        "waf": next((m for m in WAF if m in text), None),
        "mailto": list(dict.fromkeys(re.findall(r"mailto:([\w.+-]+@[\w.-]+\.\w+)", low)))[:5],
        "emails_in_page": list(dict.fromkeys(re.findall(
            r"\b[\w.+-]+@[\w-]+\.[\w.-]{2,}\b", low)))[:10],
        "phone_links": list(dict.fromkeys(re.findall(r"tel:([+\d\s().-]{7,})", low)))[:5],
    }


# severity order: the earlier, the worse the site and the better the lead
SEVERITY = ["DOMAIN_DEAD", "HTTP_ERROR", "TIMEOUT", "TLS_INVALID", "PARKED",
            "PLACEHOLDER", "SOCIAL_ONLY", "AGGREGATOR_ONLY", "BUILDER_FREE",
            "NOT_MOBILE", "INDETERMINADO", "OK"]


def worst(a: str, b: str) -> str:
    ai, bi = SEVERITY.index(a), SEVERITY.index(b)
    return a if ai <= bi else b


async def probe_url(session, url: str) -> dict:
    """Check one URL and return {status, detail, http_code, final_url, evidence}."""
    kind, host = domain_kind(url)

    if kind == "SOCIAL":
        return {"status": "SOCIAL_ONLY", "detail": host, "evidence": {"social": url}}
    if kind == "AGGREGATOR":
        return {"status": "AGGREGATOR_ONLY", "detail": host, "evidence": {"aggregator": url}}
    if kind == "INVALID":
        return {"status": "DOMAIN_DEAD", "detail": "invalid URL", "evidence": {}}

    status, html, final_url, ms, err = await fetch(session, url)
    if err:
        if err.startswith("tls:"):
            st = "TLS_INVALID"
        elif err == "timeout":
            st = "TIMEOUT"
        else:
            st = "DOMAIN_DEAD"
        return {"status": st, "detail": err[:160], "http_code": status,
                "final_url": final_url, "evidence": {"err": err[:200]}}

    sig = analyse(html)

    # WAF/anti-bot takes precedence: never call dead what is actually protected
    if sig["waf"] or status in (403, 429):
        return {"status": "INDETERMINADO", "detail": f"WAF ({sig['waf'] or status})",
                "http_code": status, "final_url": final_url, "evidence": sig,
                "response_ms": ms}

    if status and status >= 400:
        return {"status": "HTTP_ERROR", "detail": f"HTTP {status}", "http_code": status,
                "final_url": final_url, "evidence": sig, "response_ms": ms}

    # free builder: only counts as BUILDER_FREE if the page is alive;
    # Google shut down business.site, so a 404 there is a dead site.
    if kind == "BUILDER":
        return {"status": "BUILDER_FREE", "detail": host, "http_code": status,
                "final_url": final_url, "evidence": sig, "response_ms": ms}

    if sig["parked"]:
        return {"status": "PARKED", "detail": sig["parked"], "http_code": status,
                "final_url": final_url, "evidence": sig, "response_ms": ms}

    if sig["placeholder"]:
        return {"status": "PLACEHOLDER", "detail": sig["placeholder"], "http_code": status,
                "final_url": final_url, "evidence": sig, "response_ms": ms}

    if sig["body_len"] < 220 and not sig["title"]:
        return {"status": "PLACEHOLDER", "detail": f"empty page ({sig['body_len']} chars)",
                "http_code": status, "final_url": final_url, "evidence": sig, "response_ms": ms}

    # the final redirect can reveal a builder or a social network
    if final_url:
        k2, h2 = domain_kind(final_url)
        if k2 == "BUILDER":
            return {"status": "BUILDER_FREE", "detail": h2, "http_code": status,
                    "final_url": final_url, "evidence": sig, "response_ms": ms}
        if k2 == "SOCIAL":
            return {"status": "SOCIAL_ONLY", "detail": h2, "http_code": status,
                    "final_url": final_url, "evidence": sig, "response_ms": ms}

    if not sig["has_viewport"] or sig["page_kb"] > MAX_KB:
        det = "no viewport" if not sig["has_viewport"] else f"{sig['page_kb']} KB"
        return {"status": "NOT_MOBILE", "detail": det, "http_code": status,
                "final_url": final_url, "evidence": sig, "response_ms": ms}

    return {"status": "OK", "detail": sig["title"][:120] or "ok", "http_code": status,
            "final_url": final_url, "evidence": sig, "response_ms": ms}


async def classify_place(session, place: dict, sem):
    async with sem:
        websites = [w for w in (place.get("websites") or []) if (w or "").strip()]
        socials = place.get("socials") or []
        rec = {"id": place["id"], "name": place.get("name"),
               "city": place.get("city_label"), "country": place.get("country"),
               "checked_at": int(time.time()),
               "url": websites[0] if websites else None,
               "final_url": None, "http_code": None, "status": None,
               "detail": None, "evidence": {}}

        if not websites:
            rec["status"] = "NO_WEBSITE"
            rec["detail"] = "has social profile" if socials else "no site, no social"
            rec["evidence"] = {"socials": socials[:2]}
            return rec

        # check every URL; the verdict is the WORST one (the best sales opening)
        best = None
        for raw in websites[:3]:
            res = await probe_url(session, normalise(raw))
            res["_url"] = normalise(raw)
            if best is None or SEVERITY.index(res["status"]) < SEVERITY.index(best["status"]):
                best = res
            if res["status"] == "OK":
                break

        rec.update({k: v for k, v in best.items() if not k.startswith("_")})
        return rec


async def run(places: list, out_file: Path):
    sem = asyncio.Semaphore(CONC)
    connector = aiohttp.TCPConnector(limit=CONC * 2, force_close=False,
                                     enable_cleanup_closed=True)
    headers = {"User-Agent": UA,
               "Accept": "text/html,application/xhtml+xml",
               "Accept-Language": "en-US,en;q=0.9,es;q=0.8,pt;q=0.7"}
    results = []
    t0 = time.time()
    async with aiohttp.ClientSession(connector=connector, headers=headers) as session:
        tasks = [classify_place(session, p, sem) for p in places]
        for i, coro in enumerate(asyncio.as_completed(tasks), 1):
            results.append(await coro)
            if i % 100 == 0:
                el = time.time() - t0
                print(f"  {i}/{len(places)} · {el:.0f}s · {i/el:.1f}/s", flush=True)
    out_file.parent.mkdir(parents=True, exist_ok=True)
    out_file.write_text(json.dumps(results, ensure_ascii=False))
    return results


def checked_ids() -> set:
    """Ids already checked in previous runs (every file in site_checks).

    Without this, `USING SAMPLE (reservoir, 42)` re-examines the SAME handful of
    businesses on every run (fixed seed) and no new lead ever enters. The state
    is derived from the files themselves - nothing to keep in sync.
    """
    ids = set()
    for f in CHECKS.glob("*.json"):
        try:
            for rec in json.loads(f.read_text()):
                if rec.get("id"):
                    ids.add(rec["id"])
        except Exception:  # noqa: BLE001
            continue
    return ids


def load_places(files: list, limit: int, skip: set | None = None):
    src = "[" + ",".join(f"'{f}'" for f in files) + "]"
    # The list of checked ids cannot go on the command line (2,600+ ids blow
    # past the argument limit): write it to a TSV and anti-join against it.
    join_sql = ""
    skip_file = WORK / "out" / "_checked_ids.tsv"
    if skip:
        skip_file.parent.mkdir(parents=True, exist_ok=True)
        skip_file.write_text("\n".join(skip))
        join_sql = (f"ANTI JOIN read_csv('{skip_file}', header=false, "
                    f"columns={{'id':'VARCHAR'}}) AS v ON p.id = v.id")
    q = f"""
    SELECT p.id, p.name, p.city_label, p.country, p.websites, p.socials, p.emails, p.phones
    FROM read_parquet({src}) AS p
    {join_sql}
    WHERE p.name IS NOT NULL AND length(trim(p.name)) > 1
    ORDER BY hash(p.id || 'run')
    LIMIT {limit};
    """
    out = subprocess.run([DUCKDB, "-json", "-c", q], capture_output=True, text=True)
    if out.returncode != 0:
        sys.exit(out.stderr[:600])
    return json.loads(out.stdout)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--city", help="city slug (e.g. miami-dade-us)")
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--limit", type=int, default=200)
    args = ap.parse_args()

    if args.city:
        files = [DATA / f"{args.city}.parquet"]
    elif args.all:
        files = sorted(DATA.glob("*.parquet"))
    else:
        sys.exit("use --city or --all")
    files = [f for f in files if f.exists()]
    if not files:
        sys.exit("no parquet found - run collect.py first")

    places = load_places([str(f) for f in files], args.limit, checked_ids())
    if not places:
        print("nothing new: the whole base has already been checked")
        return
    print(f"checking {len(places)} new businesses ...", flush=True)
    out = CHECKS / (f"{args.city or 'all'}_{int(time.time())}.json")
    results = asyncio.run(run(places, out))

    from collections import Counter
    c = Counter(r["status"] for r in results)
    print("\n=== classification ===")
    for k, v in sorted(c.items(), key=lambda x: -x[1]):
        print(f"  {k:<16} {v:>6}  ({v/len(results)*100:.1f}%)")
    print(f"\nsaved to {out}")


if __name__ == "__main__":
    main()
