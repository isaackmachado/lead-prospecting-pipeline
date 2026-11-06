# Lead Prospecting Pipeline

Finds small businesses whose website is **missing, dead or broken**, scores them by how much they need a site, and loads the result into a CRM through an ingest API.

Built to answer one question at scale: *of these 300,000 businesses, which ones are losing customers right now because clicking their website leads nowhere?*

---

## The problem with trusting the data

Map datasets give you a `websites` field. It lies constantly:

- It often points at **Instagram, Facebook or a `linktr.ee` page**, not a real site.
- It points at **dead domains** — the business renamed and the domain expired.
- It points at **parking pages** selling the domain.
- It points at **free builders** that shut down (Google killed `business.site`).
- It returns **403 because of a WAF** — the site is fine, the bot is blocked.

Treating any of those as "no website" produces false positives, and a false positive burns the conversation: you open with *"your site is down"* and the owner replies *"it works fine here"*. So the classifier **opens every URL**, follows redirects, reads the HTML and assigns one of 13 states — and when it sees a WAF it returns `INDETERMINADO` instead of guessing.

---

## Architecture

```
collect.py  ->  probe.py  ->  score.py  ->  publish.py
   |             |             |             |
Overture       opens each    dedupes,      upserts into
Maps via       URL, reads    scores 0-100, the CRM API
DuckDB         HTML, 13      assigns       in batches
(parquet)      states        tiers         of 200
```

**Step 1 — collect** (`pipeline/collect.py`)
Reads the [Overture Maps](https://overturemaps.org) `places` theme straight from the public S3 bucket with DuckDB — no API key, no per-request cost. Filters by bounding box, by category fit, and keeps only rows that have a phone or an email. One parquet per city.

**Step 2 — probe** (`pipeline/probe.py`)
Async (`aiohttp`, 24 concurrent). Normalises the URL, checks the registrable domain against lists of social networks, aggregators, builders and shorteners — **before** making any request. Then fetches, follows redirects, and inspects the result: title, body length, viewport tag, parked-domain markers, placeholder markers, WAF signatures. Picks the *worst* verdict across all URLs a business has, because the worst site is the best sales opening.

**Step 3 — score** (`pipeline/score.py`)
Joins places with checks, dedupes on normalised phone (E.164 via `phonenumbers`) or name+city, and computes an explainable score:

| Component | Weight | What it measures |
|---|---|---|
| Site gap | 45 | How broken the site is (`NO_WEBSITE` 40 to `NOT_MOBILE` 14) |
| Contactability | 25 | Email verified > email > mobile > landline > social only |
| Category fit | 20 | How likely that business type buys a website |
| Freshness | 10 | Overture confidence |

Tiers are assigned **by percentile**, not by a fixed cut. A fixed cut saturates: in the first run 91% of leads landed in tier A because everyone plausibly has "no site + a phone". Percentiles re-calibrate every run — A is the top 15%, B the next 25%, C the next 30%, D the rest.

**Step 4 — publish** (`pipeline/publish.py`)
Upserts into a CRM backend over HTTP, 200 records per request, authenticated with a token read from the environment. Idempotent by `ext_id`, so a re-run updates instead of duplicating.

---

## Running it on your own market

Only `config.example.yaml` ships here — copy it to `config.yaml` and edit. The code reads `config.yaml` when present and falls back to the example otherwise.

```bash
cp pipeline/config.example.yaml pipeline/config.yaml
```

**Countries.** `email_allowed: false` is not a warning, it is a block. Cold B2B email has no valid legal basis in Germany, Spain, Italy and several others, so the pipeline refuses to emit a lead on that channel there rather than letting you find out from a lawyer.

**Categories.** `fit` is 0-20: how likely that business type is to buy a website. A category with `fit: 0` never becomes a lead — this is how you keep a 300k-row dataset from producing 300k "leads".

**Cities.** `bbox = [xmin, ymin, xmax, ymax]`. Start with one; each city is a probe run.

```bash
python3 pipeline/collect.py --city "Miami-Dade (US)"
python3 pipeline/probe.py   --city miami-dade-us --limit 500
python3 pipeline/score.py   --latest --min-score 30
CRM_BACKEND_URL=... CRM_INGEST_TOKEN=... \
  python3 pipeline/publish.py --file work/out/leads_XXX.parquet --tier A
```

DuckDB must be on `PATH` (or set `DUCKDB_BIN`).

---

## The 13 states

| State | Meaning | Scores as |
|---|---|---|
| `NO_WEBSITE` | No URL in the dataset at all | 40 |
| `DOMAIN_DEAD` | DNS fails / connection refused | 38 |
| `HTTP_ERROR` | 4xx or 5xx | 38 |
| `PARKED` | Domain for sale, parking page | 36 |
| `TIMEOUT` | No answer within the timeout | 36 |
| `TLS_INVALID` | Certificate rejected | 34 |
| `PLACEHOLDER` | Default nginx page, "coming soon" | 34 |
| `SOCIAL_ONLY` | The "site" is a social profile | 32 |
| `AGGREGATOR_ONLY` | The "site" is a delivery listing | 30 |
| `BUILDER_FREE` | Free builder site, no custom domain | 22 |
| `NOT_MOBILE` | Loads, but no viewport / huge page | 14 |
| `INDETERMINADO` | WAF or 403 — **never** scored as dead | 0 (review queue) |
| `OK` | Loads, has a viewport, looks alive | 0 (not a lead) |

`INDETERMINADO` is the important one. A bot that calls a Cloudflare-protected site "dead" is wrong, and being wrong once costs the whole conversation.

---

## Results

Run against Overture Maps across Brazil, the US, Mexico, Colombia, Argentina, Chile and Canada:

| Metric | Value |
|---|---|
| Businesses collected | **293,589** |
| Businesses whose site was opened and checked | **31,442** |
| Leads scored and loaded into the CRM | **17,857** |
| Leads in the top tier | 2,840 |
| Categories with a fit weight | 66 |
| Cities configured | 46 |

Site state across every business checked (deduplicated by business id):

| State | Count | Share |
|---|---|---|
| `NO_WEBSITE` | 13,121 | 41.7% |
| `OK` | 5,955 | 18.9% |
| `DOMAIN_DEAD` | 3,736 | 11.9% |
| `SOCIAL_ONLY` | 3,054 | 9.7% |
| `INDETERMINADO` | 2,119 | 6.7% |
| `HTTP_ERROR` | 1,035 | 3.3% |
| `NOT_MOBILE` | 953 | 3.0% |
| `TIMEOUT` | 682 | 2.2% |
| `PLACEHOLDER` | 513 | 1.6% |
| `BUILDER_FREE` | 167 | 0.5% |
| `AGGREGATOR_ONLY` | 44 | 0.1% |
| `PARKED` | 33 | 0.1% |
| `TLS_INVALID` | 30 | 0.1% |

**74% of them had no website or a broken one.** That is the thesis of the project: the
market is not short of plumbers, it is short of plumbers with a working website.

The `INDETERMINADO` row is why the classifier does not simply say "dead": 6.7% of the base
sat behind a WAF and had to be left unjudged rather than scored as dead. Guessing there
would have manufactured 2,119 false positives.

---

## Bugs that took real time

**The sampler was re-examining the same rows.** `USING SAMPLE (reservoir, 42)` with a fixed seed picks the same subset on every run, so the pipeline looked like it was working while never touching a new business. Fixed by deriving the already-checked ids from the previous output files and doing an anti-join — the state is derived, nothing to keep in sync.

**The id list overflowed the command line.** 2,600+ ids as a DuckDB argument raise `OSError: Argument list too long`. Writing them to a TSV and joining against it fixed it.

**`business.site` is not a free builder any more.** Google shut it down, so a 404 there is a *dead site*, not a live free one. Special-cased.

**A shortener is not a website.** `bit.ly` and friends redirect to whatever the owner wants; classifying them as `OK` hides a business that has nothing.

---

## Stack

Python 3 · DuckDB (Overture parquet over S3, no key) · aiohttp (async HTTP) · PyYAML · phonenumbers (E.164) · a REST ingest API with token auth.

## License

MIT
