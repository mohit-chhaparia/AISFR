## Private Repo Upgrade Pack (Non-Breaking)

This folder contains drop-in code for your **private collection repo** to generate the new datasets needed for dashboard pages 2, 3, and 4 while preserving your existing pipeline and page-1 behavior.

## What is included

- `analytics_common.py`  
  Shared normalization/parsing helpers (amounts, rounds, sectors, IDs, date parsing).

- `nation_reference.json`  
  Country reference metadata (ISO codes, region/subregion, default currency, map coordinates, aliases).

- `build_enriched_dataset.py`  
  Reads existing `data/{Nation}.json` files and creates:
  - `analytics/deals_enriched.json`

- `build_analytics_marts.py`  
  Reads `analytics/deals_enriched.json` and creates:
  - `analytics/page2_global.json`
  - `analytics/page3_country.json`
  - `analytics/page4_investor_startup.json`

- `automation_v2_helpers.py`  
  Helper functions/snippets for safe `automation.py` upgrades. Keeps legacy keys intact.

## Why this is non-breaking

1. Existing `data/{Nation}.json` files are not modified structurally.
2. Existing legacy keys remain unchanged for page-1 dashboard compatibility.
3. New artifacts are written under a separate `analytics/` folder.
4. You can run the new steps after your current collection + dedupe completes.

---

## Integration steps in your private repo

### 1) Copy files

Copy these files into your private repo (recommended layout):

```text
private_repo_root/
  scripts/
    analytics_common.py
    build_enriched_dataset.py
    build_analytics_marts.py
    automation_v2_helpers.py
  config/
    nation_reference.json
```

You can keep your own structure; just update import paths accordingly.

### 2) Add analytics build commands (post-collection)

After your existing `automation.py` jobs finish writing/committing nation files:

```bash
python3 scripts/build_enriched_dataset.py \
  --data-dir data \
  --output-dir analytics \
  --nation-reference config/nation_reference.json \
  --fx-rates data/fx_rates.json

python3 scripts/build_analytics_marts.py \
  --enriched analytics/deals_enriched.json \
  --output-dir analytics \
  --lookback-days 365
```

### 3) Commit new analytics artifacts

Ensure your workflow includes these files in commit steps:

- `analytics/deals_enriched.json`
- `analytics/page2_global.json`
- `analytics/page3_country.json`
- `analytics/page4_investor_startup.json`

---

## Recommended `automation.py` safety changes

Use `automation_v2_helpers.py` to:
- request richer fields from Gemini (source URL, announcement date, sectors)
- normalize output rows
- **always emit legacy keys**:
  - `Country`
  - `Startup_Name`
  - `Description`
  - `Amount`
  - `Round`
  - `Investors`
  - `Founders`
  - `LinkedIn_Profile`
  - `Hiring`
  - `Careers_Link`

The enriched builder handles optional new fields when present and falls back safely when absent.

---

## Validation commands

Run these checks locally/CI:

```bash
python3 -m py_compile scripts/analytics_common.py \
  scripts/build_enriched_dataset.py \
  scripts/build_analytics_marts.py \
  scripts/automation_v2_helpers.py

python3 scripts/build_enriched_dataset.py --data-dir data --output-dir analytics \
  --nation-reference config/nation_reference.json

python3 scripts/build_analytics_marts.py --enriched analytics/deals_enriched.json --output-dir analytics
```

---

## Public dashboard deployment note

Your current public deploy logic downloads `json/txt` from a configured private `data` folder.

To avoid accidental manifest parsing of analytics marts as nation files:
- keep page marts in `analytics/` (not in `data/`)
- update the public deploy workflow to explicitly download analytics artifacts from `analytics/` into public `data/` (or `analytics/`) as needed for page 2/3/4 rendering.

