# `eurostat-import.py`

Generic importer for Eurostat SDMX datasets into a Directus table. Currently
used for **renewable energy share in transport** (feeds the
`mobilityRenewableShare` chart in `core`).

## What it does

1. Downloads a full Eurostat dataflow as CSV via the SDMX 3.0 dissemination
   API:
   `https://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0/data/dataflow/ESTAT/<DATASET_NAME>/1.0/*.*.*.*?format=csvdata&formatVersion=2.0&lang=en&labels=name`
2. Filters to the relevant `nrg_bal` row (currently `REN_TRA`, i.e. share of
   renewables in transport).
3. Reshapes to long format: `region` (Eurostat `geo` code, e.g. `AT`, `DE`,
   `EU27_2020`), `period` (year), `value`, plus fixed `unit`/`category`/
   `source` columns.
4. Upserts into Directus table/collection set by `TABLE_NAME` (currently
   `mobility`), matching existing rows on `(region, period, category)` and
   only writing when the row is new or the value changed. Non-destructive
   and safe to re-run — it will not create duplicates or touch unrelated
   rows.

## Configuration

At the top of the script:

```python
DATASET_NAME = "nrg_ind_ren"   # Eurostat dataset code
TABLE_NAME = "mobility"        # Directus collection
CATEGORY_NAME = "share_renewable"
```

To import a different Eurostat indicator, point these at the new dataset/
collection/category and adjust the `nrg_bal` filter in `transform_data` if
the new dataset uses a different dimension to select the right series.

## Requirements

- `.env` in the repo root with `DIRECTUS_API_URL` and a `DIRECTUS_API_TOKEN`
  that has **write** access to the target collection.
- Python deps: `requests`, `pandas`, `python-dotenv`.

## Running it

```bash
cd manual
python eurostat-import.py
```

No arguments; it always re-fetches the full dataset and only writes the
diff. Safe to run repeatedly.

## Release cadence

Eurostat updates `nrg_ind_ren` irregularly (previous year's data typically
lands sometime in the following year, with revisions to earlier years
happening occasionally too). There's no fixed publish date to schedule a
cronjob against, so this stays a manual script for now — check the
dataset's `updated` timestamp via
`https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/nrg_ind_ren?geo=EU27_2020&nrg_bal=REN_TRA&format=JSON&lang=EN`
before re-running, or watch `DATA_UPDATE_NOTES.md` in the repo root for the
last-checked date. If it proves reliable enough over a few manual runs, it's
a reasonable candidate to move to `automated/` as a monthly or quarterly
cron, since the upsert logic is already idempotent.

## Update history

- **2026-09-20**: found Eurostat had published 2024 data (dataset updated
  2026-09-15) while Directus only had data through 2023. Proposed on Slack
  `#team_development`; see `notes/data-update-log.md` for details. Pending
  approval before running.
