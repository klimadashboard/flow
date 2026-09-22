# Data freshness review — working log

This is a recurring scheduled task: review charts in `klimadashboard/core`
against their original sources, check for newer official data, and if
found, propose a Directus update in `#team_development` on Slack *before*
writing anything. This file exists so future runs don't re-discover the
same context. Update it whenever you check a chart (even a "no update
needed" result, with the date) and whenever a proposal is sent/decided.

**History**: three cloud runs on 2026-09-20 left unmerged branches
(`claude/clever-shannon-8jhb71`, `-fomrd0`, `-8stuvg`). Their research was
sound and is consolidated here on `main`; two of them wrongly claimed
their Slack proposals had been sent. Root cause was environmental, not
analytical — see below. Since 2026-09-22 this routine runs locally.

## Two tasks, two cadences

Since 2026-09-22 this is split across two local scheduled tasks sharing
these two files:

- `daily-data-proposal-replies` — daily except Monday, 09:00. Reads the
  Slack threads of open proposals, executes what David approved, writes
  state back. No research, no new proposals. Exits immediately when
  nothing is open. This exists so replies are picked up within ~24h
  instead of waiting for the weekly run.
- `weekly-data-sources-check` — Mondays 09:00. The full review below.
  Also does the reply pickup first, so Monday needs no daily run.

Both commit only `notes/` to `main`. If you are the daily task, stop
reading here — the rest of this file is the weekly run's job.

## How this routine works

1. Pick a handful of charts/tables per run, not all of them (keep cost
   down — there are 60 chart dirs in `core`).
2. Trace: chart component in `core` → Directus table/category it reads →
   original data source (see `manual/`/`automated/` scripts, or the
   source named in the chart's `meta.source`).
3. Check the live source (its API/website) for data newer than what's in
   Directus (`base.klimadashboard.org`). **Only** count it if it's from
   the same scientific/official source already used — never introduce a
   new, previously-unused source without asking first.
4. Check whether an update script already exists (`manual/`/`automated/`)
   or needs writing; assess risk (additive/idempotent upsert vs.
   destructive replace).
5. Post **one proposal message per data update** to `#team_development`
   as `@klimadashbot` (short top-level message + full process/risk/links
   as a threaded reply), before writing anything to Directus. Wait for
   explicit go-ahead there — this routine never writes to Directus on its
   own, no matter how safe or idempotent the update looks.
6. Record the returned `ts` in `notes/data-watch-state.json`, then update
   this file regardless of outcome. At the start of every run, read the
   replies to every open proposal first — that is where approvals and
   source files arrive.

## Environment notes

This routine runs **locally** as a scheduled task on David's machine, in
the real clones at `~/Sites/klimadashboard-core` and
`~/Sites/klimadashboard-flow`. That means full network access to the
source websites/APIs, to `base.klimadashboard.org`, and the real `.env`
with `DIRECTUS_API_TOKEN`. Do not re-introduce the workarounds the
earlier cloud runs needed — if a host is unreachable now, that is a real
outage or a changed URL, not a sandbox restriction.

**Slack identity — this matters, an earlier run got it wrong.** The Slack
MCP connector posts as *David's personal account* (only tagged "Sent
using Claude"), which is wrong for automated proposals. Post as
`@klimadashbot` instead, via `automated/slack_bot.py`, which uses
`SLACK_BOT_TOKEN` from `.env`:

```bash
python3 automated/slack_bot.py post    --channel C0237PPU1J6 --text "..."
python3 automated/slack_bot.py reply   --channel C0237PPU1J6 --thread <ts> --text "..."
python3 automated/slack_bot.py replies --channel C0237PPU1J6 --thread <ts>
```

Every call prints JSON and exits non-zero on failure — a proposal only
counts as sent when `ok: true` came back and its `ts` was written into
`notes/data-watch-state.json`. Never record a message as sent without
that. Reading replies with the same script is how David's answers reach
the next run; he does not need to tag anyone.

## Chart → source registry (started 2026-09-20, partial — not all 60 dirs scanned)

**Backed by Directus `emissions_data`** (`source` field versioned like
`"BLI 2025 (1990-2023)"` / `"OLI 2025 (1990-2024)"`):
`emissionsByFederalStates`, `emissionsRegion`, `emissionsReductionBySector`,
`emissionsDetailedSectors`, `consumptionBasedEmissions` (SOURCES includes
`OLI 2025 (1990-2024)` + `manual_consumption`), `productionBasedEmissions`.

**Static CSV/JSON mirrors on `data.klimadashboard.org`** (no flow script
found that generates them — ask David where these are produced/uploaded
from before proposing changes here):
`historicalEmissions`, `co2budgetHistorical`, `co2budgetPaths` (PIK
PRIMAP-hist, `{VERSION}_Historical-Emissions_PIK-PRIMAP.csv`),
`globalEmissions` (`global/emissions/emissions_global.csv`),
`oilImportsDE` (Destatis, checked 2026-09-20 — up to date, 2025 is latest
complete year), `fossilEnergy`.

**Live external APIs, self-updating** (deprioritize — already current by
construction): `co2PriceHistory` (EU ETS via Directus, kept fresh by
`automated/` cron logged as "ETS-Preisscraper"), `renewablesTypes`,
`renewableShare`, `storageTypes`, `storageExplorer`, `renewablesExplorer`,
`carsHistoricLineChart`, `carsTypes`, `powerProductionExternal`,
`tramParking`/`tramParkingChart`.

**Manual scripts in `flow/manual/`** (the actual stale-data candidates,
since `automated/*.py` already self-refresh via cron):
- `at-uba-convert-emissions-bli.py` — AT Bundesländer + national THG
  emissions (Umweltbundesamt). See finding below.
- `eurostat-import.py` — generic Eurostat SDMX importer, currently wired
  to `nrg_ind_ren`/`REN_TRA` for `mobilityRenewableShare`. See finding
  below. Generalizes to any Eurostat dataset by changing 3 constants —
  worth reusing for other Eurostat-sourced charts before writing a new
  script from scratch.
- `klimadashboard-translation.py` — not a data-freshness concern (i18n
  helper).

**Not yet scanned**: ariadneExplorer, ariadneTinyMaps, carsArea,
carsDensity, climateQuiz, co2BudgetTimeline, companiesEmissions, coalMap,
gasImportsDE, gasUsageDE, handlungsbereitschaft, heatingFederalStates,
heatingHistorical, heatingPhaseout, heatingRegions, lngImportsDE, lngMap,
mobilityMap, modalSplit, renewableCounter, renewableGoalGaps,
renewableMaps, renewablePotentials, renewableProductionAndGoals(AT),
renewableRegions, renewablesWindMap, scenarios, snow, societyExplorer,
stationPicker, urbanSprawl, whiteChristmas, deathsHeat (spot-checked
2026-09-20 by a prior run — RKI data already includes 2026 nowcast rows,
looked current, not independently re-verified this run).

## Findings & proposals

### 1. AT Bundesländer THG-Emissionen 2024 (BLI) — proposed 2026-09-20

Umweltbundesamt published state-level (Bundesländer) GHG data for 2024 on
2026-05-28 (all states down vs. 2023: Steiermark ~10.9 Mt CO2e -2.0%,
Kärnten ~3.6 Mt -3.5%, Niederösterreich ~14.5 Mt -2.3%, Oberösterreich
~20.2 Mt -2.7%). Our DB's state-level data is still `BLI 2025
(1990-2023)`; the national total (`OLI 2025 (1990-2024)`) is already at
2024, so only the regional breakdown lags.

Existing tool: `manual/at-uba-convert-emissions-bli.py` (built for exactly
this source/format during the last BLI update, done by David in
April/May 2026 per Slack history). Would need: fresh UBA export (CSV/XLSX
in the same "long format, KSG-Kategorien" shape as before), `SOURCE_STATES`
label bumped to e.g. `"BLI 2026 (1990-2024)"`, dry run without `--upload`
to sanity-check row counts, then `--upload`.

**Blocker**: couldn't find a direct static CSV download link on the UBA
dashboard (`umweltbundesamt.at/klima/treibhausgase/dashboard-bundeslaender-emissionen`)
via WebFetch — looks interactive/JS-driven, same as last time. Asked
David in Slack to share the export or point to where to get it (matches
how the last update was sourced).

Slack: <https://klimadashboard.slack.com/archives/C0237PPU1J6/p1789898794439279>
(thread has full process/risk writeup).
**Status: awaiting David's reply / the source file.**

### 2. Eurostat renewable share in transport (`mobilityRenewableShare`) — proposed 2026-09-20

Eurostat dataset `nrg_ind_ren` (`nrg_bal=REN_TRA`) was updated 2026-09-15
with final (non-provisional) 2024 values for all geos we track. Our
Directus `mobility`/`share_renewable` table tops out at `period=2023`.

Existing tool: `manual/eurostat-import.py` — already generic and
idempotent (upserts by `region, period, category`, only touches
new/changed rows). No code change needed, just a re-run with a valid
`DIRECTUS_API_TOKEN`. See `manual/README-eurostat-import.md` for the full
writeup — worth reusing this script's pattern for any other
Eurostat-sourced chart found stale in a future run, instead of writing a
new importer each time.

Slack: <https://klimadashboard.slack.com/archives/C0237PPU1J6/p1789898805828689>
(thread has full process/risk writeup).
**Status: awaiting go-ahead.**

## Open items for next runs

- Once either proposal above is approved and run, update this file with
  the outcome (rows changed, new source label, any surprises) — don't
  leave it stuck on "awaiting" after the fact.
- Continue the unscanned-chart list above rather than restarting from
  chart #1 — pick a different slice each run.
- `historicalEmissions` (PIK PRIMAP-hist root CSV on
  `data.klimadashboard.org`): PRIMAP-hist v2.7 (2025) covers through 2024;
  couldn't verify what year our mirrored CSV currently has (host
  unreachable from this sandbox). Needs checking from an unrestricted
  environment, or ask David for the current max year in that CSV.
- If `eurostat-import.py` proves reliable after a couple of manual runs,
  consider moving it to `automated/` as a low-frequency (monthly/
  quarterly) cron — Eurostat's release timing is irregular but the
  upsert logic is already safe to run repeatedly.
