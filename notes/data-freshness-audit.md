# Data freshness audit — working notes

Context: recurring scheduled task that reviews Klimadashboard charts (in
`core`) against their original sources and checks for newer official data.
Process per the task brief: investigate → if a genuine update is found,
draft the update approach (Python snippet under `flow/manual/` or
`flow/automated/` + README) → propose it in #team_development on Slack
*before* writing to Directus → wait for explicit go-ahead from David.

This file exists so future runs don't re-discover the same things. Update
it whenever you check a chart, even if nothing was found (record the date
so we don't re-check too often), and whenever a proposal is sent/decided.

Environment constraints found in the sandbox session (2026-09-20):
- No live `DIRECTUS_API_URL`/`DIRECTUS_API_TOKEN` in this sandbox — actual
  writes need to happen on the real data server (or a session with the
  flow `.env` populated). This run only prepared code + a Slack proposal.
- `WebFetch` is restricted to an explicit allowlist and could not reach
  `umweltbundesamt.at`, `data.gv.at`, or `data.klimadashboard.org` — only
  `WebSearch` (snippets) worked. Verifying exact download URLs / file
  structure needs to happen from a less restricted environment, or David
  needs to confirm/share the source file.

## Chart → source registry (built 2026-09-20, partial — 61 chart dirs total, not all scanned yet)

Charts backed by **Directus** (`emissions_data` collection, `source` field
versioned like `"BLI 2025 (1990-2023)"` / `"OLI 2025 (1990-2024)"`):
- `consumptionBasedEmissions` — meta.source "UBA, Global Carbon Project"; SOURCES = ['OLI 2025 (1990-2024)', 'manual_consumption']
- `productionBasedEmissions` — meta.source "Bundesländer Inventur, Umweltbundesamt"
- `emissionsByFederalStates` — same UBA source
- `emissionsRegion`, `emissionsReductionBySector` — dynamic `source` field per region/category, likely same BLI/OLI family — **not yet inspected in detail**

Charts fetching **static CSV/JSON mirrors** on `data.klimadashboard.org`
(uploaded manually somewhere outside both repos — no flow script found that
generates them):
- `historicalEmissions`, `co2budgetHistorical`, `co2budgetPaths` — PIK PRIMAP-hist, per-country CSV named `{VERSION}_Historical-Emissions_PIK-PRIMAP.csv`
- `globalEmissions` — `global/emissions/emissions_global.csv`
- `oilImportsDE` — Destatis oil imports CSV
- `fossilEnergy` — NEA/fossil usage CSVs
- others not yet checked: `gasImportsDE`, `lngImportsDE`, `lngMap`, `coalMap`, `gasUsageDE`, `carsDensity`, etc.

Charts fetching **live external APIs directly** (self-updating, no manual
step needed): `co2PriceHistory` (EU ETS), `renewablesTypes`,
`renewableShare`, `storageTypes`, `storageExplorer`, `renewablesExplorer`,
`carsHistoricLineChart`, `carsTypes`, `powerProductionExternal`,
`tramParking(Chart)` — deprioritize these for the freshness audit, they're
already current by construction.

**Not yet scanned at all** (~35 remaining dirs): ariadneExplorer,
ariadneTinyMaps, carsArea, climateQuiz, co2BudgetTimeline,
co2budgetHistorical (partially — see above), companiesEmissions, coalMap,
deathsHeat (checked, see below), emissionsDetailedSectors, gasImportsDE,
gasUsageDE, handlungsbereitschaft, heatingFederalStates, heatingHistorical,
heatingPhaseout, heatingRegions, lngImportsDE, lngMap, mobilityMap,
mobilityRenewableShare, modalSplit, oilImportsDE (checked), renewableCounter,
renewableGoalGaps, renewableMaps, renewablePotentials,
renewableProductionAndGoals(AT), renewableRegions, renewablesWindMap, scenarios,
snow, societyExplorer, stationPicker, tramParkingChart, urbanSprawl,
whiteChristmas.

## Checked this run (2026-09-20)

- **`historicalEmissions`** (PIK PRIMAP-hist): PRIMAP-hist v2.7 was released
  in 2025 covering data through 2024. Could not verify which version our
  mirrored CSV currently contains (data.klimadashboard.org blocked from
  this sandbox). **Needs follow-up**: check the CSV's current max year from
  an unrestricted environment before proposing anything.
- **`consumptionBasedEmissions`** (UBA + Global Carbon Project): current
  DB source `OLI 2025 (1990-2024)` already covers through 2024. Global
  Carbon Budget is annual every November; GCB2025 (Nov 2025) data is what's
  already reflected. Next update won't be available until ~Nov 2026.
  **No action needed now** — recheck after Nov 2026.
- **`deathsHeat`** (Robert Koch-Institut): data already includes rows into
  2026 (nowcast), looks current. **No action needed.**
- **UBA Bundesländer/Österreichische Luftschadstoffinventur (BLI/OLI)** —
  see proposal below. **Sent to Slack 2026-09-20.**

## Proposals sent

### 1. Austrian Bundesländer THG emissions 2024 (BLI update) — sent 2026-09-20
Channel: #team_development. Status: **awaiting David's reply.**
Summary: Umweltbundesamt published state-level (Bundesländer) GHG data for
2024 on 2026-05-28 (current DB only has state data through 2023, via
`BLI 2025 (1990-2023)`). Also flagged a possible revision of the national
2024 total (nowcast -2.6% announced Aug 2025 vs. a later -3%/66.6 Mt figure
that appears in the Klimaschutzbericht 2026 / Jan 2026 announcement) —
needs verification against the actual source file before deciding whether
`OLI 2025 (1990-2024)` also needs replacing.
Existing tool to reuse: `flow/manual/at-uba-convert-emissions-bli.py`
(already parametrized for this exact source/format — new run would just
need the fresh CSV export from Umweltbundesamt and updated `SOURCE_STATES`/
`SOURCE_AUSTRIA` labels, e.g. `"BLI 2026 (1990-2024)"`).

## Open questions for David
- Can someone download the actual Umweltbundesamt Bundesländer 2024 export
  (Excel/CSV) — the sandbox can't reach umweltbundesamt.at directly?
- Is the Aug-2025-vs-Jan-2026 national 2024 figure discrepancy a
  nowcast→final revision, or did I misread two different searches?
