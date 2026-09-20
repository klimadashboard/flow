# Data update review notes

This file is working memory for the recurring "check for newer source data"
routine (see `#team_development` on Slack for the proposal/approval flow).
Its purpose is to avoid re-discovering the same context (chart → table →
source mapping, release cadence, last-checked date) on every run, and to
let each run sample a different slice of charts instead of re-scanning
everything.

## Slack posting conventions (always follow these)

- **Post as Klimadashbot, not the personal Slack account.** Use the
  `SLACK_BOT_TOKEN` from `.env` (the "Klimadashbot" app,
  `chat.postMessage` via the plain Slack Web API, e.g. `curl -X POST
  https://slack.com/api/chat.postMessage -H "Authorization: Bearer
  $SLACK_BOT_TOKEN" ...`) — do **not** use a Slack tool/integration that's
  tied to David's own account for these proposals.
- **Keep the top-level channel message minimal.** One or two lines: what
  the update is (chart/table + source) and that a proposal/result is
  ready, nothing else.
- **Put everything else — process, risk assessment, numbers, links to
  the doc/README — as a threaded reply** under that message (or a
  follow-up threaded reply once the update is actually run), not in the
  channel message itself.
- One Slack thread per data update, as before (one top-level message per
  proposed update, never bundle several).

## How this routine works

1. Pick a handful of charts/tables (not all of them, to keep cost down).
2. Trace: chart component in `core` → Directus table/category it reads →
   original data source (see `manual/`/`automated/` scripts or the source
   named in the chart's `meta.source`).
3. Check the live source (its API/website) for newer data than what's
   currently in Directus at `base.klimadashboard.org`.
4. Only count it as "newer data" if it comes from the same
   scientific/official source already used — never swap in a new,
   previously-unused source.
5. If newer data exists, check whether an update script already exists
   (`manual/` or `automated/`) or needs to be written, assess risk
   (additive vs. destructive, whether it can be idempotent upsert), and
   post ONE proposal message per data update to `#team_development`
   before writing anything to Directus. Wait for explicit go-ahead there.
6. Update this file with what was checked and when, regardless of outcome
   (found an update or not), so future runs don't redo the same check
   right away.

## Chart/table inventory checked so far

| Chart (in `core`) | Directus table.category | Source | Cadence (observed) | Last checked | Status |
|---|---|---|---|---|---|
| `mobilityRenewableShare` | `mobility` / `share_renewable` | Eurostat `nrg_ind_ren` (REN_TRA) | Annual, Eurostat publishes with ~1yr lag, revised periodically | 2026-09-20 | **Stale — proposed update, see below** |
| `oilImportsDE` | static CSV `data.klimadashboard.org/de/energy/fossil/oil_imports-destatis.csv` | Destatis foreign trade stats | Annual | 2026-09-20 | Up to date (2025 is latest complete year, matches Destatis) |
| AT emissions (Bundesländer/Österreich) | `emissions_data`, source `BLI 2025`/`OLI 2025` | Umweltbundesamt AT (Klimaschutzbericht) | Annual, ~Q1/Q2 | 2026-09-20 (from Slack history, updated April 2026) | Up to date as of last manual update |
| DE emissions (Bundesländer) | `emissions_data` | UBA/Bundesländer-Initiative | Annual | 2026-09-20 (from Slack history, Cedric posted CSV 2026-08-07) | Recently updated by team, not re-checked in depth this run |

Charts backed by `automated/*.py` cron jobs (DWD, Geosphere, ENTSO-E,
Marktstammdatenregister, gas usage/import, global CO2 concentration,
renewable share, tram parking, solar potential) were **not** re-checked
this run — they run on a schedule already and should self-refresh. Worth
spot-checking occasionally that the cronjobs are actually still running
(check `automated/slack_logger.py` output / server crontab), but that's an
ops check, not a "newer source data" check.

## 2026-09-20 finding: `mobility` / `share_renewable` (Eurostat `nrg_ind_ren`)

- Directus (`base.klimadashboard.org/items/mobility`, filter
  `category=share_renewable`) tops out at period **2023** (36 rows), 0 rows
  for 2024.
- Eurostat dataset `nrg_ind_ren` (share of renewable energy in transport,
  `nrg_bal=REN_TRA`), the same dataset `manual/eurostat-import.py` already
  imports, was updated **2026-09-15** and now has **2024** data for 38
  geos (all EU/EFTA/candidate countries currently in our table, plus
  `EU27_2020` aggregate). No `OBS_FLAG` (provisional) markers — these are
  final annual values.
- `manual/eurostat-import.py` already generalizes over dataset/table/
  category and upserts by `(region, period, category)`, only inserting/
  updating rows that are new or changed — re-running it as-is should be
  sufficient, no code changes needed.
- Proposed on Slack `#team_development` on 2026-09-20, awaiting go-ahead.
  See `manual/README-eurostat-import.md` for the process writeup.

## Open items for next runs

- Haven't yet checked: `renewableShare` (`energy_renewable_share`),
  `carsHistoricLineChart`/`carsTypes` (KBA vehicle registrations),
  `co2PriceHistory`, `heatingFederalStates`/`heatingHistorical`,
  `renewablePotentials`, `temperature`/DWD-adjacent manual charts,
  `historicalEmissions` root data at `data.klimadashboard.org`.
- Consider whether `manual/eurostat-import.py` should move to
  `automated/` as a low-frequency cron (e.g. monthly) once we've run it
  manually a few times and trust it — Eurostat release timing is
  irregular but the upsert logic is already idempotent/non-destructive.
