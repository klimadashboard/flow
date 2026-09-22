# Data update task – running notes

Notes for the recurring "check for newer source data" task, so each run doesn't
have to rediscover the same things. Append, don't rewrite history here.

## Environment constraints (confirmed 2026-09-22)

- **Directus writes require a live approval.** The sandbox's auto-mode
  permission classifier denies any write call to `base.klimadashboard.org`
  (`Modify Shared Resources`) when no human is present to approve it in the
  moment. This means an unattended/scheduled run can validate a diff and get
  Slack sign-off, but the actual `--upload` / upsert step needs to happen in a
  session where a person can click "allow" — or David runs the already-approved
  script himself. Don't keep retrying different tool wrappers to route around
  this; it's a deliberate boundary, not a flaky permission.
- **`data.gv.at` and `umweltbundesamt.at` are network-blocked** from this
  environment (both via the direct proxy and via the auto-mode classifier,
  confirmed again on 2026-09-22, first found 2026-09-20). No static CSV/XLSX
  export of AT Bundesländer-level THG data has been found there for 2024 —
  only the interactive UBA dashboard and a news article. `ec.europa.eu`
  (Eurostat API) and `base.klimadashboard.org` (Directus) ARE reachable.

## Per-dataset status

### AT Bundesländer-Emissionen (BLI) — chart `emissionsByFederalStates` / `emissionsRegion`
- DB source label: `BLI 2025 (1990-2023)`. UBA published 2024 state-level data
  2026-05-28, no static file found (see above).
- Proposed 2026-09-20 in #team_development (thread on message `1789898794.439279`).
  **Blocked**: waiting on David to export/send the CSV from the UBA dashboard
  himself (asked in-thread). Do not re-propose; just check the thread for a
  reply/file before re-investigating.
- Script ready: `manual/at-uba-convert-emissions-bli.py` — once the CSV is in
  hand, update the `SOURCE_STATES` label to `"BLI 2026 (1990-2024)"`, dry-run,
  then upload.

### Mobility renewable share (Eurostat `nrg_ind_ren`, `REN_TRA`) — chart `mobilityRenewableShare`
- Table `mobility`, `category=share_renewable`. Proposed 2026-09-20 (thread on
  `1789898805.828689`), David approved same day ("yes, go").
- 2026-09-22: re-validated with a dry run against the live DB (731 existing
  rows / 39 regions). Diff: 38 new 2024 rows (one per tracked region/aggregate,
  incl. `EU27_2020`), 40 minor revisions to 2021-2023 (Eurostat finalizing
  provisional values — all small deltas except Norway, which moved up ~5pp
  across 2021-2023, still Eurostat-sourced, plausible). No dropped regions,
  `EA20` in our DB simply isn't in this Eurostat cut (pre-existing, unrelated).
  **Could not execute the actual upload** — blocked by the write-permission
  constraint above. Script `manual/eurostat-import.py` is upsert-safe (only
  touches `share_renewable` rows, compares by `(region, period, category)`,
  never deletes) — safe to just re-run as-is once someone can approve the
  write, no code changes needed.

## Process reminders
- One Slack message per proposed update in #team_development, wait for reply.
- Keep dry-run diffs (row counts, sample values) before any `--upload`/write,
  even after approval, and post them for the record.
- Don't rescan every chart every run — pick up open threads first, then a
  small number of new charts, to keep token spend down.
