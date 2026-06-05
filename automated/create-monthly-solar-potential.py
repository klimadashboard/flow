"""
Monthly solar potential timeseries sync.

One GET call to /sync-solar-potential fetches raw monthly net deltas (added
minus removed) for every AGS code in Germany. The script then resolves each
region UUID to its AGS prefix, accumulates cumulative totals locally, and
bulk-writes the results to de_solar_potential_timeseries.

Every run overwrites all existing rows so backfilled MaStR data is always
reflected. New rows are inserted; existing rows are updated.

Cron (4th of each month, 23:00):
    0 23 4 * * python /path/to/create-monthly-solar-potential.py
"""

import os
import sys
import time
from collections import defaultdict
from datetime import date, timedelta
from urllib.parse import urlparse

import requests
from dotenv import load_dotenv
from slack_logger import slack_log

load_dotenv()

API_URL   = os.getenv("DIRECTUS_API_URL", "").rstrip("/")
API_TOKEN = os.getenv("DIRECTUS_API_TOKEN", "")
BATCH_SIZE = int(os.getenv("DIRECTUS_BATCH_SIZE", 1000))

_parsed = urlparse(API_URL)
BASE_URL = f"{_parsed.scheme}://{_parsed.netloc}"

HEADERS = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Content-Type": "application/json",
}

TARGET_TABLE  = "de_solar_potential_timeseries"
BACKFILL_START = date(2025, 1, 31)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def log(msg: str, level: str = "INFO") -> None:
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} {level}: {msg}", flush=True)


def last_day_of_month(year: int, month: int) -> date:
    if month == 12:
        return date(year + 1, 1, 1) - timedelta(days=1)
    return date(year, month + 1, 1) - timedelta(days=1)


def generate_month_ends(start: date, end: date) -> list[date]:
    months = []
    year, month = start.year, start.month
    while True:
        d = last_day_of_month(year, month)
        if d > end:
            break
        months.append(d)
        month += 1
        if month > 12:
            month = 1
            year += 1
    return months


def fetch_all_pages(collection: str, params: dict | None = None) -> list[dict]:
    params = params or {}
    page_size = 10_000
    offset = 0
    all_records: list[dict] = []
    while True:
        p = {**params, "limit": page_size, "offset": offset}
        resp = requests.get(
            f"{API_URL}/items/{collection}", headers=HEADERS, params=p, timeout=120
        )
        resp.raise_for_status()
        data = resp.json().get("data", [])
        all_records.extend(data)
        if len(data) < page_size:
            break
        offset += page_size
    return all_records


def flush_batch(records: list[dict], method: str) -> tuple[int, int]:
    if not records:
        return 0, 0
    if method == "insert":
        resp = requests.post(f"{API_URL}/items/{TARGET_TABLE}", json=records, headers=HEADERS, timeout=120)
    else:
        resp = requests.patch(f"{API_URL}/items/{TARGET_TABLE}", json=records, headers=HEADERS, timeout=120)
    if resp.ok:
        return len(records), 0
    log(f"  {method.capitalize()} error {resp.status_code}: {resp.text[:200]}", "ERROR")
    return 0, 1


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    start_time = time.time()
    log("Starting solar potential timeseries sync")
    slack_log("Starting solar potential timeseries sync", level="INFO")

    # ------------------------------------------------------------------
    # 1. Fetch raw monthly deltas from server (one call)
    # ------------------------------------------------------------------
    log("Fetching monthly AGS deltas from server …")
    resp = requests.get(f"{BASE_URL}/get-solar-potential-deltas", headers=HEADERS, timeout=180)
    resp.raise_for_status()
    raw_deltas: list[dict] = resp.json()
    log(f"  {len(raw_deltas)} delta rows received")

    # ------------------------------------------------------------------
    # 2. Fetch region metadata
    # ------------------------------------------------------------------
    log("Fetching de_solar_potential_static …")
    static_rows = fetch_all_pages("de_solar_potential_static", {"fields": "region,potential,roofs_count"})
    log(f"  {len(static_rows)} static entries")

    log("Fetching regions metadata …")
    region_rows = fetch_all_pages("regions", {
        "fields": "id,code_short,layer",
        "filter[country][_eq]": "DE",
    })
    regions_by_id: dict[str, dict] = {r["id"]: r for r in region_rows if r.get("id")}
    log(f"  {len(regions_by_id)} DE regions loaded")

    # ------------------------------------------------------------------
    # 3. Build prefix lookup maps and static data index
    #    state    → match on first 2 chars of AGS code
    #    district → match on first 5 chars of AGS code
    #    municipality → exact match
    # ------------------------------------------------------------------
    state_prefix_map:    dict[str, list[str]] = defaultdict(list)
    district_prefix_map: dict[str, list[str]] = defaultdict(list)
    muni_code_map:       dict[str, list[str]] = defaultdict(list)
    static_by_region:    dict[str, dict]      = {}

    for row in static_rows:
        uuid = row.get("region")
        if not uuid:
            continue
        meta  = regions_by_id.get(uuid, {})
        code  = str(meta.get("code_short") or "").strip()
        layer = meta.get("layer") or "municipality"
        static_by_region[uuid] = {
            "potential_mwh": float(row.get("potential") or 0.0),
            "roofs_count":   int(row["roofs_count"]) if row.get("roofs_count") else 0,
        }
        if not code:
            continue
        if layer == "state":
            state_prefix_map[code[:2]].append(uuid)
        elif layer == "district":
            district_prefix_map[code[:5]].append(uuid)
        else:
            muni_code_map[code].append(uuid)

    log(f"  Prefix maps built for {len(static_by_region)} regions")

    # ------------------------------------------------------------------
    # 4. Apply prefix matching: accumulate deltas per (region_uuid, ym)
    #    ym = year * 100 + month  (e.g. 202501)
    # ------------------------------------------------------------------
    region_deltas: dict[str, dict[int, dict]] = defaultdict(lambda: defaultdict(lambda: {"power": 0.0, "units": 0}))

    for row in raw_deltas:
        ags   = str(row.get("ags_code") or "").strip()
        year  = int(row["year"])
        month = int(row["month"])
        ym    = year * 100 + month
        power = float(row.get("delta_power_kw") or 0.0)
        units = int(row.get("delta_units") or 0)

        for uuid in state_prefix_map.get(ags[:2], []):
            region_deltas[uuid][ym]["power"] += power
            region_deltas[uuid][ym]["units"] += units

        if len(ags) >= 5:
            for uuid in district_prefix_map.get(ags[:5], []):
                region_deltas[uuid][ym]["power"] += power
                region_deltas[uuid][ym]["units"] += units

        for uuid in muni_code_map.get(ags, []):
            region_deltas[uuid][ym]["power"] += power
            region_deltas[uuid][ym]["units"] += units

    log(f"  Prefix matching complete — {len(region_deltas)} regions have activity data")

    # ------------------------------------------------------------------
    # 5. Load existing rows for insert/update detection
    # ------------------------------------------------------------------
    log("Fetching existing timeseries rows …")
    existing_rows = fetch_all_pages(TARGET_TABLE, {"fields": "id,region,date"})
    existing_map: dict[tuple[str, str], str] = {
        (r["region"], r["date"][:10]): r["id"] for r in existing_rows
    }
    log(f"  {len(existing_map)} existing rows in {TARGET_TABLE}")

    # ------------------------------------------------------------------
    # 6. Generate target month-end dates
    # ------------------------------------------------------------------
    today = date.today()
    prev_month_end = date(today.year, today.month, 1) - timedelta(days=1)
    month_ends = generate_month_ends(BACKFILL_START, prev_month_end)
    if not month_ends:
        log("No months to process — nothing to do.")
        return
    log(f"Processing {len(month_ends)} months: {month_ends[0]} → {month_ends[-1]}")

    # ------------------------------------------------------------------
    # 7. Compute cumulative totals and build insert/update lists
    # ------------------------------------------------------------------
    all_inserts: list[dict] = []
    all_updates: list[dict] = []
    backfill_ym = BACKFILL_START.year * 100 + BACKFILL_START.month

    for uuid, static in static_by_region.items():
        potential_mwh = static["potential_mwh"]
        roofs_count   = static["roofs_count"]
        deltas        = region_deltas.get(uuid, {})

        # Sum up everything before the backfill window as a starting baseline
        cumulative_power = sum(d["power"] for ym, d in deltas.items() if ym < backfill_ym)
        cumulative_units = sum(d["units"] for ym, d in deltas.items() if ym < backfill_ym)

        for month_end in month_ends:
            ym    = month_end.year * 100 + month_end.month
            delta = deltas.get(ym, {"power": 0.0, "units": 0})
            cumulative_power += delta["power"]
            cumulative_units += delta["units"]

            net_power_kw = round(cumulative_power, 4)
            units_count  = int(cumulative_units)
            date_str     = month_end.isoformat()

            record: dict = {
                "region":             uuid,
                "date":               date_str,
                "net_power_kw":       net_power_kw,
                "net_potential_share": round((net_power_kw / potential_mwh) * 100.0, 4) if potential_mwh > 0 else 0.0,
                "roofs_solar_share":  round((units_count  / roofs_count)    * 100.0, 4) if roofs_count   > 0 else 0.0,
                "units_count":        units_count,
            }

            existing_id = existing_map.get((uuid, date_str))
            if existing_id:
                record["id"] = existing_id
                all_updates.append(record)
            else:
                all_inserts.append(record)

    log(f"Queued {len(all_inserts)} inserts, {len(all_updates)} updates")

    # ------------------------------------------------------------------
    # 8. Bulk-write to Directus
    # ------------------------------------------------------------------
    total_inserted = total_updated = total_errors = 0

    log(f"Writing {len(all_inserts)} inserts …")
    n_insert_batches = max(1, (len(all_inserts) + BATCH_SIZE - 1) // BATCH_SIZE)
    for batch_num, i in enumerate(range(0, len(all_inserts), BATCH_SIZE), 1):
        n, e = flush_batch(all_inserts[i : i + BATCH_SIZE], "insert")
        total_inserted += n
        total_errors   += e
        log(f"  inserts [{batch_num}/{n_insert_batches}] — {total_inserted} rows written")

    log(f"Writing {len(all_updates)} updates …")
    n_update_batches = max(1, (len(all_updates) + BATCH_SIZE - 1) // BATCH_SIZE)
    for batch_num, i in enumerate(range(0, len(all_updates), BATCH_SIZE), 1):
        n, e = flush_batch(all_updates[i : i + BATCH_SIZE], "update")
        total_updated += n
        total_errors  += e
        if batch_num % 10 == 0 or batch_num == n_update_batches:
            elapsed = round(time.time() - start_time)
            log(f"  updates [{batch_num}/{n_update_batches}] — {total_updated} rows written, {elapsed}s elapsed")

    duration = round(time.time() - start_time)
    summary = f"inserted: {total_inserted}, updated: {total_updated}, errors: {total_errors}"
    log(f"Finished in {duration}s — {summary}")
    level = "ERROR" if total_errors else "SUCCESS"
    slack_log(f"Solar potential timeseries sync finished in {duration}s — {summary}", level=level)


if __name__ == "__main__":
    if not API_URL or not API_TOKEN:
        sys.exit("ERROR: DIRECTUS_API_URL or DIRECTUS_API_TOKEN not set in environment / .env")
    main()
