"""
Marktstammdatenregister Storage Sync.

Downloads storage unit data from MaStR and syncs to Directus.
Follows the OpenEnergyTracker methodology for battery plausibility filtering.
Reference: https://gitlab.com/diw-evu/oet/openenergytracker/-/blob/main/mastr/mastr_2_4_battery_storage.py

Usage:
    python marktstammdatenregister-storage-sync.py                  # incremental (last N days)
    python marktstammdatenregister-storage-sync.py --full           # full reload, skip unchanged
    python marktstammdatenregister-storage-sync.py --rebuild        # clear table, re-insert everything
    python marktstammdatenregister-storage-sync.py --skip-download  # skip MaStR download (parent already did it)
"""

import os
import sys
import time
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy import inspect as sa_inspect
from dotenv import load_dotenv
from open_mastr import Mastr

try:
    from slack_logger import slack_log
except ImportError:
    def slack_log(msg, level="INFO"):
        print(f"[SLACK {level}] {msg}")

load_dotenv()

DIRECTUS_URL = os.getenv("DIRECTUS_API_URL")
DIRECTUS_TOKEN = os.getenv("DIRECTUS_API_TOKEN")
TABLE_NAME = "energy_storage_units"
BATCH_SIZE = int(os.getenv("DIRECTUS_BATCH_SIZE", 1000))
UPDATE_DAYS_BACK = int(os.getenv("UPDATE_DAYS_BACK", 10))
SKIP_DOWNLOAD = "--skip-download" in sys.argv
REBUILD = "--rebuild" in sys.argv
FULL_LOAD = "--full" in sys.argv or REBUILD

HEADERS = {
    "Authorization": f"Bearer {DIRECTUS_TOKEN}",
    "Content-Type": "application/json",
}

THRESHOLD_DATE = datetime.today() - timedelta(days=UPDATE_DAYS_BACK)

# One entry manually restored by OET after filter 4 (known valid large unit)
_OET_FILTER4_EXCEPTIONS = {"SSE978006940074"}


def log(msg, level="INFO"):
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {level}: {msg}")


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def download_storage_data():
    """
    Download storage data from MaStR and return a DataFrame.

    open-mastr populates two relevant SQLite tables from the bulk export:
      storage_extended  – one row per generation unit (Erzeugungseinheit)
      storage_units     – one row per storage plant (Speicheranlage), holds
                          NutzbareSpeicherkapazitaet

    We load storage_extended, then merge NutzbareSpeicherkapazitaet from
    storage_units via SpeMastrNummer → MastrNummer, which is how OET does it.
    """
    log("Initializing open-mastr...")
    db = Mastr()

    if not SKIP_DOWNLOAD:
        log("Downloading storage data from MaStR...")
        db.download(data=["storage"], bulk_cleansing=True)
    else:
        log("Skipping download (data pre-loaded by parent).")

    # Log available tables to help diagnose future format changes
    available_tables = sa_inspect(db.engine).get_table_names()
    log(f"Available SQLite tables: {available_tables}")

    # Detect available columns without loading all data
    probe = pd.read_sql("SELECT * FROM storage_extended LIMIT 1", con=db.engine)
    all_columns = probe.columns.tolist()
    date_col = next(
        (c for c in ["DatumLetzteAktualisierung", "date_last_update"] if c in all_columns),
        None,
    )

    log("Loading storage data from database...")
    if not FULL_LOAD and date_col:
        threshold_str = THRESHOLD_DATE.strftime("%Y-%m-%d")
        log(f"Incremental filter: {date_col} >= {threshold_str}")
        df = pd.read_sql(
            f"SELECT * FROM storage_extended WHERE {date_col} >= '{threshold_str}'",
            con=db.engine,
        )
        log(f"Loaded {len(df):,} storage units updated since {threshold_str}.")
    elif not FULL_LOAD:
        log("No update-date column found; loading all entries.", level="WARNING")
        df = pd.read_sql("SELECT * FROM storage_extended", con=db.engine)
        log(f"Loaded {len(df):,} storage units.")
    else:
        # Full/rebuild: read in chunks to avoid OOM on large tables
        log("Full mode: reading storage_extended in chunks...")
        chunks = []
        chunk_size = 100_000
        for chunk in pd.read_sql("SELECT * FROM storage_extended", con=db.engine, chunksize=chunk_size):
            chunks.append(chunk)
            log(f"  ...read {sum(len(c) for c in chunks):,} rows so far")
        df = pd.concat(chunks, ignore_index=True)
        log(f"Loaded {len(df):,} storage units total.")

    # Merge NutzbareSpeicherkapazitaet from storage_units via SpeMastrNummer.
    # Drop storage_extended's own column first to avoid conflicts — the
    # storage_units value is authoritative (plant-level, not unit-level).
    if "storage_units" in available_tables and "SpeMastrNummer" in df.columns:
        try:
            storage_units = pd.read_sql(
                "SELECT MastrNummer, NutzbareSpeicherkapazitaet FROM storage_units",
                con=db.engine,
            )
            log(f"Loaded {len(storage_units):,} entries from storage_units.")
            df = df.drop(columns=["NutzbareSpeicherkapazitaet"], errors="ignore")
            df = df.merge(
                storage_units,
                left_on="SpeMastrNummer",
                right_on="MastrNummer",
                how="left",
                suffixes=("", "_plant"),
            )
            filled = df["NutzbareSpeicherkapazitaet"].notna().sum()
            log(f"Capacity merged: {filled:,}/{len(df):,} units have NutzbareSpeicherkapazitaet")
        except Exception as e:
            log(f"Could not merge storage_units: {e}", level="WARNING")
    else:
        native_filled = df["NutzbareSpeicherkapazitaet"].notna().sum() if "NutzbareSpeicherkapazitaet" in df.columns else 0
        log(f"storage_units table not available; using storage_extended directly ({native_filled:,} capacity values)")

    return df, db


# ---------------------------------------------------------------------------
# Plausibility pipeline (exactly as OET mastr_2_4_battery_storage.py)
# ---------------------------------------------------------------------------

def apply_plausibility_checks(df):
    """
    OET plausibility pipeline for battery storage entries.

    Applies the four sequential filters from openenergytracker to battery
    entries. Non-battery technologies pass through with the technology name
    as their category.
    """
    batteries = df[df["Technologie"] == "Batterie"].copy()
    non_batteries = df[df["Technologie"] != "Batterie"].copy()
    non_batteries["category"] = non_batteries["Technologie"]

    n_start = len(batteries)
    log(f"Battery entries before filtering: {n_start}")

    # Filter 1: Batterietechnologie must be defined
    batteries = batteries.dropna(subset=["Batterietechnologie"])
    log(f"Filter 1 (battery technology defined): {len(batteries)} kept, {n_start - len(batteries)} dropped")

    # Filter 2: E/P ratio between 0.1 h and 15 h
    ep = batteries["NutzbareSpeicherkapazitaet"] / batteries["Nettonennleistung"]
    batteries = batteries[ep.between(0.1, 15)]
    log(f"Filter 2 (E/P ratio 0.1–15 h): {len(batteries)} kept, {n_start - len(batteries)} total dropped so far")

    # Filter 3: Sort by commissioning date
    batteries["Inbetriebnahmedatum"] = pd.to_datetime(batteries["Inbetriebnahmedatum"], errors="coerce")
    batteries = batteries.sort_values("Inbetriebnahmedatum").reset_index(drop=True)

    # Filter 4: Remove unreviewed large-capacity entries (> 1000 kWh, not grid-reviewed)
    # Exception: OET manually restores SSE978006940074
    mask_remove = (
        (batteries["NutzbareSpeicherkapazitaet"] > 1000)
        & (batteries["NetzbetreiberpruefungStatus"] == "0")
        & (~batteries["SpeMastrNummer"].isin(_OET_FILTER4_EXCEPTIONS))
    )
    batteries = batteries[~mask_remove]
    log(f"Filter 4 (large unreviewed removed): {len(batteries)} kept")

    # Categorise by capacity (exactly as OET's pd.cut)
    batteries["category"] = pd.cut(
        batteries["NutzbareSpeicherkapazitaet"],
        bins=[-np.inf, 30, 1000, np.inf],
        labels=["Heimspeicher", "Gewerbespeicher", "Grossspeicher"],
    ).astype(str)

    result = pd.concat([batteries, non_batteries], ignore_index=True)
    log(f"Plausibility complete: {len(result)} records ({n_start - len(batteries)} batteries dropped)")
    return result


# ---------------------------------------------------------------------------
# Map to Directus schema
# ---------------------------------------------------------------------------

def map_to_directus_schema(df):
    """Map open-mastr column names to Directus field names and coerce types."""
    STATUS_CODES = {
        "In Betrieb": "35",
        "Voruebergehend stillgelegt": "37",
        "Endgueltig stillgelegt": "38",
        "In Planung": "31",
    }

    column_mapping = {
        "EinheitMastrNummer": "id",
        "NameStromerzeugungseinheit": "name",
        "EinheitBetriebsstatus": "status",
        "Inbetriebnahmedatum": "commissioning_date",
        "DatumLetzteAktualisierung": "last_update",
        "DatumEndgueltigeStilllegung": "shutdown_date",
        "GeplantesInbetriebnahmedatum": "planned_commissioning_date",
        "Energietraeger": "energy_source",
        "Technologie": "technology",
        "Batterietechnologie": "battery_technology",
        "Pumpspeichertechnologie": "pumped_storage_technology",
        "Bruttoleistung": "power_kw",
        "Nettonennleistung": "net_power_kw",
        "NutzbareSpeicherkapazitaet": "usable_storage_capacity_kwh",
        "PumpbetriebLeistungsaufnahme": "pump_power_kw",
        "ZugeordneteWirkleistungWechselrichter": "inverter_power_kw",
        "AcDcKoppelung": "ac_dc_coupling",
        "AnschlussAnHoechstOderHochSpannung": "high_voltage_connection",
        "Einspeisungsart": "feed_in_type",
        "AnlagenbetreiberMastrNummer": "operator_name",
        "GemeinsamRegistrierteSolareinheitMastrNummer": "co_registered_solar_unit",
        "Einsatzort": "deployment_location",
        "Bundesland": "federal_state",
        "Landkreis": "district",
        "Gemeinde": "municipality",
        "Gemeindeschluessel": "region",
        "Postleitzahl": "postcode",
        "Breitengrad": "lat",
        "Laengengrad": "lon",
        "NetzbetreiberpruefungStatus": "grid_operator_review_status",
        "SpeMastrNummer": "storage_mastr_number",
        "category": "category",
    }

    actual_mapping = {k: v for k, v in column_mapping.items() if k in df.columns}
    mapped = df[list(actual_mapping.keys())].rename(columns=actual_mapping).copy()

    if "id" not in mapped.columns:
        log("No ID column found!", level="ERROR")
        return []

    mapped["id"] = mapped["id"].astype(str).str.strip()
    mapped = mapped[mapped["id"].notna() & ~mapped["id"].isin(["", "nan", "None"])].copy()

    n_before = len(mapped)
    mapped = mapped.drop_duplicates(subset=["id"], keep="first")
    if len(mapped) < n_before:
        log(f"Dropped {n_before - len(mapped):,} duplicate IDs")

    mapped["country"] = "DE"

    for col in {"power_kw", "net_power_kw", "usable_storage_capacity_kwh",
                "pump_power_kw", "inverter_power_kw", "lat", "lon"} & set(mapped.columns):
        mapped[col] = pd.to_numeric(
            mapped[col].astype(str).str.replace(",", ".", regex=False), errors="coerce"
        )

    for col in {"commissioning_date", "shutdown_date", "planned_commissioning_date"} & set(mapped.columns):
        ts = pd.to_datetime(mapped[col], errors="coerce")
        mapped[col] = ts.dt.strftime("%Y-%m-%d").where(ts.notna(), None)

    for col in {"last_update"} & set(mapped.columns):
        ts = pd.to_datetime(mapped[col], errors="coerce")
        mapped[col] = ts.dt.strftime("%Y-%m-%dT%H:%M:%S").where(ts.notna(), None)

    for col in {"high_voltage_connection"} & set(mapped.columns):
        na_mask = mapped[col].isna()
        mapped[col] = mapped[col].astype(str).isin(["1", "true", "True", "ja", "Ja"]).astype(object)
        mapped.loc[na_mask, col] = None

    if "status" in mapped.columns:
        mapped["status"] = (
            mapped["status"].astype(str)
            .map(lambda v: STATUS_CODES.get(v, v))
            .where(mapped["status"].notna(), None)
        )

    string_cols = {
        "technology", "battery_technology", "pumped_storage_technology", "ac_dc_coupling",
        "deployment_location", "federal_state", "district", "municipality",
        "grid_operator_review_status", "co_registered_solar_unit", "storage_mastr_number",
        "region", "postcode", "name", "energy_source", "feed_in_type", "operator_name", "category",
    }
    for col in string_cols & set(mapped.columns):
        na_mask = mapped[col].isna()
        mapped.loc[~na_mask, col] = mapped.loc[~na_mask, col].astype(str)
        mapped.loc[na_mask, col] = None

    mapped = mapped.astype(object).where(mapped.notna(), None)
    records = mapped.to_dict("records")
    log(f"Mapped {len(records)} records")
    return records


# ---------------------------------------------------------------------------
# Directus API helpers
# ---------------------------------------------------------------------------

def _normalize_ts(ts_str):
    """Normalize a timestamp string to YYYY-MM-DDTHH:MM:SS for comparison."""
    if not ts_str:
        return None
    return str(ts_str)[:19]


def get_existing_ids(ids):
    existing = set()
    for i in range(0, len(ids), 100):
        chunk = ids[i:i + 100]
        try:
            r = requests.get(
                f"{DIRECTUS_URL}/items/{TABLE_NAME}",
                headers=HEADERS,
                params={"filter[id][_in]": ",".join(chunk), "fields": "id", "limit": -1},
                timeout=60,
            )
            if r.status_code == 200:
                existing.update(item["id"] for item in r.json().get("data", []))
            else:
                log(f"Error querying IDs: {r.status_code}", level="WARNING")
        except Exception as e:
            log(f"Error checking IDs: {e}", level="WARNING")
    return existing


def batch_upsert(batch, op, max_retries=3):
    if not batch:
        return 0
    url = f"{DIRECTUS_URL}/items/{TABLE_NAME}"
    for attempt in range(max_retries):
        try:
            fn = requests.post if op == "insert" else requests.patch
            r = fn(url, json=batch, headers=HEADERS, timeout=120)
            if r.status_code in [200, 201, 204]:
                return len(batch)
            elif r.status_code == 503:
                time.sleep(2 ** attempt)
            else:
                log(f"Batch {op} error: {r.status_code} - {r.text[:200]}", level="ERROR")
                return 0
        except Exception as e:
            log(f"Batch {op} exception (attempt {attempt + 1}): {e}", level="WARNING")
            time.sleep(2 ** attempt)
    return 0


def sync_to_directus(records, existing_meta=None):
    """
    existing_meta: dict {id -> last_update_str} (full mode) or None (incremental).
    In full mode, records whose last_update matches Directus are skipped.
    """
    total_inserted = total_updated = total_skipped = 0
    log(f"Syncing {len(records)} records to Directus...")

    if existing_meta is None:
        # Incremental: check existence per batch
        total_batches = (len(records) + BATCH_SIZE - 1) // BATCH_SIZE
        for i in range(0, len(records), BATCH_SIZE):
            batch_num = i // BATCH_SIZE + 1
            batch = records[i:i + BATCH_SIZE]
            existing = get_existing_ids([r["id"] for r in batch])
            to_insert = [r for r in batch if r["id"] not in existing]
            to_update = [r for r in batch if r["id"] in existing]
            n_ins = batch_upsert(to_insert, "insert")
            n_upd = batch_upsert(to_update, "update")
            total_inserted += n_ins
            total_updated += n_upd
            log(
                f"Incremental batch {batch_num}/{total_batches}: "
                f"+{n_ins} inserted, ~{n_upd} updated "
                f"(running total: {total_inserted} inserted, {total_updated} updated)"
            )
    else:
        # Full: pre-partitioned by comparing last_update to skip unchanged records
        existing_ids = set(existing_meta.keys())
        to_insert = [r for r in records if r["id"] not in existing_ids]
        to_update = []
        for r in records:
            if r["id"] in existing_ids:
                if _normalize_ts(r.get("last_update")) != _normalize_ts(existing_meta[r["id"]]):
                    to_update.append(r)
                else:
                    total_skipped += 1
        log(
            f"Full-mode partition: {len(to_insert)} new, {len(to_update)} changed, "
            f"{total_skipped} unchanged (skipped)"
        )

        insert_batches = (len(to_insert) + BATCH_SIZE - 1) // BATCH_SIZE or 1
        for i in range(0, len(to_insert), BATCH_SIZE):
            batch_num = i // BATCH_SIZE + 1
            n = batch_upsert(to_insert[i:i + BATCH_SIZE], "insert")
            total_inserted += n
            log(f"Insert batch {batch_num}/{insert_batches}: {n} records (total inserted: {total_inserted}/{len(to_insert)})")

        update_batches = (len(to_update) + BATCH_SIZE - 1) // BATCH_SIZE or 1
        for i in range(0, len(to_update), BATCH_SIZE):
            batch_num = i // BATCH_SIZE + 1
            n = batch_upsert(to_update[i:i + BATCH_SIZE], "update")
            total_updated += n
            log(f"Update batch {batch_num}/{update_batches}: {n} records (total updated: {total_updated}/{len(to_update)})")

    return total_inserted, total_updated


def get_all_directus_meta():
    """Return {id: last_update_str} for every record in Directus."""
    meta = {}
    page_size, offset = 10000, 0
    log("Fetching all existing IDs + last_update from Directus...")
    while True:
        try:
            r = requests.get(
                f"{DIRECTUS_URL}/items/{TABLE_NAME}",
                headers=HEADERS,
                params={"fields": "id,last_update", "limit": page_size, "offset": offset},
                timeout=120,
            )
            if r.status_code == 200:
                data = r.json().get("data", [])
                if not data:
                    break
                for item in data:
                    meta[item["id"]] = item.get("last_update")
                log(f"  ...fetched {len(meta):,} records so far (offset {offset})")
                if len(data) < page_size:
                    break
                offset += page_size
            else:
                log(f"Error fetching meta: {r.status_code}", level="WARNING")
                break
        except Exception as e:
            log(f"Error fetching meta: {e}", level="WARNING")
            break
    log(f"Directus meta fetch complete: {len(meta):,} records")
    return meta


def delete_stale_records(stale_ids):
    deleted = 0
    stale_list = list(stale_ids)
    total_batches = (len(stale_list) + BATCH_SIZE - 1) // BATCH_SIZE or 1
    log(f"Deleting {len(stale_list)} stale records in {total_batches} batches...")
    for i in range(0, len(stale_list), BATCH_SIZE):
        batch_num = i // BATCH_SIZE + 1
        batch = stale_list[i:i + BATCH_SIZE]
        try:
            r = requests.delete(
                f"{DIRECTUS_URL}/items/{TABLE_NAME}", headers=HEADERS, json=batch, timeout=120
            )
            if r.status_code in [200, 204]:
                deleted += len(batch)
                log(f"Delete batch {batch_num}/{total_batches}: {len(batch)} records (total deleted: {deleted})")
            else:
                log(f"Delete batch {batch_num}/{total_batches} error: {r.status_code} - {r.text[:200]}", level="ERROR")
        except Exception as e:
            log(f"Delete batch {batch_num}/{total_batches} error: {e}", level="ERROR")
    log(f"Deleted {deleted} stale records.")
    return deleted


def clear_table():
    """Delete every record from the Directus table in batches."""
    log("Fetching all IDs for table clear...")
    all_ids = []
    page_size, offset = 10000, 0
    while True:
        try:
            r = requests.get(
                f"{DIRECTUS_URL}/items/{TABLE_NAME}",
                headers=HEADERS,
                params={"fields": "id", "limit": page_size, "offset": offset},
                timeout=120,
            )
            if r.status_code == 200:
                data = r.json().get("data", [])
                if not data:
                    break
                all_ids.extend(item["id"] for item in data)
                log(f"  ...fetched {len(all_ids):,} IDs so far")
                if len(data) < page_size:
                    break
                offset += page_size
            else:
                log(f"Error fetching IDs for clear: {r.status_code}", level="WARNING")
                break
        except Exception as e:
            log(f"Error fetching IDs for clear: {e}", level="WARNING")
            break

    if not all_ids:
        log("Table is already empty.")
        return 0

    log(f"Deleting {len(all_ids):,} records...")
    deleted = 0
    total_batches = (len(all_ids) + BATCH_SIZE - 1) // BATCH_SIZE
    for i in range(0, len(all_ids), BATCH_SIZE):
        batch_num = i // BATCH_SIZE + 1
        batch = all_ids[i:i + BATCH_SIZE]
        try:
            r = requests.delete(
                f"{DIRECTUS_URL}/items/{TABLE_NAME}", headers=HEADERS, json=batch, timeout=120
            )
            if r.status_code in [200, 204]:
                deleted += len(batch)
                log(f"Clear batch {batch_num}/{total_batches}: {len(batch)} deleted (total: {deleted:,})")
            else:
                log(f"Clear batch {batch_num}/{total_batches} error: {r.status_code} - {r.text[:200]}", level="ERROR")
        except Exception as e:
            log(f"Clear batch {batch_num}/{total_batches} error: {e}", level="ERROR")

    log(f"Table cleared: {deleted:,} records deleted.")
    return deleted


def cleanup():
    if SKIP_DOWNLOAD:
        return  # Parent script handles cleanup
    from pathlib import Path
    mastr_dir = Path.home() / ".open-MaStR" / "data" / "xml_download"
    if mastr_dir.exists():
        today = datetime.today().date()
        for zip_file in mastr_dir.glob("Gesamtdatenexport_*.zip"):
            file_date = datetime.fromtimestamp(zip_file.stat().st_mtime).date()
            if FULL_LOAD and file_date == today:
                log(f"Keeping {zip_file.name} (reusable today).")
                continue
            try:
                zip_file.unlink()
                log(f"Deleted {zip_file.name}.")
            except Exception as e:
                log(f"Could not delete {zip_file.name}: {e}", level="WARNING")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    start_time = time.time()
    mode = "rebuild" if REBUILD else ("full" if FULL_LOAD else "incremental")
    slack_log(f"Storage-Sync gestartet (Modus: {mode}).", level="INFO")

    try:
        df, _db = download_storage_data()

        if len(df) == 0:
            log("No entries to sync.")
            slack_log("Keine Speicher-Eintraege zum Synchronisieren.", level="INFO")
            return

        df = apply_plausibility_checks(df)
        records = map_to_directus_schema(df)
        log(f"Prepared {len(records)} records for Directus sync.")

        total_deleted = 0
        if REBUILD:
            # Clear the table entirely, then insert everything fresh
            total_deleted = clear_table()
            existing_meta = {}  # treat all records as new
        elif FULL_LOAD:
            existing_meta = get_all_directus_meta()
        else:
            existing_meta = None

        total_inserted, total_updated = sync_to_directus(records, existing_meta=existing_meta)

        if FULL_LOAD and not REBUILD:
            raw_ids = {r["id"] for r in records if r.get("id")}
            stale_ids = set(existing_meta.keys()) - raw_ids
            if stale_ids:
                total_deleted = delete_stale_records(stale_ids)
            else:
                log("No stale records to delete.")

        duration = round(time.time() - start_time)
        slack_log(
            f"Storage-Sync abgeschlossen in {duration}s\n"
            f"- Modus: {mode}\n"
            f"- Nach Plausibilitaet: {len(records)}\n"
            f"- Eingefuegt: {total_inserted}\n"
            f"- Aktualisiert: {total_updated}"
            + (f"\n- Geloescht: {total_deleted}" if FULL_LOAD else ""),
            level="SUCCESS",
        )
        log(
            f"Sync complete in {duration}s. Inserted: {total_inserted}, Updated: {total_updated}"
            + (f", Deleted: {total_deleted}" if FULL_LOAD else "")
        )

    except Exception as e:
        slack_log(f"Fehler beim Storage-Sync: {e}", level="ERROR")
        log(f"Error: {e}", level="ERROR")
        raise

    finally:
        cleanup()


if __name__ == "__main__":
    main()
