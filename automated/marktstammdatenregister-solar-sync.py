"""
Marktstammdatenregister Solar Sync.

Syncs solar unit data from MaStR to Directus.
Invoked by marktstammdatenregister-daily-sync.py (with --skip-download),
or run standalone.

Usage:
    python marktstammdatenregister-solar-sync.py                  # incremental (last N days)
    python marktstammdatenregister-solar-sync.py --full           # full reload, skip unchanged
    python marktstammdatenregister-solar-sync.py --rebuild        # clear table, re-insert everything
    python marktstammdatenregister-solar-sync.py --skip-download  # skip MaStR download (parent already did it)
"""

import os
import math
import sys
import time
import requests
import pandas as pd
from datetime import datetime, timedelta
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
TABLE_NAME = "energy_solar_units"
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


def log(msg, level="INFO"):
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} [solar-sync] {level}: {msg}")


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def download_solar_data():
    log("Initializing open-mastr...")
    db = Mastr()

    if not SKIP_DOWNLOAD:
        log("Downloading solar data from MaStR...")
        db.download(data=["solar"], bulk_cleansing=True)
    else:
        log("Skipping download (data pre-loaded by parent).")

    log("Reading solar data from database...")

    threshold_str = THRESHOLD_DATE.strftime("%Y-%m-%d")
    test_df = pd.read_sql(sql="SELECT * FROM solar_extended LIMIT 1", con=db.engine)
    columns = test_df.columns.tolist()

    date_column = next(
        (c for c in ["DatumLetzteAktualisierung", "date_last_update", "DateLastUpdate", "LastUpdate"]
         if c in columns),
        None,
    )

    if FULL_LOAD:
        log("Full mode: loading all solar units (no date filter).")
        df = pd.read_sql(sql="SELECT * FROM solar_extended", con=db.engine)
    elif date_column is None:
        log(f"Available columns: {columns}", level="WARNING")
        log("Could not find last update column. Loading all entries.", level="WARNING")
        df = pd.read_sql(sql="SELECT * FROM solar_extended", con=db.engine)
    else:
        log(f"Filtering by {date_column} >= {threshold_str}")
        query = f"SELECT * FROM solar_extended WHERE {date_column} >= '{threshold_str}'"
        df = pd.read_sql(sql=query, con=db.engine)

    log(f"Loaded {len(df)} solar units{'' if FULL_LOAD else f' updated in the last {UPDATE_DAYS_BACK} days'}.")
    return df, db


# ---------------------------------------------------------------------------
# Map to Directus schema
# ---------------------------------------------------------------------------

def map_to_directus_schema(df):
    STATUS_CODES = {
        "In Betrieb": "35",
        "Vorübergehend stillgelegt": "37",
        "Endgültig stillgelegt": "38",
        "In Planung": "31",
    }
    TYPE_CODES = {
        "Gebäudesolaranlage": "853",
        "Freiflächensolaranlage": "852",
        "Sonstige Solaranlage": "2484",
        "Steckerfertige Solaranlage (sog. Balkonkraftwerk)": "2961",
        "Großparkplatz": "3058",
        "Gewässer": "3002",
    }
    column_mapping = {
        "EinheitMastrNummer": "id",
        "mastr_id": "id",
        "unit_registration_id": "id",
        "NameStromerzeugungseinheit": "name",
        "name_unit": "name",
        "unit_name": "name",
        "EinheitBetriebsstatus": "status",
        "operating_status": "status",
        "unit_operating_status": "status",
        "Inbetriebnahmedatum": "commissioning_date",
        "commissioning_date": "commissioning_date",
        "date_commissioning": "commissioning_date",
        "DatumLetzteAktualisierung": "last_update",
        "date_last_update": "last_update",
        "DatumEndgueltigeStilllegung": "shutdown_date",
        "date_decommissioning": "shutdown_date",
        "Hauptausrichtung": "orientation",
        "main_orientation": "orientation",
        "ArtDerSolaranlage": "type",
        "solar_type": "type",
        "usage_area": "type",
        "Bruttoleistung": "power_kw",
        "gross_capacity": "power_kw",
        "capacity_gross": "power_kw",
        "Nettonennleistung": "net_power_kw",
        "net_capacity": "net_power_kw",
        "AnzahlModule": "module_count",
        "number_modules": "module_count",
        "module_count": "module_count",
        "SpeicherAmGleichenOrt": "storage_installed",
        "storage_at_same_location": "storage_installed",
        "Bundesland": "federal_state",
        "federal_state": "federal_state",
        "state": "federal_state",
        "Landkreis": "district",
        "district": "district",
        "Gemeinde": "municipality",
        "municipality": "municipality",
        "Gemeindeschluessel": "region",
        "municipality_key": "region",
        "Postleitzahl": "postcode",
        "postcode": "postcode",
        "NetzbetreiberpruefungStatus": "grid_operator_review_status",
        "grid_operator_review_status": "grid_operator_review_status",
    }

    actual_mapping = {}
    for source_col, target_col in column_mapping.items():
        if source_col in df.columns and target_col not in actual_mapping.values():
            actual_mapping[source_col] = target_col

    records = []
    for _, row in df.iterrows():
        mastr_id = None
        for col in ["EinheitMastrNummer", "mastr_id", "unit_registration_id"]:
            if col in df.columns and pd.notna(row.get(col)):
                mastr_id = str(row[col]).strip()
                break
        if not mastr_id:
            continue

        record = {"id": mastr_id, "country": "DE"}

        for source_col, target_col in actual_mapping.items():
            if target_col == "id":
                continue
            value = row.get(source_col)
            if pd.isna(value):
                record[target_col] = None
                continue

            if target_col in ("power_kw", "net_power_kw"):
                try:
                    record[target_col] = float(str(value).replace(",", "."))
                except (ValueError, TypeError):
                    record[target_col] = None
            elif target_col == "module_count":
                try:
                    record[target_col] = math.floor(float(str(value).replace(",", ".")))
                except (ValueError, TypeError):
                    record[target_col] = None
            elif target_col == "storage_installed":
                if isinstance(value, bool):
                    record[target_col] = value
                elif str(value) in ["1", "true", "True", "ja", "Ja"]:
                    record[target_col] = True
                else:
                    record[target_col] = False
            elif target_col in ("commissioning_date", "last_update", "shutdown_date"):
                if isinstance(value, pd.Timestamp):
                    record[target_col] = value.strftime("%Y-%m-%d")
                else:
                    record[target_col] = str(value)[:10] if value else None
            elif target_col == "status":
                record[target_col] = STATUS_CODES.get(str(value), str(value)) if value else None
            elif target_col == "type":
                record[target_col] = TYPE_CODES.get(str(value), str(value)) if value else None
            else:
                record[target_col] = str(value) if value else None

        records.append(record)

    log(f"Mapped {len(records)} records.")
    return records


# ---------------------------------------------------------------------------
# Directus API helpers
# ---------------------------------------------------------------------------

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
                log(f"Error querying existing IDs: {r.status_code}", level="WARNING")
        except Exception as e:
            log(f"Error checking existing IDs: {e}", level="WARNING")
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

    deleted = 0
    total_batches = (len(all_ids) + BATCH_SIZE - 1) // BATCH_SIZE
    log(f"Deleting {len(all_ids):,} records in {total_batches} batches...")
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


def sync_to_directus(records):
    total_inserted = total_updated = 0
    total_batches = (len(records) + BATCH_SIZE - 1) // BATCH_SIZE
    log(f"Syncing {len(records)} records to Directus in {total_batches} batches...")

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
            f"Batch {batch_num}/{total_batches}: +{n_ins} inserted, ~{n_upd} updated "
            f"(running total: {total_inserted} inserted, {total_updated} updated)"
        )

    return total_inserted, total_updated


def cleanup():
    if SKIP_DOWNLOAD:
        return  # Parent script handles cleanup
    from pathlib import Path
    mastr_dir = Path.home() / ".open-MaStR" / "data" / "xml_download"
    if mastr_dir.exists():
        for zip_file in mastr_dir.glob("Gesamtdatenexport_*.zip"):
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
    slack_log(f"Solar-Sync gestartet (Modus: {mode}).", level="INFO")

    try:
        df, _db = download_solar_data()

        if len(df) == 0:
            log("No entries to sync.")
            slack_log("Keine Solar-Eintraege zum Synchronisieren.", level="INFO")
            return

        records = map_to_directus_schema(df)
        log(f"Prepared {len(records)} records for Directus sync.")

        total_deleted = 0
        if REBUILD:
            total_deleted = clear_table()

        total_inserted, total_updated = sync_to_directus(records)

        duration = round(time.time() - start_time)
        slack_log(
            f"Solar-Sync abgeschlossen in {duration}s\n"
            f"- Modus: {mode}\n"
            f"- Geladen: {len(df)}\n"
            f"- Eingefuegt: {total_inserted}\n"
            f"- Aktualisiert: {total_updated}"
            + (f"\n- Geloescht: {total_deleted}" if REBUILD else ""),
            level="SUCCESS",
        )
        log(
            f"Solar sync complete in {duration}s. Inserted: {total_inserted}, Updated: {total_updated}"
            + (f", Deleted: {total_deleted}" if REBUILD else "")
        )

    except Exception as e:
        slack_log(f"Fehler beim Solar-Sync: {e}", level="ERROR")
        log(f"Error: {e}", level="ERROR")
        raise

    finally:
        cleanup()


if __name__ == "__main__":
    main()
