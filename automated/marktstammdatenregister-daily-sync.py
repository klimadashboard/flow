"""
Marktstammdatenregister Daily Sync using open-mastr library.

Usage:
    pip install open-mastr pandas
    python marktstammdatenregister-daily-sync-openmastr.py
"""

import os
import math
import time
import requests
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
from open_mastr import Mastr
from slack_logger import slack_log

# Load environment variables
load_dotenv()

# Configuration
DIRECTUS_URL = os.getenv("DIRECTUS_API_URL")
DIRECTUS_TOKEN = os.getenv("DIRECTUS_API_TOKEN")
TABLE_NAME = "energy_solar_units"
BATCH_SIZE = int(os.getenv("DIRECTUS_BATCH_SIZE", 1000))
UPDATE_DAYS_BACK = int(os.getenv("UPDATE_DAYS_BACK", 10))
HEADERS = {
    "Authorization": f"Bearer {DIRECTUS_TOKEN}",
    "Content-Type": "application/json"
}

# Threshold date for filtering recently updated entries
THRESHOLD_DATE = datetime.today() - timedelta(days=UPDATE_DAYS_BACK)


def log(msg, level="INFO"):
    """Log a message with timestamp."""
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {level}: {msg}")


def download_solar_data():
    """
    Download solar data from MaStR using open-mastr library.
    Returns a pandas DataFrame with only recently updated solar unit data.
    """
    log("Initializing open-mastr...")

    # Initialize Mastr - downloads to ~/.open-MaStR/data/sqlite
    db = Mastr()

    # Download fresh solar data from MaStR bulk export
    log("Downloading solar data from MaStR...")
    db.download(data=["solar"], bulk_cleansing=True)

    log("Reading recent solar data from database...")

    # Filter directly in SQL for much better performance
    # Try different possible column names for the last update date
    threshold_str = THRESHOLD_DATE.strftime('%Y-%m-%d')

    # First, check which date column exists in the table
    test_query = "SELECT * FROM solar_extended LIMIT 1"
    test_df = pd.read_sql(sql=test_query, con=db.engine)
    columns = test_df.columns.tolist()

    date_column = None
    for col in ['DatumLetzteAktualisierung', 'date_last_update', 'DateLastUpdate', 'LastUpdate']:
        if col in columns:
            date_column = col
            break

    if date_column is None:
        log(f"Available columns: {columns}", level="WARNING")
        log("Could not find last update column. Loading all entries (slow).", level="WARNING")
        df = pd.read_sql(sql="solar_extended", con=db.engine)
    else:
        log(f"Filtering by {date_column} >= {threshold_str}")
        query = f"SELECT * FROM solar_extended WHERE {date_column} >= '{threshold_str}'"
        df = pd.read_sql(sql=query, con=db.engine)

    log(f"Loaded {len(df)} solar units updated in the last {UPDATE_DAYS_BACK} days.")

    return df, db


def map_to_directus_schema(df):
    """
    Map open-mastr DataFrame columns to the Directus schema.
    Returns a list of dictionaries ready for Directus insertion.
    """
    # Value mappings: convert German text labels to numeric codes
    # Fill in the numeric codes for each label
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
        "Gewässer": "3002"
    }


    # Column mapping from open-mastr to Directus schema
    # open-mastr uses English column names after translation
    column_mapping = {
        'EinheitMastrNummer': 'id',
        'mastr_id': 'id',
        'unit_registration_id': 'id',

        'NameStromerzeugungseinheit': 'name',
        'name_unit': 'name',
        'unit_name': 'name',

        'EinheitBetriebsstatus': 'status',
        'operating_status': 'status',
        'unit_operating_status': 'status',

        'Inbetriebnahmedatum': 'commissioning_date',
        'commissioning_date': 'commissioning_date',
        'date_commissioning': 'commissioning_date',

        'DatumLetzteAktualisierung': 'last_update',
        'date_last_update': 'last_update',

        'DatumEndgueltigeStilllegung': 'shutdown_date',
        'date_decommissioning': 'shutdown_date',

        'Hauptausrichtung': 'orientation',
        'main_orientation': 'orientation',

        'ArtDerSolaranlage': 'type',
        'solar_type': 'type',
        'usage_area': 'type',

        'Bruttoleistung': 'power_kw',
        'gross_capacity': 'power_kw',
        'capacity_gross': 'power_kw',

        'AnzahlModule': 'module_count',
        'number_modules': 'module_count',
        'module_count': 'module_count',

        'SpeicherAmGleichenOrt': 'storage_installed',
        'storage_at_same_location': 'storage_installed',

        'Bundesland': 'federal_state',
        'federal_state': 'federal_state',
        'state': 'federal_state',

        'Landkreis': 'district',
        'district': 'district',

        'Gemeinde': 'municipality',
        'municipality': 'municipality',

        'Gemeindeschluessel': 'region',
        'municipality_key': 'region',

        'Postleitzahl': 'postcode',
        'postcode': 'postcode',

        'NetzbetreiberpruefungStatus': 'grid_operator_review_status',
        'grid_operator_review_status': 'grid_operator_review_status',
    }

    # Find actual columns in the DataFrame
    actual_mapping = {}
    for source_col, target_col in column_mapping.items():
        if source_col in df.columns:
            if target_col not in actual_mapping.values():  # Avoid duplicates
                actual_mapping[source_col] = target_col

    log(f"Mapped columns: {actual_mapping}")

    records = []
    for _, row in df.iterrows():
        # Get the MaStR ID - this is required
        mastr_id = None
        for col in ['EinheitMastrNummer', 'mastr_id', 'unit_registration_id']:
            if col in df.columns and pd.notna(row.get(col)):
                mastr_id = str(row[col]).strip()
                break

        if not mastr_id:
            continue

        # Build the record
        record = {
            'id': mastr_id,
            'country': 'DE'
        }

        # Map all other fields
        for source_col, target_col in actual_mapping.items():
            if target_col == 'id':  # Already handled
                continue

            value = row.get(source_col)

            if pd.isna(value):
                record[target_col] = None
                continue

            # Handle specific field types
            if target_col == 'power_kw':
                try:
                    record[target_col] = float(str(value).replace(',', '.'))
                except (ValueError, TypeError):
                    record[target_col] = None

            elif target_col == 'module_count':
                try:
                    record[target_col] = math.floor(float(str(value).replace(',', '.')))
                except (ValueError, TypeError):
                    record[target_col] = None

            elif target_col == 'storage_installed':
                # Handle boolean/string conversion
                if isinstance(value, bool):
                    record[target_col] = value
                elif str(value) in ['1', 'true', 'True', 'ja', 'Ja']:
                    record[target_col] = True
                else:
                    record[target_col] = False

            elif target_col in ['commissioning_date', 'last_update', 'shutdown_date']:
                # Convert datetime to ISO string
                if isinstance(value, pd.Timestamp):
                    record[target_col] = value.strftime('%Y-%m-%d')
                else:
                    record[target_col] = str(value)[:10] if value else None

            elif target_col == 'status':
                # Convert status label to numeric code
                record[target_col] = STATUS_CODES.get(str(value), str(value)) if value else None

            elif target_col == 'type':
                # Convert type label to numeric code
                record[target_col] = TYPE_CODES.get(str(value), str(value)) if value else None

            elif target_col in ['orientation', 'federal_state']:
                # Keep as text from open-mastr
                record[target_col] = str(value) if value else None

            else:
                record[target_col] = str(value) if value else None

        records.append(record)

    return records


def get_existing_ids(ids):
    """
    Query Directus to find which IDs already exist in the database.
    Returns a set of existing IDs.
    """
    existing = set()

    # Query in chunks to avoid URL length limits
    chunk_size = 100  # Smaller chunks to avoid URL length issues
    for i in range(0, len(ids), chunk_size):
        chunk = ids[i:i + chunk_size]
        try:
            response = requests.get(
                f"{DIRECTUS_URL}/items/{TABLE_NAME}",
                headers=HEADERS,
                params={
                    "filter[id][_in]": ",".join(chunk),
                    "fields": "id",
                    "limit": -1  # No limit, get all matches
                },
                timeout=60
            )
            if response.status_code == 200:
                data = response.json().get("data", [])
                existing.update(item["id"] for item in data)
            else:
                log(f"Error querying existing IDs: {response.status_code} - {response.text[:200]}", level="WARNING")
        except Exception as e:
            log(f"Error checking existing IDs: {e}", level="WARNING")

    log(f"Found {len(existing)} existing IDs out of {len(ids)} checked.")
    return existing


def batch_insert(batch, max_retries=3, retry_delay=2):
    """
    Insert multiple items in a single POST request.
    """
    if not batch:
        return 0

    for attempt in range(max_retries):
        try:
            response = requests.post(
                f"{DIRECTUS_URL}/items/{TABLE_NAME}",
                json=batch,
                headers=HEADERS,
                timeout=120
            )

            if response.status_code in [200, 201]:
                return len(batch)
            elif response.status_code == 503:
                log(f"Server unavailable, retry {attempt + 1}/{max_retries}...", level="WARNING")
                time.sleep(retry_delay * (2 ** attempt))
            else:
                log(f"Batch insert error: {response.status_code} - {response.text[:500]}", level="ERROR")
                return 0
        except Exception as e:
            log(f"Exception on batch insert attempt {attempt + 1}: {e}", level="WARNING")
            time.sleep(retry_delay * (2 ** attempt))

    return 0


def batch_update(batch, max_retries=3, retry_delay=2):
    """
    Update multiple items in a single PATCH request.
    Directus supports PATCH /items/{collection} with an array of objects.
    """
    if not batch:
        return 0

    for attempt in range(max_retries):
        try:
            response = requests.patch(
                f"{DIRECTUS_URL}/items/{TABLE_NAME}",
                json=batch,
                headers=HEADERS,
                timeout=120
            )

            if response.status_code in [200, 204]:
                return len(batch)
            elif response.status_code == 503:
                log(f"Server unavailable, retry {attempt + 1}/{max_retries}...", level="WARNING")
                time.sleep(retry_delay * (2 ** attempt))
            else:
                log(f"Batch update error: {response.status_code} - {response.text[:500]}", level="ERROR")
                return 0
        except Exception as e:
            log(f"Exception on batch update attempt {attempt + 1}: {e}", level="WARNING")
            time.sleep(retry_delay * (2 ** attempt))

    return 0


def process_batch(batch, max_retries=3, retry_delay=2):
    """
    Process a batch of records: check which exist, then insert new and update existing.
    """
    if not batch:
        return 0, 0

    # Get all IDs in this batch
    batch_ids = [item["id"] for item in batch]

    # Check which IDs already exist in Directus
    existing_ids = get_existing_ids(batch_ids)

    # Split batch into new items and existing items
    to_insert = [item for item in batch if item["id"] not in existing_ids]
    to_update = [item for item in batch if item["id"] in existing_ids]

    inserted = 0
    updated = 0

    # Insert new items
    if to_insert:
        inserted = batch_insert(to_insert, max_retries, retry_delay)
        if inserted > 0:
            log(f"Inserted {inserted} new entries.")

    # Update existing items
    if to_update:
        updated = batch_update(to_update, max_retries, retry_delay)
        if updated > 0:
            log(f"Updated {updated} existing entries.")

    return inserted, updated

def sync_to_directus(records):
    """
    Sync records to Directus in batches.
    """
    total_inserted = 0
    total_updated = 0

    log(f"Starting sync of {len(records)} records to Directus...")

    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i:i + BATCH_SIZE]
        batch_num = (i // BATCH_SIZE) + 1
        total_batches = math.ceil(len(records) / BATCH_SIZE)

        log(f"Processing batch {batch_num}/{total_batches} ({len(batch)} records)...")

        inserted, updated = process_batch(batch)
        total_inserted += inserted
        total_updated += updated

    return total_inserted, total_updated


def cleanup(db=None):
    """
    Optional cleanup of open-mastr database.
    By default, open-mastr keeps the database for faster subsequent runs.
    """
    # The open-mastr library manages its own cleanup
    # We can optionally delete the database to save disk space
    # But keeping it allows for faster incremental updates
    log("Cleanup complete (database retained for future runs).")


def main():
    """Main entry point."""
    start_time = time.time()

    slack_log("Sync des Marktstammdatenregisters gestartet (open-mastr).", level="INFO")

    try:
        # Step 1: Download solar data and filter to recent updates (done in SQL)
        df, db = download_solar_data()

        if len(df) == 0:
            log("No entries to update.")
            slack_log("Keine neuen Eintraege im Marktstammdatenregister.", level="INFO")
            return

        # Step 2: Map to Directus schema
        records = map_to_directus_schema(df)
        log(f"Prepared {len(records)} records for Directus sync.")

        # Step 3: Sync to Directus
        total_inserted, total_updated = sync_to_directus(records)

        duration = round(time.time() - start_time)

        slack_log(
            f"Sync abgeschlossen in {duration}s\n"
            f"- Gefiltert: {len(df)}\n"
            f"- Eingefuegt: {total_inserted}\n"
            f"- Aktualisiert: {total_updated}",
            level="SUCCESS"
        )

        log(f"Sync completed in {duration}s. Inserted: {total_inserted}, Updated: {total_updated}")

    except Exception as e:
        slack_log(f"Fehler beim Marktstammdatenregister Sync: {e}", level="ERROR")
        log(f"Error: {e}", level="ERROR")
        raise

    finally:
        cleanup()


if __name__ == "__main__":
    main()
