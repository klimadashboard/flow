import requests
import logging
from datetime import datetime
import statistics
from dotenv import load_dotenv
import os
from slack_logger import slack_log
from time import time

# Load from the nearest .env file (searches parent directories too)
load_dotenv()

HEADERS = {
    'Content-Type': 'application/json',
    'Authorization': f'Bearer {os.getenv("DIRECTUS_API_TOKEN")}',
}

# List of country codes (region) to fetch data for
COUNTRIES = ['at', 'de']  # Lowercase country codes as used by the API

DAILY_API_URL = "https://api.energy-charts.info/ren_share_daily_avg?country={country}&year={year}"
FIFTEEN_MIN_API_URL = "https://api.energy-charts.info/signal?country={country}"

TABLE_NAME = 'energy_renewable_share'

# =====================================
# Logging Setup
# =====================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# =====================================
# Normalization Helper
# =====================================
def normalize_period(period_str, category):
    """
    Normalize the period string for comparison.
    
    For 15min data, convert to "YYYY-MM-DD HH:MM:SS" (removing any T, Z, or microseconds).
    For daily (or weekly/monthly/yearly) data, use the date part "YYYY-MM-DD".
    """
    if not period_str:
        return ""
    
    if category == "15min":
        # If the period string is in ISO format, e.g. "2025-02-02T13:48:23.000Z", replace T and remove Z.
        if "T" in period_str:
            period_str = period_str.replace("T", " ").replace("Z", "")
        # Try parsing with microseconds, then without.
        try:
            dt = datetime.strptime(period_str, "%Y-%m-%d %H:%M:%S.%f")
        except ValueError:
            try:
                dt = datetime.strptime(period_str, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                dt = None
        if dt:
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        else:
            return period_str  # fallback
    else:
        # For other categories, we want just the date (first 10 characters)
        if "T" in period_str:
            return period_str.split("T")[0]
        else:
            return period_str[:10]

# =====================================
# Helper Functions
# =====================================
def convert_date_format(date_str):
    # Expects input format like "31.12.2020" and returns "YYYY-MM-DD"
    return datetime.strptime(date_str, "%d.%m.%Y").strftime("%Y-%m-%d")

def convert_unix_to_datetime_str(unix_ts):
    # Returns datetime string as "YYYY-MM-DD HH:MM:SS"
    return datetime.utcfromtimestamp(unix_ts).strftime('%Y-%m-%d %H:%M:%S')

def uppercase_country(country_code):
    return country_code.upper() if country_code else None

def fetch_all_years_daily_data(country):
    records = []
    current_year = datetime.utcnow().year
    while current_year >= 2024:
        url = DAILY_API_URL.format(country=country, year=current_year)
        logging.info(f"Fetching daily data for {country} in {current_year}...")
        try:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            days = data.get('days', [])
            values = data.get('data', [])
            if not days:
                logging.info(f"No data for {country} in {current_year}. Stopping.")
                break  # No more data available for this year
            
            records.extend([
                {
                    "period": convert_date_format(d),  # e.g. "2025-02-02"
                    "value": s,
                    "country": uppercase_country(country),
                    "category": "day",
                    "year": convert_date_format(d)[:4]
                }
                for d, s in zip(days, values)
            ])
            current_year -= 1  # Fetch the previous year
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching daily data for {country} in {current_year}: {e}")
            break
    
    return records

def calculate_rolling_averages(data, window_size, category):
    """Calculate rolling averages over a given window size."""
    rolling_records = []
    sorted_data = sorted(data, key=lambda x: x['period'])
    for i in range(len(sorted_data) - window_size + 1):
        window = sorted_data[i:i + window_size]
        avg_value = statistics.mean([entry['value'] for entry in window])
        rolling_records.append({
            "period": sorted_data[i + window_size - 1]['period'],  # For day/week/month/year, remains "YYYY-MM-DD"
            "value": avg_value,
            "category": category,
            "year": sorted_data[i + window_size - 1]['year'],
            "country": sorted_data[i + window_size - 1]['country']
        })
    return rolling_records

def fetch_fifteen_min_data(country):
    """Fetch 15-min renewable share data with timeout handling."""
    url = FIFTEEN_MIN_API_URL.format(country=country)
    logging.info(f"Fetching 15-min data for {country}...")
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching 15-min data for {country}: {e}")
        return []
    
    data = resp.json()
    times = data.get('unix_seconds', [])
    values = data.get('share', [])
    uc_country = uppercase_country(country)
    
    records = [
        {
            # New records use our conversion function that yields "YYYY-MM-DD HH:MM:SS"
            "period": convert_unix_to_datetime_str(t),
            "value": s,
            "country": uc_country,
            "category": "15min",
            "year": convert_unix_to_datetime_str(t)[:4]
        }
        for t, s in zip(times, values)
    ]
    
    logging.info(f"Fetched {len(records)} 15-min records for {country}.")
    return records

def fetch_existing_keys():
    """
    Fetch all existing records' unique keys (normalized period, country, category)
    from Directus using pagination.
    """
    keys = set()
    page = 1
    page_size = 1000
    while True:
        url = f"{os.getenv('DIRECTUS_API_URL')}/items/{TABLE_NAME}?fields=period,country,category&limit={page_size}&page={page}"
        try:
            response = requests.get(url, headers=HEADERS)
            response.raise_for_status()
            data = response.json().get('data', [])
            if not data:
                break
            for item in data:
                # Normalize the period and ensure country is uppercase.
                period_val = normalize_period(item.get('period', ""), item.get('category', ""))
                country_val = uppercase_country(item.get('country'))
                key = (period_val, country_val, item.get('category'))
                keys.add(key)
            page += 1
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching existing records on page {page}: {e}")
            break
    return keys

def upsert_records(records):
    """
    Insert new records in Directus in batches, skipping duplicates.
    
    Deduplication is based on the normalized key: (normalized period, uppercase country, category).
    """
    if not records:
        logging.info("No records to upsert.")
        return

    logging.info("Fetching existing records from Directus to avoid duplicates...")
    existing_keys = fetch_existing_keys()
    logging.info(f"Fetched {len(existing_keys)} existing keys.")

    new_keys = set()
    new_records = []
    for r in records:
        norm_period = normalize_period(r.get("period", ""), r.get("category", ""))
        key = (norm_period, uppercase_country(r.get("country")), r.get("category"))
        if key in existing_keys or key in new_keys:
            logging.debug(f"Duplicate record found, skipping: {key}")
            continue
        new_keys.add(key)
        new_records.append(r)

    logging.info(f"{len(new_records)} new records after filtering duplicates out of {len(records)} total records.")

    # Batch new_records into groups of 1000 (or less)
    BATCH_SIZE = 1000
    total_batches = (len(new_records) + BATCH_SIZE - 1) // BATCH_SIZE
    for i in range(total_batches):
        batch = new_records[i * BATCH_SIZE:(i + 1) * BATCH_SIZE]
        logging.info(f"Inserting batch {i + 1}/{total_batches} with {len(batch)} records.")
        url = f"{os.getenv('DIRECTUS_API_URL')}/items/{TABLE_NAME}"
        try:
            response = requests.post(url, json=batch, headers=HEADERS)
            response.raise_for_status()
            logging.info(f"Batch {i + 1} inserted successfully: {response.status_code} - {response.text}")
        except requests.exceptions.HTTPError as e:
            logging.error(f"HTTP error on batch {i + 1}: {response.status_code} - {response.text}")
        except requests.exceptions.RequestException as e:
            logging.error(f"Error inserting batch {i + 1}: {e}")

# =====================================
# Main Script
# =====================================
if __name__ == "__main__":
    start_time = time()
    slack_log("🔄 Start der Aktualisierung des renewable_share", level="INFO")
    logging.info("Starting data update process...")

    try:
        all_records = []

        for country in COUNTRIES:
            daily_records = fetch_all_years_daily_data(country)
            weekly_records = calculate_rolling_averages(daily_records, 7, "week")
            monthly_records = calculate_rolling_averages(daily_records, 30, "month")
            yearly_records = calculate_rolling_averages(daily_records, 365, "year")
            fifteen_min_records = fetch_fifteen_min_data(country)

            all_records.extend(daily_records)
            all_records.extend(weekly_records)
            all_records.extend(monthly_records)
            all_records.extend(yearly_records)
            all_records.extend(fifteen_min_records)

        total = len(all_records)
        logging.info(f"Total records collected: {total}")
        upsert_records(all_records)

        duration = round(time() - start_time)
        slack_log(f"✅ Datenaktualisierung renewable_share abgeschlossen in {duration}s\nGesamtanzahl Einträge: {total}", level="SUCCESS")
        logging.info("Data update complete.")
    except Exception as e:
        slack_log(f"❌ Fehler bei renewable_share: {e}", level="ERROR")
        logging.error(f"Unexpected error: {e}")

