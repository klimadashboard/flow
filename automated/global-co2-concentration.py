"""
Updates the Directus collection `global_co2_concentration` from two sources:

  1. Law Dome ice core CO2 (Etheridge et al. 1996), 1006–1978 A.D.
     https://www.ncei.noaa.gov/pub/data/paleo/icecore/antarctica/law/law_co2.txt

  2. NOAA Global Monitoring Laboratory global annual mean CO2, 1979–present.
     https://gml.noaa.gov/ccgg/trends/global.html
     CSV: https://gml.noaa.gov/webdata/ccgg/trends/co2/co2_annmean_gl.csv

Both sources are open data and re-published by NOAA. Modern years may be
revised, so we wipe the table and reinsert on every run.
"""

import os
import csv
import io
import logging
from collections import defaultdict
from time import time

import requests
from dotenv import load_dotenv
from slack_logger import slack_log

load_dotenv()

DIRECTUS_API_URL = os.getenv("DIRECTUS_API_URL")
DIRECTUS_API_TOKEN = os.getenv("DIRECTUS_API_TOKEN")
HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {DIRECTUS_API_TOKEN}",
}

TABLE_NAME = "global_co2_concentration"

NOAA_URL = "https://gml.noaa.gov/webdata/ccgg/trends/co2/co2_annmean_gl.csv"
NOAA_SOURCE = (
    "NOAA Global Monitoring Laboratory – Global Marine Surface Annual Mean CO2 "
    f"({NOAA_URL})"
)

LAW_DOME_URL = (
    "https://www.ncei.noaa.gov/pub/data/paleo/icecore/antarctica/law/law_co2.txt"
)
LAW_DOME_SOURCE = (
    "Etheridge et al. 1996, Law Dome DE08/DE08-2/DSS ice cores, "
    f"NOAA Paleoclimatology ({LAW_DOME_URL})"
)

# Where the Law Dome record ends and we switch to the NOAA modern series.
MODERN_START_YEAR = 1979

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def fetch_noaa_annual_means():
    """Download and parse NOAA's global annual mean CO2 CSV."""
    logging.info(f"Fetching {NOAA_URL}")
    resp = requests.get(NOAA_URL, timeout=30)
    resp.raise_for_status()

    records = []
    reader = csv.reader(io.StringIO(resp.text))
    for row in reader:
        if not row or row[0].lstrip().startswith("#"):
            continue
        if not row[0].strip().lstrip("-").isdigit():
            continue  # header row "year,mean,unc"
        year = int(row[0].strip())
        if year < MODERN_START_YEAR:
            continue
        records.append({
            "year": year,
            "mean": float(row[1].strip()),
            "source": NOAA_SOURCE,
        })
    logging.info(f"NOAA: {len(records)} records ({records[0]['year']}–{records[-1]['year']}).")
    return records


def fetch_law_dome_annual_means():
    """Download and parse the Law Dome ice-core file.

    The file contains three whitespace-delimited tables (DE08, DE08-2, DSS),
    each with columns: <core> <sample_id> <analysis_date> <depth_m>
    <ice_age> <mean_air_age> <co2_ppm>.

    Many years have multiple measurements (same air-age year across the three
    cores or repeat analyses); we average ppm by integer air-age year.
    """
    logging.info(f"Fetching {LAW_DOME_URL}")
    logging.info("Opening connection...")
    resp = requests.get(LAW_DOME_URL, timeout=30, headers={"User-Agent": "Mozilla/5.0"}, stream=True)
    logging.info(f"Connected. Status: {resp.status_code}, headers: {dict(resp.headers)}")
    content = resp.content
    logging.info(f"Downloaded {len(content)} bytes")
    resp.raise_for_status()
    resp.encoding = "latin-1"  # file contains non-UTF-8 characters in the readme header

    by_year = defaultdict(list)
    for raw in resp.text.splitlines():
        tokens = raw.split()
        if len(tokens) != 7:
            continue
        # Data rows always start with a known core name.
        if tokens[0] not in ("DE08", "DE08-2", "DSS"):
            continue
        try:
            air_age = float(tokens[5])
            ppm = float(tokens[6])
        except ValueError:
            continue
        year = int(round(air_age))
        if year >= MODERN_START_YEAR:
            continue  # let NOAA cover modern years
        by_year[year].append(ppm)

    records = [
        {
            "year": year,
            "mean": round(sum(values) / len(values), 2),
            "source": LAW_DOME_SOURCE,
        }
        for year, values in sorted(by_year.items())
    ]
    logging.info(f"Law Dome: {len(records)} records ({records[0]['year']}–{records[-1]['year']}).")
    return records


def fetch_existing_ids():
    """Fetch all existing record IDs from Directus via pagination."""
    ids = []
    page = 1
    page_size = 1000
    while True:
        url = f"{DIRECTUS_API_URL}/items/{TABLE_NAME}?fields=id&limit={page_size}&page={page}"
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        data = resp.json().get("data", [])
        if not data:
            break
        ids.extend(item["id"] for item in data)
        if len(data) < page_size:
            break
        page += 1
    return ids


def clear_table():
    """Delete all existing rows in the collection."""
    ids = fetch_existing_ids()
    if not ids:
        logging.info("Table already empty.")
        return
    logging.info(f"Deleting {len(ids)} existing rows…")
    BATCH = 100
    for i in range(0, len(ids), BATCH):
        batch = ids[i:i + BATCH]
        url = f"{DIRECTUS_API_URL}/items/{TABLE_NAME}"
        resp = requests.delete(url, headers=HEADERS, json=batch, timeout=30)
        if resp.status_code not in (200, 204):
            raise RuntimeError(f"Delete failed: {resp.status_code} - {resp.text}")
    logging.info("Table cleared.")


def insert_records(records):
    """Insert records into Directus in batches."""
    BATCH = 500
    for i in range(0, len(records), BATCH):
        batch = records[i:i + BATCH]
        url = f"{DIRECTUS_API_URL}/items/{TABLE_NAME}"
        resp = requests.post(url, headers=HEADERS, json=batch, timeout=30)
        if resp.status_code not in (200, 204):
            raise RuntimeError(f"Insert failed: {resp.status_code} - {resp.text}")
        logging.info(f"Inserted batch {i // BATCH + 1} ({len(batch)} rows).")


if __name__ == "__main__":
    start = time()
    slack_log("🔄 Start der Aktualisierung von global_co2_concentration", level="INFO")
    try:
        records = fetch_law_dome_annual_means() + fetch_noaa_annual_means()
        clear_table()
        insert_records(records)
        duration = round(time() - start)
        latest = max(records, key=lambda r: r["year"])
        slack_log(
            f"✅ global_co2_concentration aktualisiert in {duration}s\n"
            f"Einträge: {len(records)} ({records[0]['year']}–{records[-1]['year']})\n"
            f"Aktuellster Wert: {latest['year']} = {latest['mean']} ppm",
            level="SUCCESS",
        )
        logging.info("Done.")
    except Exception as e:
        slack_log(f"❌ Fehler bei global_co2_concentration: {e}", level="ERROR")
        logging.exception("Update failed")
        raise
