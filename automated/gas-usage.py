import requests
import csv
import datetime
from datetime import date, timedelta
from dotenv import load_dotenv
import os
from slack_logger import slack_log

# Load from the nearest .env file (searches parent directories too)
load_dotenv()

# Directus API configuration
api_url = os.getenv("DIRECTUS_API_URL")  # Replace with your Directus instance URL
api_token = os.getenv("DIRECTUS_API_TOKEN")  # Replace with your Directus API token
headers = {
    "Authorization": f"Bearer {api_token}",
    "Content-Type": "application/json"
}

# Endpoint for creating items in your "energy" table
energy_endpoint = f"{api_url}/items/energy"

# CSV file URLs
csv_url_aggm = "https://energie.wifo.ac.at/data/gas/consumption-aggm.csv"
csv_url_bna = "https://www.bundesnetzagentur.de/_tools/SVG/js2/_functions/csv_export.html?view=renderCSV&id=870330"

def fetch_csv(url):
    """Fetch the CSV file from the given URL."""
    print(f"Fetching CSV from: {url}")
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Failed to fetch CSV. Status: {response.status_code}, Response: {response.text}")
        return None
    return response.text


def fetch_existing_data():
    """Fetch all existing data from Directus."""
    query = f"{energy_endpoint}?limit=-1"
    print("Fetching all existing data from Directus...")
    try:
        response = requests.get(query, headers=headers)
        if response.status_code == 200:
            data = response.json()
            return { (row['region'], row['period']): row for row in data.get('data', []) }
        print(f"Failed to fetch existing data. Status: {response.status_code}, Response: {response.text}")
        return {}
    except Exception as e:
        print(f"Error fetching existing data: {e}")
        return {}

def week_to_date(year, week):
    """Convert a week number to the last date of that week (Sunday)."""
    first_day = date(year, 1, 1)
    start_of_week = first_day + timedelta(days=(week - 1) * 7)
    end_of_week = start_of_week + timedelta(days=6)
    return end_of_week.isoformat()

def upsert_data(existing_data, region, period, source, category, unit, value):
    """Insert a new row or update an existing row only if the rounded value is different."""
    today_date = datetime.date.today().isoformat()
    rounded_value = round(value, 3)
    existing_row = existing_data.get((region, period))
    
    if existing_row:
        existing_value = round(float(existing_row.get("value", 0)), 3)
        if existing_value != rounded_value:
            payload = {
                "region": region,
                "period": period,
                "source": source,
                "category": category,
                "unit": unit,
                "value": rounded_value,
                "update": today_date,
                "note": ""
            }
            print(f"Updating row: {payload}")
            try:
                update_endpoint = f"{energy_endpoint}/{existing_row['id']}"
                response = requests.patch(update_endpoint, headers=headers, json=payload)
                print(f"Update response: {response.status_code}, {response.text}")
            except Exception as e:
                print(f"Error updating row: {e}")
    else:
        payload = {
            "region": region,
            "period": period,
            "source": source,
            "category": category,
            "unit": unit,
            "value": rounded_value,
            "update": today_date,
            "note": ""
        }
        print(f"Inserting new row: {payload}")
        try:
            response = requests.post(energy_endpoint, headers=headers, json=payload)
            print(f"Insert response: {response.status_code}, {response.text}")
        except Exception as e:
            print(f"Error inserting row: {e}")

def process_and_upload_bna(existing_data, csv_data):
    """Process Bundesnetzagentur CSV data and upload it to the Directus API."""
    print("Processing Bundesnetzagentur CSV data...")
    csv_reader = csv.reader(csv_data.splitlines(), delimiter=';')
    header = next(csv_reader)  # Extract header row to identify years
    years = [col for col in header if col.isdigit()]
    
    for row in csv_reader:
        if len(row) < len(years) + 1 or not row[0].isdigit():
            continue
        week = int(row[0])
        for i, year in enumerate(years):
            if row[i + 1]:  # Ensure there is data in this column
                try:
                    value = float(row[i + 1])
                except ValueError:
                    continue
                period = week_to_date(int(year), week)
                print(f"Processing row: de, {period}, {value}")
                upsert_data(existing_data, "de", period, "Bundesnetzagentur", "gas|usage", "GWh", value)

def process_and_upload_aggm(existing_data, csv_data):
    """Process AGGM CSV data and upload it to the Directus API."""
    print("Processing AGGM CSV data...")
    csv_reader = csv.DictReader(csv_data.splitlines())
    for row in csv_reader:
        if row["variable"] != "value":
            continue
        region = "at"
        period = row["date"]
        value = float(row["value"])
        print(f"Processing row: {region}, {period}, {value}")
        upsert_data(existing_data, region, period, "AGGM", "gas|usage", "TWh", value)

def main():
    slack_log("📊 Starte Gasdaten-Sync (AGGM & BNetzA)", level="INFO")

    try:
        existing_data = fetch_existing_data()
        
        csv_data_aggm = fetch_csv(csv_url_aggm)
        if csv_data_aggm:
            process_and_upload_aggm(existing_data, csv_data_aggm)

        csv_data_bna = fetch_csv(csv_url_bna)
        if csv_data_bna:
            process_and_upload_bna(existing_data, csv_data_bna)

        slack_log("Gasdaten-Sync abgeschlossen.", level="SUCCESS")
    
    except Exception as e:
        slack_log(f"❌ Fehler bei Gasdaten-Sync (AGGM & BNetzA): {e}", level="ERROR")
        raise

if __name__ == "__main__":
    main()
