import requests
import pandas as pd
import numpy as np
import zipfile
import io
import urllib.request
from datetime import datetime
import time
from slack_logger import slack_log
from dotenv import load_dotenv
import os

# Load from the nearest .env file (searches parent directories too)
load_dotenv()

# Directus API endpoint and access token
directus_api_url = os.getenv("DIRECTUS_API_URL")  # Replace with your Directus API URL
access_token = os.getenv("DIRECTUS_API_TOKEN")  # Replace with your actual Directus access token

headers = {
    'Content-Type': 'application/json',
    'Authorization': f'Bearer {access_token}',
}

def get_station_ids():
    print("Fetching station IDs from de_dwd_stations...")
    response = requests.get(
        f'{directus_api_url}/items/de_dwd_stations',
        headers=headers,
        params={'fields': 'id', 'limit': -1}
    )
    if response.status_code != 200:
        print(f"Failed to retrieve stations: {response.status_code}, {response.text}")
        return []
    data = response.json()
    station_ids = [item['id'] for item in data.get('data', [])]
    print(f"Retrieved {len(station_ids)} station IDs.")
    return station_ids

def get_recent_data_for_station(station_id):
    zero_padded_id = str(station_id).zfill(5)
    url_recent = (
        f"https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/daily/kl/recent/"
        f"tageswerte_KL_{zero_padded_id}_akt.zip"
    )
    print(f"Fetching recent data for station {station_id} from URL: {url_recent}")
    try:
        # Download and read the ZIP file
        with urllib.request.urlopen(url_recent) as resp:
            with zipfile.ZipFile(io.BytesIO(resp.read())) as zip_file:
                # Find the data file within the ZIP archive
                data_file = [name for name in zip_file.namelist() if name.startswith('produkt_klima')][0]
                with zip_file.open(data_file) as f:
                    df = pd.read_csv(f, sep=';')
                    return df
    except Exception as e:
        print(f"Error fetching data for station {station_id}: {e}")
        return pd.DataFrame()

def process_station_data(station_id):
    df = get_recent_data_for_station(station_id)
    if df.empty:
        print(f"No data found for station {station_id}")
        return
    # Remove duplicates
    df.drop_duplicates(subset=['MESS_DATUM'], inplace=True)
    # Extract required columns
    required_columns = ['MESS_DATUM', ' TNK', ' TXK', ' TMK', 'SHK_TAG']
    for col in required_columns:
        if col not in df.columns:
            print(f"Column '{col}' not found in data for station {station_id}. Filling with NaN.")
            df[col] = pd.NA
    df = df[required_columns]
    # Rename columns
    df.rename(columns={
        'MESS_DATUM': 'date',
        ' TNK': 'tlmin',
        ' TXK': 'tlmax',
        ' TMK': 'tl_mittel',
        'SHK_TAG': 'sh'
    }, inplace=True)
    # Convert date to datetime
    df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')
    # Clean data: Replace invalid values with NaN
    invalid_values = [-999, -999.0]
    df.replace(invalid_values, np.nan, inplace=True)
    # Convert numeric fields
    df[['tlmin', 'tlmax', 'tl_mittel', 'sh']] = df[['tlmin', 'tlmax', 'tl_mittel', 'sh']].apply(pd.to_numeric, errors='coerce')
    # Add station ID
    df['station'] = station_id
    # Convert date to string in 'YYYY-MM-DD' format
    df['date'] = df['date'].dt.strftime('%Y-%m-%d')
    # Convert DataFrame to list of dictionaries
    data_records = df.to_dict(orient='records')
    # Convert data types
    for record in data_records:
        for key in record:
            if pd.isna(record[key]):
                record[key] = None
            elif isinstance(record[key], np.generic):
                record[key] = record[key].item()
    # Prepare data for batch insertion/updating
    batch_update_data_in_directus(data_records, station_id)

def batch_update_data_in_directus(data_list, station_id):
    print(f"Updating data in Directus for station {station_id}...")
    if not data_list:
        print(f"No data to update for station {station_id}.")
        return
    # Extract dates from data_list
    dates = [record['date'] for record in data_list]
    # Fetch existing data for these dates and station, including required fields
    filter_query = f'?filter[station][_eq]={station_id}&filter[date][_in]={",".join(dates)}&limit=-1&fields=id,date,station,tlmin,tlmax,tl_mittel,sh'
    response = requests.get(
        f'{directus_api_url}/items/de_dwd_data{filter_query}',
        headers=headers
    )
    if response.status_code != 200:
        print(f"Failed to fetch existing data for station {station_id}: {response.status_code}, {response.text}")
        return
    existing_data = response.json().get('data', [])
    # Create a mapping of (station, date) to existing record
    existing_records_map = {(item['station'], item['date']): item for item in existing_data}
    # Prepare lists for individual updates and batch inserts
    records_to_update = []
    records_to_insert = []
    for record in data_list:
        key = (record['station'], record['date'])
        if key in existing_records_map:
            existing_record = existing_records_map[key]
            # Compare temperature and sh fields
            fields_to_compare = ['tlmin', 'tlmax', 'tl_mittel', 'sh']
            data_changed = False
            for field in fields_to_compare:
                new_value = record.get(field)
                existing_value = existing_record.get(field)
                if existing_value != new_value:
                    data_changed = True
                    break  # No need to check further if any field is different
            if data_changed:
                # Record needs to be updated
                record['id'] = existing_record['id']
                records_to_update.append(record)
            else:
                # Data is the same, skip update
                pass
        else:
            # Record does not exist, prepare for insert
            records_to_insert.append(record)
    # Perform individual updates
    for record in records_to_update:
        record_id = record.pop('id')
        update_response = requests.patch(
            f'{directus_api_url}/items/de_dwd_data/{record_id}',
            headers=headers,
            json=record
        )
        if update_response.status_code not in (200, 204):
            print(f"Failed to update record {record_id} for station {station_id}: {update_response.status_code}, {update_response.text}")
        else:
            print(f"Successfully updated record {record_id} for station {station_id}.")
        time.sleep(0.05)  # Short delay to avoid overwhelming the API
    # Perform batch inserts
    if records_to_insert:
        print(f"Inserting {len(records_to_insert)} new records for station {station_id}.")
        # Split records_to_insert into batches if necessary
        max_batch_size = 1000  # Adjust based on Directus API limits
        for i in range(0, len(records_to_insert), max_batch_size):
            batch = records_to_insert[i:i + max_batch_size]
            insert_response = requests.post(
                f'{directus_api_url}/items/de_dwd_data',
                headers=headers,
                json=batch  # Send the batch directly, without wrapping in 'data'
            )
            if insert_response.status_code not in (200, 201):
                print(f"Failed to insert records for station {station_id}: {insert_response.status_code}, {insert_response.text}")
            else:
                print(f"Successfully inserted {len(batch)} records for station {station_id}.")
            time.sleep(0.1)  # Short delay to avoid overwhelming the API
    if not records_to_update and not records_to_insert:
        print(f"No changes needed for station {station_id}.")

# Main script
if __name__ == "__main__":
    slack_log("start dwd data sync", level="INFO")
    station_ids = get_station_ids()
    for station_id in station_ids:
        try:
            process_station_data(station_id)
        except Exception as e:
            print(f"ERROR processing station {station_id}: {e}")
            slack_log(f"❌ Fehler bei DWD-Datensync für Station {station_id}: {e}", level="ERROR")
    slack_log("dwd data sync completed", level="SUCCESS")