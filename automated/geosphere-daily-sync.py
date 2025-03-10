import requests
import datetime
import json
from dotenv import load_dotenv
import os

# Load from the nearest .env file (searches parent directories too)
load_dotenv()

def main():
    # ---------------------------
    # 1) CONFIGURATION VARIABLES
    # ---------------------------
    # Directus instance
    directus_api_url = os.getenv("DIRECTUS_API_URL")
    directus_api_key = os.getenv("DIRECTUS_API_TOKEN")
    
    # Number of days to fetch from the Geosphere API
    days_back = 500
    
    # Geosphere endpoint (for daily data klima-v2-1d)
    geosphere_base_url = "https://dataset.api.hub.geosphere.at/v1/station/historical/klima-v2-1d"
    
    # ---------------------------
    # 2) FETCH STATIONS FROM DIRECTUS
    # ---------------------------
    print("[DEBUG] Fetching station list from Directus...")
    stations_endpoint = f"{directus_api_url}/items/at_geosphere_stations?limit=-1"
    headers = {
        "Authorization": f"Bearer {directus_api_key}"
    }
    
    try:
        station_response = requests.get(stations_endpoint, headers=headers)
        station_response.raise_for_status()
    except Exception as e:
        print("[ERROR] Could not fetch stations from Directus:", e)
        return
    
    stations_data = station_response.json()
    stations = stations_data.get("data", [])
    print(f"[DEBUG] Found {len(stations)} stations in Directus.")
    
    # ---------------------------
    # 3) PREPARE DATE RANGE
    # ---------------------------
    end_date = datetime.date.today() - datetime.timedelta(days=1) # yesterday
    start_date = end_date - datetime.timedelta(days=days_back)
    
    # Format as YYYY-MM-DD
    start_str = start_date.isoformat()
    end_str = end_date.isoformat()
    
    # Example: 2025-02-01, 2025-02-25
    print(f"[DEBUG] Fetching data from {start_str} to {end_str} (last {days_back} days).")
    
    # ---------------------------
    # 4) LOOP OVER STATIONS
    # ---------------------------

    for station_entry in stations:
        station_id = station_entry.get("id")
        if not station_id:
            print("[WARN] No 'station' field found in station entry, skipping:", station_entry)
            continue
        
        print(f"[DEBUG] Processing station {station_id}...")

        params = {
            "start": start_str,  # e.g. '2025-02-01'
            "end": end_str,      # e.g. '2025-02-25'
            "parameters": "tl_mittel,tlmax,tlmin,sh",
            "station_ids": station_id
        }
        try:
            geo_resp = requests.get(geosphere_base_url, params=params)
            geo_resp.raise_for_status()
            geo_data = geo_resp.json()
        except Exception as e:
            print(f"[ERROR] Could not fetch data for station {station_id} from Geosphere:", e)
            continue
        
        features = geo_data.get("features", [])
        if not features:
            print(f"[DEBUG] No 'features' returned for station {station_id}, skipping.")
            continue
        
        feature = features[0]
        timestamps = geo_data.get("timestamps", [])
    
        # If no timestamps, skip
        if not timestamps:
            print(f"[DEBUG] No timestamps returned for station {station_id}, skipping.")
            continue
        
        # Parse parameters
        parameters = feature.get("properties", {}).get("parameters", {})
        sh_data = parameters.get("sh", {}).get("data", [])
        tlmax_data = parameters.get("tlmax", {}).get("data", [])
        tlmin_data = parameters.get("tlmin", {}).get("data", [])
        tlmittel_data = parameters.get("tl_mittel", {}).get("data", [])
    
        #
        # 2) GET EARLIEST AND LATEST DATES FROM THE RETURNED TIMESTAMPS
        #
        # Example timestamp: "2025-02-01T00:00+00:00"
        geo_start_date = timestamps[0].split("T")[0]  # earliest date in Geosphere data
        geo_end_date   = timestamps[-1].split("T")[0] # latest date in Geosphere data
    
        print(f"[DEBUG] Geosphere range for station={station_id}: {geo_start_date} to {geo_end_date}")
    
        #
        # 3) FETCH ONLY THE RELEVANT DATE RANGE FROM DIRECTUS
        #    Using logical operators (_and) to filter station + date range.
        #
        directus_filter = {
            "_and": [
                {"station": {"_eq": str(station_id)}},
                {"date": {"_gte": geo_start_date}},
                {"date": {"_lte": geo_end_date}}
            ]
        }
    
        existing_data_url = f"{directus_api_url}/items/at_geosphere_data"
        try:
            # Include ?limit=-1 to get all matching records
            directus_existing_resp = requests.get(
                existing_data_url,
                headers=headers,
                params={
                    "limit": -1,
                    "filter": json.dumps(directus_filter)
                }
            )
            directus_existing_resp.raise_for_status()
            existing_records_list = directus_existing_resp.json().get("data", [])
        except Exception as e:
            print(f"[ERROR] Could not fetch existing data for station {station_id}:", e)
            continue
        
        # Store existing records in a dict keyed by date for quick lookup
        existing_data_dict = {}
        for record in existing_records_list:
            existing_data_dict[record["date"]] = record
    
        print(f"[DEBUG] Directus returned {len(existing_records_list)} existing records for station {station_id} in that date range.")
    
        #
        # 4) COMPARE AND INSERT OR UPDATE
        #
        for i, ts in enumerate(timestamps):
            date_part = ts.split("T")[0]  # "YYYY-MM-DD"

            # Extract parameter values for the i-th timestamp
            sh_val = sh_data[i] if i < len(sh_data) else None
            tlmax_val = tlmax_data[i] if i < len(tlmax_data) else None
            tlmin_val = tlmin_data[i] if i < len(tlmin_data) else None
            tlmittel_val = tlmittel_data[i] if i < len(tlmittel_data) else None

            # 1) ADD THIS CHECK
            # If all four parameters are None, skip this date
            if ((sh_val is None or sh_val == 0) and
                tlmax_val is None and
                tlmin_val is None and
                tlmittel_val is None):
                print(f"[DEBUG] All parameters empty for station={station_id}, date={date_part}. Skipping.")
                continue
            
            # 2) BUILD PAYLOAD
            payload = {
                "station": str(station_id),
                "date": date_part,
                "sh": sh_val,
                "tlmax": tlmax_val,
                "tlmin": tlmin_val,
                "tl_mittel": tlmittel_val
            }

            # 3) CHECK DIRECTUS DICTIONARY & INSERT/UPDATE AS BEFORE
            existing_record = existing_data_dict.get(date_part)
            if existing_record is None:
                # CREATE a new record if none exists
                post_url = f"{directus_api_url}/items/at_geosphere_data"
                try:
                    create_resp = requests.post(post_url, headers=headers, json=payload)
                    create_resp.raise_for_status()
                    new_record = create_resp.json().get("data", {})
                    existing_data_dict[date_part] = new_record  # track in dict
                    print(f"[DEBUG] Created new record for station={station_id}, date={date_part}")
                except Exception as e:
                    print(f"[ERROR] Could not create record for station={station_id}, date={date_part}:", e)
            else:
                # UPDATE only if something changed
                needs_update = (
                    existing_record.get("sh") != sh_val or
                    existing_record.get("tlmax") != tlmax_val or
                    existing_record.get("tlmin") != tlmin_val or
                    existing_record.get("tl_mittel") != tlmittel_val
                )
                if needs_update:
                    record_id = existing_record["id"]
                    patch_url = f"{directus_api_url}/items/at_geosphere_data/{record_id}"
                    try:
                        update_resp = requests.patch(patch_url, headers=headers, json=payload)
                        update_resp.raise_for_status()
                        updated_record = update_resp.json().get("data", {})
                        existing_data_dict[date_part] = updated_record
                        print(f"[DEBUG] Updated record ID={record_id} (station={station_id}, date={date_part})")
                    except Exception as e:
                        print(f"[ERROR] Could not update record ID={record_id}:", e)
                else:
                    print(f"[DEBUG] No changes for station={station_id}, date={date_part}, skipping update.")
    
    print("[INFO] Processing complete.")

if __name__ == "__main__":
    main()
