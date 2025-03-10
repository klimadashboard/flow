import requests
import pandas as pd
import logging
from dotenv import load_dotenv
import os

load_dotenv()

DIRECTUS_API_URL = os.getenv("DIRECTUS_API_URL")
DIRECTUS_API_TOKEN = os.getenv("DIRECTUS_API_TOKEN")

# Configure these variables to match the data you want to import
DATASET_NAME = "nrg_ind_ren"
TABLE_NAME = "mobility"
CATEGORY_NAME = "share_renewable"

def fetch_eurostat_data(dataset_name):
    """Fetches dataset from Eurostat API."""
    logging.info(f"Fetching dataset: {dataset_name}")
    url = f"https://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0/data/dataflow/ESTAT/{dataset_name}/1.0/*.*.*.*?compress=false&format=csvdata&formatVersion=2.0&lang=en&labels=name"
    response = requests.get(url)
    response.raise_for_status()
    
    with open("eurostat_data.csv", "wb") as f:
        f.write(response.content)
    
    logging.info("Dataset successfully downloaded.")
    return pd.read_csv("eurostat_data.csv")

def transform_data(df, category_name, source_name):
    """Transforms the dataset into the required long format and filters data."""
    logging.info("Transforming data into long format.")
    if "geo" not in df.columns or "TIME_PERIOD" not in df.columns or "OBS_VALUE" not in df.columns or "nrg_bal" not in df.columns:
        logging.error("Required columns 'geo', 'TIME_PERIOD', 'OBS_VALUE', or 'nrg_bal' not found in dataset.")
        raise KeyError("Could not find the required columns in the dataset.")
    
    # Filter only rows where nrg_bal = 'REN'
    df = df[df["nrg_bal"] == "REN_TRA"]
    
    df_long = df.rename(columns={"geo": "region", "TIME_PERIOD": "period", "OBS_VALUE": "value"})
    df_long["unit"] = "Percentage"
    df_long["category"] = category_name
    df_long["source"] = source_name
    df_long["note"] = ""
    
    # Ensure 'value' is numeric, convert invalid values to None
    df_long["value"] = pd.to_numeric(df_long["value"], errors='coerce')
    df_long = df_long.dropna(subset=["value"])  # Remove rows where value is NaN
    
    logging.info("Data transformation complete.")
    return df_long[["region", "period", "unit", "category", "source", "value", "note"]]

def row_exists(directus_url, table_name, region, period, category, headers):
    """Check if a row already exists in the Directus API."""
    query_url = f"{directus_url}/items/{table_name}?filter[region][_eq]={region}&filter[period][_eq]={period}&filter[category][_eq]={category}"
    try:
        response = requests.get(query_url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            if data and data.get('data'):
                return data['data'][0]  # Return the existing row
        return None
    except Exception as e:
        logging.error(f"Error checking if row exists: {e}")
        return None

def upsert_to_directus(df, directus_url, directus_token, table_name):
    """Upserts data into Directus."""
    logging.info("Starting data upsert to Directus.")
    headers = {"Authorization": f"Bearer {directus_token}", "Content-Type": "application/json"}
    
    for _, row in df.iterrows():
        existing_row = row_exists(directus_url, table_name, row['region'], row['period'], row['category'], headers)
        data = {
            "region": row["region"],
            "period": row["period"],
            "unit": row["unit"],
            "category": row["category"],
            "source": row["source"],
            "value": row["value"],
            "note": row["note"]
        }
        
        if existing_row:
            # Update only if the value has changed
            if float(existing_row.get("value", 0)) != row["value"]:
                update_url = f"{directus_url}/items/{table_name}/{existing_row['id']}"
                response = requests.patch(update_url, headers=headers, json=data)
                if response.status_code in [200, 204]:
                    logging.info(f"Successfully updated row: {data}")
                else:
                    logging.error(f"Failed to update row: {data}. Status code: {response.status_code}, Response: {response.text}")
        else:
            insert_url = f"{directus_url}/items/{table_name}"
            response = requests.post(insert_url, headers=headers, json=data)
            if response.status_code in [200, 201]:
                logging.info(f"Successfully inserted new row: {data}")
            else:
                logging.error(f"Failed to insert row: {data}. Status code: {response.status_code}, Response: {response.text}")
    logging.info("Data upsert to Directus completed.")

def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info("Script started.")
    
    logging.info("Fetching data from Eurostat.")
    df = fetch_eurostat_data(DATASET_NAME)
    logging.info("Transforming data.")
    transformed_df = transform_data(df, CATEGORY_NAME, "Eurostat")
    logging.info("Uploading data to Directus.")
    upsert_to_directus(transformed_df, DIRECTUS_API_URL, DIRECTUS_API_TOKEN, TABLE_NAME)
    
    logging.info("Data successfully transferred to Directus.")
    print("Data successfully transferred to Directus.")

if __name__ == "__main__":
    main()
