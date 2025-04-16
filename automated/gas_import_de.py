import requests
from dotenv import load_dotenv
import os
import pandas as pd
from datetime import datetime

# Load from the nearest .env file (searches parent directories too)
load_dotenv()
# Set these as environment variables or update the defaults below.
api_url = os.getenv("DIRECTUS_API_URL")  # Replace with your Directus instance URL
api_token = os.getenv("DIRECTUS_API_TOKEN")  # Replace with your Directus API token
headers = {
    "Authorization": f"Bearer {api_token}",
    "Content-Type": "application/json"
}

gas_endpoint = f"{api_url}/items/gas_imports"

def get_country_by_german_name(country_de):
    """
    Fetch a country from Directus by it's German name.
    """
    params = {
        "filter[name_de][_eq]": country_de
    }
    url = f"{api_url}/items/countries"
    headers = {"Authorization": f"Bearer {api_token}"}
    response = requests.get(url, headers=headers, params=params)
    if response.status_code != 200:
        print("Error fetching regions:", response.text)
        return []
    data = response.json()
    return data.get("data", [])

def get_existing_gas_data(country):
    """
    Fetch all current data on gas imports for a specific country.
    """
    params = {
        "filter[Country][_eq]": country,
        "limit": -1
    }
    headers = {"Authorization": f"Bearer {api_token}"}
    response = requests.get(gas_endpoint, headers=headers, params=params)
    if response.status_code != 200:
        print("Error fetching regions:", response.text)
        return []
    data = response.json().get("data", [])
    database_df = pd.DataFrame(data)
    if len(database_df) <= 0:
        database_df = pd.DataFrame(columns=["id", "Country", "import_country", "import_source", "date", "value"])
    
    database_df.set_index("id")
    print(f"Existing data successfully loaded. Found {len(database_df)} rows for {country}.")
    return database_df

def read_csv_data(path):
    """
    Read data from the given csv and transform it into database format.
    """
    raw_df = pd.read_csv(path, sep=";")
    raw_df.rename(columns={'                     .': 'date'}, inplace=True)
    raw_df['date'] = pd.to_datetime(raw_df['date'], format="%d.%m.%Y").astype(str)
    df = raw_df.melt('date').dropna()
    df["Country"] = "DE"
    df["import_country"] = None
    df["import_source"] = None

    import_source_keys = df["variable"].unique()
    for key in import_source_keys:
        country = get_country_by_german_name(key)
        if len(country) > 0: # country found
            country = country[0]
            df.loc[df["variable"] == key, "import_country"] = country["id"]
        else: # no country found -> set custom import_source
            df.loc[df["variable"] == key, "import_source"] = key

    del df["variable"]
    print(f"CSV data successfully loaded. Loaded {len(df)} rows from {path}.")
    return df

def get_insert_update_df(existing_df, csv_df):
    """
    Find, which rows already exist in the database, which are new, and which need to be updated.
    """
    
    # 1. find existing rows and remove exact matches from insert_dataframe
    merged_df = pd.merge(csv_df, existing_df, on=["Country", "import_country", "import_source", "date", "value"], how='left', indicator=True)
    merged_df.head()
    # "both" means that we can skip this datapoint
    # "left_only" means that it is a new datapoint or that the value of this datapoint was updated
    filtered_df = merged_df[merged_df["_merge"] == "left_only"]
    del filtered_df["_merge"], filtered_df["id"]
    
    # 2. find existing rows that have an updated value and update the corresponding row in the database; remove the row from insert_dataframe
    merged_df = pd.merge(filtered_df, existing_df, on=["Country", "import_country", "import_source", "date"], how="left", indicator=True)
    # "left_only" means that it is a new datapoint
    # "both" means that we need to update the value of this datapoint

    insert_df = merged_df[merged_df["_merge"] == "left_only"]
    update_df = merged_df[merged_df["_merge"] == "both"]
    del insert_df["_merge"], insert_df["id"], insert_df["value_y"], update_df["_merge"]
    insert_df = insert_df.rename(columns={'value_x': 'value'})
    
    return insert_df, update_df

def update_gas_datapoint(gas_id, update_data):
    """
    Update a gas import (by ID) with new data using the Directus API.
    """
    url = f"{gas_endpoint}/{gas_id}"
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json"
    }
    response = requests.patch(url, json=update_data, headers=headers)
    if response.status_code not in (200, 204):
        print(f"Failed to update region {gas_id}: {response.text}")
    else:
        print(f"Region {gas_id} updated successfully.")

def insert_gas_datapoints(insert_df):
    """
    Insert new gas import data using the Directus API.
    """
    response = requests.post(
        gas_endpoint,
        headers=headers,
        json=insert_df.to_dict('records'),
    )

    if response.status_code == 200:
        print(f"Data successfully inserted. Inserted {len(insert_df)} rows.")
    else:
        print(f"Error inserting data: {response.status_code} - {response.text}")


def main():
    print(f"---running gas_import_de.py on {datetime.now()}")
    existing_df = get_existing_gas_data("DE")
    csv_df = read_csv_data("https://www.bundesnetzagentur.de/_tools/SVG/js2/_functions/csv_export.html?view=renderCSV&id=1081248")
    # csv_df = csv_df[80:89]

    insert_df, update_df = get_insert_update_df(existing_df, csv_df)

    print(f"updating {len(update_df)} rows; inserting {len(insert_df)} rows")

    # update modified values
    for _, row in update_df.iterrows():
        update_gas_datapoint(int(row.id), {'value': row.value_x})

    # insert new datapoints in batches
    batch_size = 100
    for i in range(0, len(insert_df), batch_size):
        insert_batch = insert_df.iloc[i:i + batch_size]
        insert_gas_datapoints(insert_batch)

    print("---patch completed")
    

if __name__ == "__main__":
    main()