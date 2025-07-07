import os
import requests
import pandas as pd
import logging
from dotenv import load_dotenv
from slack_logger import slack_log

# Load environment
load_dotenv()
BASE_URL = os.getenv("DIRECTUS_API_URL")
TOKEN = os.getenv("DIRECTUS_API_TOKEN")
if not BASE_URL or not TOKEN:
    raise EnvironmentError("Missing Directus credentials")

headers = {"Authorization": f"Bearer {TOKEN}"}

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
slack_log("🔄 Start PV & Wasserkraft Upload", level="INFO")

# Download Excel
excel_path = "MoMeGes_Bilanz-2025.xlsx"
url = "https://www.e-control.at/documents/1785851/12985028/MoMeGes_Bilanz-2025.xlsx"
with open(excel_path, "wb") as f:
    f.write(requests.get(url).content)

# Load Excel
xls = pd.ExcelFile(excel_path)
df_erzeugung = xls.parse("Erzeugung", header=None)
header_row = 7
labels = df_erzeugung.iloc[header_row].tolist()

# Extract helper
def extract_and_prepare(label_text, type_tag):
    if label_text not in labels:
        raise ValueError(f"'{label_text}' not found in Excel header.")
    col_idx = labels.index(label_text)
    date_col = df_erzeugung.iloc[:, 0].reset_index(drop=True)
    value_col = df_erzeugung.iloc[:, col_idx].reset_index(drop=True)

    start = value_col[(date_col == "Einheit") & (value_col == "GWh")].index[0] + 1
    end = value_col[(date_col == "Einheit") & (value_col != "GWh")].index[0]

    dates = date_col[start:end]
    values = value_col[start:end]
    df = pd.DataFrame({"Date": dates, "value": values})
    df = df[pd.to_datetime(df["Date"], errors="coerce").notnull()]
    df["Date"] = pd.to_datetime(df["Date"]) + pd.DateOffset(months=1)
    df["value"] = pd.to_numeric(df["value"], errors="coerce") / 1000  # GWh → TWh
    df = df.dropna()
    df["Type"] = type_tag
    return df

# Extract Photovoltaik and Wasserkraft
df_pv = extract_and_prepare("Photo-\nvoltaik\n(7)", "pv")
df_wk = extract_and_prepare("Summe\nWasser-\nkraft", "wasserkraft")
df_all = pd.concat([df_pv, df_wk], ignore_index=True)

# Fetch existing entries
existing = []
offset = 0
while True:
    res = requests.get(
        f"{BASE_URL}/items/ee_produktion",
        headers=headers,
        params={
            "filter[Country][_eq]": "AT",
            "filter[Type][_in]": "pv,wasserkraft",
            "limit": 1000,
            "offset": offset,
            "sort": "DateTime",
            "fields": "DateTime,value,Type"
        }
    )
    res.raise_for_status()
    data = res.json()["data"]
    if not data:
        break
    existing.extend(data)
    offset += 1000

existing_df = pd.DataFrame(existing)
existing_df["Date"] = pd.to_datetime(existing_df["DateTime"]).dt.normalize()
existing_df["value"] = pd.to_numeric(existing_df["value"], errors="coerce")
existing_df["Type"] = existing_df["Type"].apply(lambda x: x[0] if isinstance(x, list) else x)
existing_df = existing_df.dropna(subset=["Date", "value", "Type"])
df_all["key"] = df_all["Date"].dt.strftime("%Y-%m-%d") + "_" + df_all["Type"]
existing_df["key"] = existing_df["Date"].dt.strftime("%Y-%m-%d") + "_" + existing_df["Type"]

# Compute Jahresproduktion
combined = pd.concat([existing_df[["Date", "value", "Type"]], df_all[["Date", "value", "Type"]]])
combined = combined.drop_duplicates(subset=["Date", "Type"]).sort_values("Date").reset_index(drop=True)
combined["Jahresproduktion"] = combined.groupby("Type")["value"].rolling(12).sum().reset_index(drop=True)

# Merge rolling sum back
df_all = df_all.merge(
    combined[["Date", "Type", "Jahresproduktion"]],
    on=["Date", "Type"],
    how="left"
)
df_all = df_all[~df_all["key"].isin(existing_df["key"])].copy()

# Build payloads
payloads = []
for _, row in df_all.iterrows():
    payloads.append({
        "DateTime": row["Date"].strftime("%Y-%m-%dT00:00:00"),
        "value": round(row["value"], 6),
        "Jahresproduktion": round(row["Jahresproduktion"], 6) if pd.notnull(row["Jahresproduktion"]) else None,
        "Country": "AT",
        "Type": [row["Type"]],
        "unit": "TWh"
    })

# Upload directly
if not payloads:
    msg = "✅ Keine neuen Einträge zum Hochladen."
    logging.info(msg)
    slack_log(msg, level="INFO")
else:
    logging.info(f"Uploading {len(payloads)} new entries...")
    success, fail = 0, 0
    for entry in payloads:
        r = requests.post(f"{BASE_URL}/items/ee_produktion", headers=headers, json=entry)
        if r.status_code in [200, 201]:
            logging.info(f"✅ {entry['DateTime']} ({entry['Type'][0]}) uploaded")
            success += 1
        else:
            logging.error(f"❌ Failed: {entry['DateTime']} – {r.text}")
            fail += 1
    slack_log(f"✅ Upload abgeschlossen: {success} erfolgreich, {fail} fehlgeschlagen.", level="SUCCESS" if fail == 0 else "WARNING")

# Clean up Excel
try:
    os.remove(excel_path)
    logging.info("🧹 Excel-Datei gelöscht.")
except Exception as e:
    logging.warning(f"⚠️ Excel-Datei konnte nicht gelöscht werden: {e}")
