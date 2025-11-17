import os
import io
import requests
import pandas as pd
import logging
from dotenv import load_dotenv
from slack_logger import slack_log

# ── Config & logging ────────────────────────────────────────────────────────────
load_dotenv()
BASE_URL = os.getenv("DIRECTUS_API_URL")
TOKEN = os.getenv("DIRECTUS_API_TOKEN")
if not BASE_URL or not TOKEN:
    raise EnvironmentError("Missing Directus credentials")

headers = {"Authorization": f"Bearer {TOKEN}"}

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
slack_log("🔄 Start PV & Wasserkraft Upload", level="INFO")

# ── Download Excel (to disk) ───────────────────────────────────────────────────
excel_path = "MoMeGes_Bilanz-2025.xlsx"
url = "https://www.e-control.at/documents/1785851/12985028/MoMeGes_Bilanz_2025.xlsx"

resp = requests.get(url, timeout=60)
resp.raise_for_status()
with open(excel_path, "wb") as f:
    f.write(resp.content)

# ── Load Excel with explicit engine ────────────────────────────────────────────
def load_excel_xlsx(path: str) -> pd.ExcelFile:
    """
    Try openpyxl first (xlsx), fall back with a clear error if not installed.
    """
    try:
        return pd.ExcelFile(path, engine="openpyxl")
    except ImportError as e:
        raise RuntimeError(
            "openpyxl is required to read .xlsx files. "
            "Install it via: pip install openpyxl"
        ) from e
    except ValueError:
        # In case the server changed format unexpectedly, try without engine
        # (lets Pandas auto-detect for legacy .xls or others).
        return pd.ExcelFile(path)

xls = load_excel_xlsx(excel_path)

# Sheet parsing
df_erzeugung = xls.parse("Erzeugung", header=None)
header_row = 7  # zero-based
labels = df_erzeugung.iloc[header_row].tolist()

# ── Helper to extract series ───────────────────────────────────────────────────
def extract_and_prepare(label_text: str, type_tag: str) -> pd.DataFrame:
    if label_text not in labels:
        raise ValueError(f"'{label_text}' not found in Excel header.")
    col_idx = labels.index(label_text)

    date_col = df_erzeugung.iloc[:, 0].reset_index(drop=True)
    value_col = df_erzeugung.iloc[:, col_idx].reset_index(drop=True)

    # Find block delimited by "Einheit" row where unit == "GWh"
    start_candidates = value_col[(date_col == "Einheit") & (value_col == "GWh")].index
    if start_candidates.empty:
        raise ValueError("Could not find start of data (row with Einheit == GWh).")
    start = start_candidates[0] + 1

    # End is the next "Einheit" row where unit != "GWh"
    end_candidates = value_col[(date_col == "Einheit") & (value_col != "GWh")].index
    if end_candidates.empty:
        # If no explicit end marker is found, go to the end of column
        end = len(value_col)
    else:
        end = end_candidates[0]

    dates = date_col[start:end]
    values = value_col[start:end]
    df = pd.DataFrame({"Date": dates, "value": values})

    # Keep only valid dates, then normalize
    df = df[pd.to_datetime(df["Date"], errors="coerce").notnull()].copy()
    df["Date"] = pd.to_datetime(df["Date"]) + pd.DateOffset(months=1)

    # Convert numeric, GWh → TWh
    df["value"] = pd.to_numeric(df["value"], errors="coerce") / 1000.0
    df = df.dropna(subset=["value"])
    df["Type"] = type_tag
    return df

# ── Extract PV & Wasserkraft ───────────────────────────────────────────────────
df_pv = extract_and_prepare("Photo-\nvoltaik\n(7)", "pv")
df_wk = extract_and_prepare("Summe\nWasser-\nkraft", "wasserkraft")
df_all = pd.concat([df_pv, df_wk], ignore_index=True)
df_all["key"] = df_all["Date"].dt.strftime("%Y-%m-%d") + "_" + df_all["Type"]

# ── Fetch existing entries from Directus ───────────────────────────────────────
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
        },
        timeout=60
    )
    res.raise_for_status()
    # Your Directus is configured without the "data" wrapper (per your setup).
    page = res.json()
    # If it *does* come wrapped for any reason, still handle it:
    if isinstance(page, dict) and "data" in page:
        page = page["data"]
    if not page:
        break
    existing.extend(page)
    if len(page) < 1000:
        break
    offset += 1000

existing_df = pd.DataFrame(existing, columns=["DateTime", "value", "Type"])
if not existing_df.empty:
    existing_df["Date"] = pd.to_datetime(existing_df["DateTime"]).dt.normalize()
    existing_df["value"] = pd.to_numeric(existing_df["value"], errors="coerce")
    # Type might be an array; normalize to scalar strings
    existing_df["Type"] = existing_df["Type"].apply(
        lambda x: (x[0] if isinstance(x, list) and x else x)
    )
    existing_df = existing_df.dropna(subset=["Date", "value", "Type"])
    existing_df["key"] = existing_df["Date"].dt.strftime("%Y-%m-%d") + "_" + existing_df["Type"]
else:
    # Create empty frame with expected columns
    existing_df = pd.DataFrame(columns=["Date", "value", "Type", "key"])

# ── Compute rolling 12-month sum ("Jahresproduktion") ─────────────────────────
combined = pd.concat(
    [existing_df[["Date", "value", "Type"]], df_all[["Date", "value", "Type"]]],
    ignore_index=True
)
combined = combined.drop_duplicates(subset=["Date", "Type"]).sort_values(["Type", "Date"]).reset_index(drop=True)

# Rolling per Type with proper index handling, require full 12 months
combined["Jahresproduktion"] = (
    combined.groupby("Type", group_keys=False)
            .apply(lambda g: g.set_index("Date")["value"].rolling(12, min_periods=12).sum())
            .reset_index(level=0, drop=True)
)

# Merge the rolling sum back to the new rows
df_all = df_all.merge(
    combined[["Date", "Type", "Jahresproduktion"]],
    on=["Date", "Type"],
    how="left"
)

# Filter out entries that already exist
if not existing_df.empty:
    df_all = df_all[~df_all["key"].isin(existing_df["key"])].copy()

# ── Build payloads ─────────────────────────────────────────────────────────────
payloads = []
for _, row in df_all.iterrows():
    payloads.append({
        "DateTime": row["Date"].strftime("%Y-%m-%dT00:00:00"),
        "value": round(float(row["value"]), 6),
        "Jahresproduktion": (round(float(row["Jahresproduktion"]), 6)
                             if pd.notnull(row["Jahresproduktion"]) else None),
        "Country": "AT",
        "Type": [row["Type"]],
        "unit": "TWh",
    })

# ── Upload ────────────────────────────────────────────────────────────────────
if not payloads:
    msg = "✅ Keine neuen Einträge zum Hochladen."
    logging.info(msg)
    slack_log(msg, level="INFO")
else:
    logging.info(f"Uploading {len(payloads)} new entries...")
    success, fail = 0, 0
    for entry in payloads:
        r = requests.post(f"{BASE_URL}/items/ee_produktion", headers=headers, json=entry, timeout=60)
        if r.status_code in (200, 201):
            logging.info(f"✅ {entry['DateTime']} ({entry['Type'][0]}) uploaded")
            success += 1
        else:
            logging.error(f"❌ Failed: {entry['DateTime']} – {r.text}")
            fail += 1
    slack_log(
        f"✅ Upload abgeschlossen: {success} erfolgreich, {fail} fehlgeschlagen.",
        level="SUCCESS" if fail == 0 else "WARNING"
    )

# ── Clean up Excel ────────────────────────────────────────────────────────────
try:
    os.remove(excel_path)
    logging.info("🧹 Excel-Datei gelöscht.")
except Exception as e:
    logging.warning(f"⚠️ Excel-Datei konnte nicht gelöscht werden: {e}")
