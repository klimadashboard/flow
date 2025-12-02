import os
import subprocess
import pandas as pd
import requests
from dotenv import load_dotenv
from datetime import datetime
from pyproj import Transformer

# ============================================================
# Configuration
# ============================================================

ZENODO_DOI = "10.5281/zenodo.17592700"
DOWNLOAD_DIR = "zenodo_files"  # where zenodo_get will store files

TABLE_NAME = "energy_wind_units"

# Load environment variables
load_dotenv()
API_URL = os.getenv("DIRECTUS_API_URL").rstrip("/")
API_TOKEN = os.getenv("DIRECTUS_API_TOKEN")

# Headers for Directus API
HEADERS = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Content-Type": "application/json",
}

# Status mapping
STATUS_MAP = {
    "In Betrieb": "35",
    "In Realisierung": "31",
    "Stillgelegt": "50",
    "Genehmigt": "23",
    "Geplant": "20",
    "Unbekannt": "0",
}

# Coordinate transformer: EPSG:25832 (ETRS89 / UTM zone 32N) -> EPSG:4326 (lon/lat)
transformer = Transformer.from_crs("EPSG:25832", "EPSG:4326", always_xy=True)


# ============================================================
# Helpers
# ============================================================

def run_zenodo_get():
    """
    Download all CSVs for the given Zenodo DOI into DOWNLOAD_DIR using zenodo_get.
    Requires `zenodo_get` to be installed and on PATH.
    """
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    # -g "*.csv" ensures we only download CSV files from the record :contentReference[oaicite:1]{index=1}
    cmd = [
        "zenodo_get",
        ZENODO_DOI,
        "-o",
        DOWNLOAD_DIR,
        "-g",
        "*.csv",
    ]
    print("📥 Running:", " ".join(cmd))
    subprocess.run(cmd, check=True)
    print("✅ Downloaded CSV files from Zenodo.")


def get_latest_csv(download_dir: str) -> str:
    """
    Find the 'latest' CSV in download_dir.
    Strategy: choose the CSV whose filename is lexicographically greatest.
    This works well if filenames include a date like ..._2025_11_12.csv.
    """
    csv_files = [
        os.path.join(download_dir, f)
        for f in os.listdir(download_dir)
        if f.lower().endswith(".csv")
    ]

    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in '{download_dir}'")

    # Sort by filename and take the last one
    csv_files.sort()
    latest = csv_files[-1]
    print(f"📄 Using latest CSV: {os.path.basename(latest)}")
    return latest


def to_date(val):
    try:
        if val is None or str(val).strip() == "":
            return None
        return pd.to_datetime(val).strftime("%Y-%m-%d")
    except Exception:
        return None


def to_float(val):
    try:
        if val is None or str(val).strip() == "":
            return None
        return float(str(val).replace(",", "."))
    except Exception:
        return None


def is_valid_id(value):
    return isinstance(value, str) and value.startswith("SEE9") and value.isalnum()


def is_valid_latlon(lat, lon):
    return (
        isinstance(lat, (float, int))
        and isinstance(lon, (float, int))
        and -90 <= lat <= 90
        and -180 <= lon <= 180
    )


# ============================================================
# Main import logic
# ============================================================

def main():
    # 1) Download files from Zenodo
    run_zenodo_get()

    # 2) Pick the latest CSV
    csv_path = get_latest_csv(DOWNLOAD_DIR)

    # 3) Read CSV
    df = pd.read_csv(csv_path, sep=",", encoding="utf-8", dtype=str).fillna("")

    records = []
    skipped = []

    for idx, row in df.iterrows():
        id_raw = str(row["einheit_mastr_nummer"]).strip() if "einheit_mastr_nummer" in row else ""
        if not is_valid_id(id_raw):
            skipped.append((idx, "Invalid ID"))
            continue

        # Coordinates in EPSG:25832
        x = to_float(row.get("x_25832"))
        y = to_float(row.get("y_25832"))
        if x is None or y is None:
            skipped.append((idx, "Missing projected coordinates"))
            continue

        try:
            lon, lat = transformer.transform(x, y)
        except Exception:
            skipped.append((idx, "Coordinate transform failed"))
            continue

        if not is_valid_latlon(lat, lon):
            skipped.append((idx, "Invalid transformed lat/lon"))
            continue

        net_power = to_float(row.get("nettonennleistung"))
        if not net_power or net_power <= 0:
            skipped.append((idx, "Invalid or missing net power"))
            continue

        record = {
            "id": id_raw,
            "name": str(row.get("name_windpark", "")),
            "status": STATUS_MAP.get(str(row.get("einheit_betriebsstatus", "")).strip(), "0"),
            "commissioning_date": to_date(row.get("datum_inbetriebnahme")),
            "last_update": to_date(row.get("datum_genehmigung") or row.get("datum_antrag")),
            "shutdown_date": to_date(row.get("datum_endgueltige_stilllegung")),
            "power_kw": net_power,
            "net_power_kw": net_power,
            "height": to_float(row.get("nabenhoehe")),
            "rotor_diameter": to_float(row.get("rotordurchmesser")),
            # use text bundesland/landkreis/gemeinde from CSV
            "federal_state": str(row.get("bundesland")) or None,
            "district": str(row.get("landkreis")) or None,
            "municipality": str(row.get("gemeinde")) or None,
            # AGS Gemeinde -> 8-digit region code
            "region": (
                str(row.get("ags_gemeinde")).zfill(8)
                if row.get("ags_gemeinde")
                else None
            ),
            "country": "DE",
            "lat": lat,
            "lon": lon,
        }
        records.append(record)

    print(f"✅ Valid records: {len(records)}")
    print(f"⚠️ Skipped records: {len(skipped)}")

    # Optionally save skipped rows to file
    with open("skipped_rows.log", "w", encoding="utf-8") as f:
        for index, reason in skipped:
            f.write(f"Row {index}: {reason}\n")

    # 4) Delete all existing entries in the Directus table
    print("🧹 Deleting existing records...")
    delete_url = f"{API_URL}/items/{TABLE_NAME}"
    existing = requests.get(delete_url, headers=HEADERS, params={"limit": -1, "fields": "id"}).json()

    if "data" in existing:
        ids = [item["id"] for item in existing["data"]]
    else:
        # If your Directus is configured to return bare arrays instead of {"data": [...]}
        if isinstance(existing, list):
            ids = [item["id"] for item in existing]
        else:
            ids = []

    for batch_ids in [ids[i : i + 1000] for i in range(0, len(ids), 1000)]:
        if not batch_ids:
            continue
        res = requests.delete(delete_url, headers=HEADERS, json={"keys": batch_ids})
        print(f"Deleted {len(batch_ids)} entries:", res.status_code, res.text)

    # 5) Insert new records
    print(f"⬆️ Inserting {len(records)} new records...")
    for i in range(0, len(records), 500):
        batch = records[i : i + 500]
        # Adjust payload shape if your Directus expects {"data": batch} instead of just batch
        payload = batch
        res = requests.post(delete_url, headers=HEADERS, json=payload)
        print(
            f"Inserted {i+1}-{i+len(batch)}:",
            res.status_code,
            "" if res.status_code in (200, 201) else res.text,
        )

    print("✅ Done.")


if __name__ == "__main__":
    main()
