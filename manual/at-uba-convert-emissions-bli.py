"""
Convert thg-emissionen CSV (BLI/OLI) to JSON or CSV for Directus import.

Usage:
    python convert_emissions_bli.py
    python convert_emissions_bli.py --format csv

The output file (emissions_import.json or emissions_import.csv) is written
next to this script and can be imported via Directus UI:
    Content → emissions_data → Import

Before importing, manually delete the old BLI records in Directus:
    filter source = "BLI 2025 (1990-2023)" and delete all matches.
"""

import argparse
import csv
import json
import os
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

try:
    import requests
except ImportError:
    requests = None

# ---------------------------------------------------------------------------
# Configuration — adjust as needed
# ---------------------------------------------------------------------------

CSV_FILE = Path("thg-emissionen_1990-2024_nach_ksg_long.csv")
OUTPUT_DIR = Path(__file__).parent

DIRECTUS_URL = os.getenv("DIRECTUS_API_URL", "https://base.klimadashboard.org")

# Source labels that will be stored in the DB `source` field.
SOURCE_STATES  = "BLI 2025 (1990-2023)"
SOURCE_AUSTRIA = "OLI 2025 (1990-2024)"

# ---------------------------------------------------------------------------
# Mappings
# ---------------------------------------------------------------------------

REGION_MAP = {
    "Burgenland":       "2bc3faed-7cb4-492c-9097-145a0f8f1f01",
    "Niederösterreich": "39b741d5-c7ca-4ff2-9c5c-c0709daea62c",
    "Steiermark":       "63bf6c1a-cb90-40f9-913e-daba7431fd56",
    "Wien":             "76888a3b-ce35-482c-88e6-79cff27be8c5",
    "Kärnten":          "9297c491-c419-4ca5-a10f-4d7b93998909",
    "Oberösterreich":   "9cde29fb-d189-4461-b766-5dfd3de56303",
    "Tirol":            "a4bafe0a-f71b-40a0-8d1d-1baa8af05692",
    "Salzburg":         "b9b8feef-4d7d-4c23-ac21-60377d96ead0",
    "Vorarlberg":       "fc5cfc23-3b75-4b7a-9d50-6e4ec018bd1a",
    "Österreich":       "3373d6d8-5fa2-4d5a-ac0b-790e69982f81",
}

CATEGORY_MAP = {
    "Gesamt":        "ksg",
    "Energie":       "ksg_energy",
    "Industrie":     "ksg_industry",
    "Verkehr":       "ksg_transport",
    "Gebäude":       "ksg_buildings",
    "Landwirtschaft":"ksg_agriculture",
    "Abfallwirtschaft": "ksg_waste",
    "F-Gase":        "ksg_fgases",
}

# "Energie & Industrie" is intentionally absent → rows will be skipped.
SKIP_SECTORS = {"Energie & Industrie"}

TYPE_MAP = {
    "Gesamt":                  "Gesamt",
    "KSG":                     "KSG",
    "EH":                      "EH",
    "EH Abgrenzung ab 2013":   "EH Abgrenzung ab 2013",
}

VALID_SOURCES = {SOURCE_STATES, SOURCE_AUSTRIA}

STATE_NAMES = set(REGION_MAP.keys()) - {"Österreich"}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def parse_date(date_str: str) -> str:
    """Convert "DD.MM.YYYY" → "YYYY-MM-DDT00:00:00"."""
    return datetime.strptime(date_str.strip(), "%d.%m.%Y").strftime("%Y-%m-%dT00:00:00")


def parse_value(raw: str) -> int:
    """Parse a German-locale decimal string (comma separator) and round to integer."""
    return round(float(raw.strip().replace(",", ".")))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(fmt: str, upload: bool = False) -> None:
    records = []
    skipped_na = 0
    skipped_sector = 0
    skipped_source = 0
    unknown_warnings: list[str] = []

    with open(CSV_FILE, encoding="windows-1252", newline="") as fh:
        reader = csv.DictReader(fh, delimiter=";")
        # Columns: Region, Schadstoff, Einheit, Sektor, Einteilung,
        #          Quelle, Datenstand, Jahr, Werte
        for row in reader:
            region_name  = row["Region"].strip()
            sektor        = row["Sektor"].strip()
            einteilung    = row["Einteilung"].strip()
            quelle        = row["Quelle"].strip()
            datenstand    = row["Datenstand"].strip()
            jahr          = row["Jahr"].strip()
            werte         = row["Werte"].strip()

            # Only process known sources
            if quelle not in VALID_SOURCES:
                skipped_source += 1
                continue

            # Skip combined energy+industry sector
            if sektor in SKIP_SECTORS:
                skipped_sector += 1
                continue

            # Skip missing values
            if werte == "NA":
                skipped_na += 1
                continue

            # Validate mappings
            if region_name not in REGION_MAP:
                unknown_warnings.append(f"Unknown region: {region_name!r}")
                continue
            if sektor not in CATEGORY_MAP:
                unknown_warnings.append(f"Unknown sector: {sektor!r}")
                continue
            if einteilung not in TYPE_MAP:
                unknown_warnings.append(f"Unknown type: {einteilung!r}")
                continue

            # Determine source label for DB
            source_label = SOURCE_STATES if region_name in STATE_NAMES else SOURCE_AUSTRIA

            records.append({
                "gas":      "THG",
                "source":   source_label,
                "year":     int(jahr),
                "update":   parse_date(datenstand),
                "value":    parse_value(werte),
                "country":  "AT",
                "region":   REGION_MAP[region_name],
                "category": CATEGORY_MAP[sektor],
                "type":     TYPE_MAP[einteilung],
            })

    # Print warnings
    if unknown_warnings:
        seen = set()
        for w in unknown_warnings:
            if w not in seen:
                print(f"  WARNING: {w}", file=sys.stderr)
                seen.add(w)

    # Summary by region
    from collections import Counter
    by_region = Counter(r["region"] for r in records)
    region_name_by_uuid = {v: k for k, v in REGION_MAP.items()}
    print(f"\nRecords by region:")
    for uuid, count in sorted(by_region.items(), key=lambda x: region_name_by_uuid[x[0]]):
        print(f"  {region_name_by_uuid[uuid]:<20} {count:>5}")
    print(f"\nTotal records:        {len(records):>6}")
    print(f"Skipped (NA value):   {skipped_na:>6}")
    print(f"Skipped (Energie & Industrie): {skipped_sector:>4}")
    print(f"Skipped (other source): {skipped_source:>4}")

    # Write output
    if fmt == "json":
        out_path = OUTPUT_DIR / "emissions_import.json"
        with open(out_path, "w", encoding="utf-8") as fh:
            json.dump(records, fh, ensure_ascii=False, indent=2)
    else:
        import csv as csv_mod
        out_path = OUTPUT_DIR / "emissions_import.csv"
        fieldnames = ["gas", "source", "year", "update", "value", "country", "region", "category", "type"]
        with open(out_path, "w", encoding="utf-8", newline="") as fh:
            writer = csv_mod.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(records)

    print(f"\nOutput written to: {out_path}")

    if upload:
        upload_to_directus(records)


def collect_ids_to_delete(session: "requests.Session", records: list[dict]) -> list[int]:
    """Return IDs of existing records whose (source, category, type, year, region) matches a record we're inserting."""
    new_keys = {
        (r["source"], r["category"], r["type"], r["year"], r["region"])
        for r in records
    }
    sources = {r["source"] for r in records}

    ids = []
    for source in sources:
        offset = 0
        limit = 500
        while True:
            resp = session.get(
                f"{DIRECTUS_URL}/items/emissions_data",
                params={
                    "filter[source][_eq]": source,
                    "fields[]": ["id", "source", "category", "type", "year", "region"],
                    "limit": limit,
                    "offset": offset,
                },
            )
            resp.raise_for_status()
            batch = resp.json()["data"]
            for r in batch:
                key = (r["source"], r["category"], r["type"], r["year"], r["region"])
                if key in new_keys:
                    ids.append(r["id"])
            if len(batch) < limit:
                break
            offset += limit
            print(f"  Scanned {offset} existing {source!r} records…", end="\r")
        print()
    return ids


def upload_to_directus(records: list[dict]) -> None:
    if requests is None:
        print("\nERROR: 'requests' library not installed. Run: pip install requests", file=sys.stderr)
        sys.exit(1)

    token = os.getenv("DIRECTUS_API_TOKEN")
    if not token:
        print("\nERROR: DIRECTUS_API_TOKEN not found. Make sure .env is present.", file=sys.stderr)
        sys.exit(1)

    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {token}", "Content-Type": "application/json"})

    # Only delete existing records whose exact (source, category, type, year, region) we're replacing.
    print("\nScanning existing records to find matches…")
    ids_to_delete = collect_ids_to_delete(session, records)
    print(f"  Found {len(ids_to_delete)} matching records to delete.")

    print(f"\nReady to delete {len(ids_to_delete)} existing records and insert {len(records)} new records.")
    answer = input("Proceed? [y/N] ").strip().lower()
    if answer != "y":
        print("Aborted.")
        return

    # Delete in batches
    batch_size = 200
    deleted = 0
    for i in range(0, len(ids_to_delete), batch_size):
        chunk = ids_to_delete[i : i + batch_size]
        resp = session.delete(f"{DIRECTUS_URL}/items/emissions_data", json=chunk)
        resp.raise_for_status()
        deleted += len(chunk)
        print(f"  Deleted {deleted}/{len(ids_to_delete)}…", end="\r")
    print(f"  Deleted {deleted} records.          ")

    # Insert in batches
    inserted = 0
    for i in range(0, len(records), batch_size):
        chunk = records[i : i + batch_size]
        resp = session.post(f"{DIRECTUS_URL}/items/emissions_data", json=chunk)
        resp.raise_for_status()
        inserted += len(chunk)
        print(f"  Inserted {inserted}/{len(records)}…", end="\r")
    print(f"  Inserted {inserted} records.          ")
    print("Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert BLI/OLI CSV for Directus import")
    parser.add_argument("--format", choices=["json", "csv"], default="json",
                        help="Output format (default: json)")
    parser.add_argument("--upload", action="store_true",
                        help="After generating JSON, interactively delete old records and insert new ones via Directus API")
    args = parser.parse_args()
    main(args.format, upload=args.upload)
