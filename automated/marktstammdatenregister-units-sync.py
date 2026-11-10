"""
Marktstammdatenregister generic unit sync (biomass, hydro, ...).

One config entry per carrier describes which open_mastr tables to read and how
to map them to a Directus table that mirrors energy_solar_units, so that the
get-renewables-growth endpoint works with ?table=<directus table>.

Invoked by marktstammdatenregister-daily-sync.py (with --skip-download),
or run standalone.

Usage:
    python marktstammdatenregister-units-sync.py --carrier biomass,hydro                  # incremental
    python marktstammdatenregister-units-sync.py --carrier biomass --full                 # full reload + prune
    python marktstammdatenregister-units-sync.py --carrier hydro --rebuild                # clear table, re-insert
    python marktstammdatenregister-units-sync.py --carrier biomass,hydro --skip-download  # data already in sqlite

--full also deletes rows from Directus whose MaStR number no longer exists in
the source tables (deleted/deactivated units).
"""

import os
import sys
import time
import argparse
import requests
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
from open_mastr import Mastr
from open_mastr.utils.constants import BULK_INCLUDE_TABLES_MAP

try:
    from slack_logger import slack_log
except ImportError:
    def slack_log(msg, level="INFO"):
        print(f"[SLACK {level}] {msg}")

load_dotenv()

DIRECTUS_URL = (os.getenv("DIRECTUS_API_URL") or "").rstrip("/")
DIRECTUS_TOKEN = os.getenv("DIRECTUS_API_TOKEN")
BATCH_SIZE = int(os.getenv("DIRECTUS_BATCH_SIZE", 1000))
UPDATE_DAYS_BACK = int(os.getenv("UPDATE_DAYS_BACK", 10))

HEADERS = {
    "Authorization": f"Bearer {DIRECTUS_TOKEN}",
    "Content-Type": "application/json",
}

STATUS_CODES = {
    "In Betrieb": "35",
    "In Planung": "31",
    "Vorübergehend stillgelegt": "37",
    "Endgültig stillgelegt": "38",
}

# Fields shared by all unit tables: Directus field -> MaStR column
COMMON_COLUMNS = {
    "name": "NameStromerzeugungseinheit",
    "status": "EinheitBetriebsstatus",
    "energy_source": "Energietraeger",
    "commissioning_date": "Inbetriebnahmedatum",
    "planned_commissioning_date": "GeplantesInbetriebnahmedatum",
    "shutdown_date": "DatumEndgueltigeStilllegung",
    "last_update": "DatumLetzteAktualisierung",
    "power_kw": "Bruttoleistung",
    "net_power_kw": "Nettonennleistung",
    "federal_state": "Bundesland",
    "district": "Landkreis",
    "municipality": "Gemeinde",
    "region": "Gemeindeschluessel",
    "postcode": "Postleitzahl",
    "lat": "Breitengrad",
    "lon": "Laengengrad",
    "grid_operator_review_status": "NetzbetreiberpruefungStatus",
    "eeg_id": "EegMastrNummer",
}

DATE_FIELDS = {"commissioning_date", "planned_commissioning_date", "shutdown_date", "last_update"}
FLOAT_FIELDS = {"power_kw", "net_power_kw", "lat", "lon"}

# Per-carrier config.
#   download: open_mastr data types needed
#   sources:  list of (sqlite table, optional SQL WHERE for that table, fixed values)
#   columns:  extra Directus field -> MaStR column
#   type:     function(row) -> value for the `type` field
CARRIERS = {
    "biomass": {
        "table": "energy_biomass_units",
        "label": "Biomasse",
        "download": ["biomass", "gsgk"],
        "sources": [
            ("biomass_extended", None),
            # Klärschlamm units are registered with the geothermal/mine gas units
            ("gsgk_extended", "Energietraeger = 'Klärschlamm'"),
        ],
        "columns": {
            "fuel": "Hauptbrennstoff",
            "technology": "Technologie",
            "chp_id": "KwkMastrNummer",
        },
        "type": lambda row: "Klärschlamm" if row.get("Energietraeger") == "Klärschlamm" else row.get("Biomasseart"),
    },
    "hydro": {
        "table": "energy_hydro_units",
        "label": "Wasserkraft",
        "download": ["hydro"],
        # Pumped storage is in the storage tables, not here.
        "sources": [("hydro_extended", None)],
        "columns": {
            "inflow_type": "ArtDesZuflusses",
            "plant_name": "NameKraftwerk",
        },
        "type": lambda row: row.get("ArtDerWasserkraftanlage"),
    },
}


def log(msg, level="INFO", carrier="units"):
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} [{carrier}-sync] {level}: {msg}")


def only_unit_files(data_types):
    """Skip the EEG files (Anlagen*), we only read the Einheiten* tables."""
    for key in data_types:
        BULK_INCLUDE_TABLES_MAP[key] = [f for f in BULK_INCLUDE_TABLES_MAP[key] if f.startswith("einheiten")]


# ---------------------------------------------------------------------------
# Loading + mapping
# ---------------------------------------------------------------------------

def load_units(db, cfg, full, threshold):
    frames = []
    for table, where in cfg["sources"]:
        clauses = [where] if where else []
        if not full:
            clauses.append(f"DatumLetzteAktualisierung >= '{threshold}'")
        sql = f"SELECT * FROM {table}" + (f" WHERE {' AND '.join(clauses)}" if clauses else "")
        frames.append(pd.read_sql(sql=sql, con=db.engine))
    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    return df


def clean(field, value):
    if value is None or (not isinstance(value, str) and pd.isna(value)):
        return None
    if field in DATE_FIELDS:
        if isinstance(value, (pd.Timestamp, datetime)):
            return value.strftime("%Y-%m-%d")
        return str(value)[:10] or None
    if field in FLOAT_FIELDS:
        try:
            return float(str(value).replace(",", "."))
        except (TypeError, ValueError):
            return None
    if field == "status":
        return STATUS_CODES.get(str(value), str(value))
    s = str(value).strip()
    return s or None


def map_records(df, cfg):
    columns = {**COMMON_COLUMNS, **cfg["columns"]}
    records = []
    for row in df.to_dict("records"):
        mastr_id = row.get("EinheitMastrNummer")
        if not mastr_id:
            continue
        rec = {"id": str(mastr_id).strip(), "country": "DE"}
        for field, col in columns.items():
            rec[field] = clean(field, row.get(col))
        rec["type"] = clean("type", cfg["type"](row))
        records.append(rec)
    return records


# ---------------------------------------------------------------------------
# Directus helpers
# ---------------------------------------------------------------------------

def fetch_all_ids(table):
    ids, offset, page = [], 0, 10000
    while True:
        r = requests.get(
            f"{DIRECTUS_URL}/items/{table}", headers=HEADERS,
            params={"fields": "id", "limit": page, "offset": offset}, timeout=120,
        )
        r.raise_for_status()
        data = r.json().get("data", [])
        ids.extend(d["id"] for d in data)
        if len(data) < page:
            return ids
        offset += page


def write_batch(table, batch, method, max_retries=3):
    if not batch:
        return 0
    for attempt in range(max_retries):
        try:
            r = requests.request(method, f"{DIRECTUS_URL}/items/{table}", json=batch, headers=HEADERS, timeout=180)
            if r.status_code in (200, 201, 204):
                return len(batch)
            if r.status_code >= 500:
                time.sleep(2 ** attempt)
                continue
            raise RuntimeError(f"{method} {table}: {r.status_code} {r.text[:300]}")
        except requests.RequestException as e:
            if attempt == max_retries - 1:
                raise
            time.sleep(2 ** attempt)
    raise RuntimeError(f"{method} {table}: giving up after {max_retries} attempts")


def delete_ids(table, ids):
    deleted = 0
    for i in range(0, len(ids), BATCH_SIZE):
        deleted += write_batch(table, ids[i:i + BATCH_SIZE], "DELETE")
    return deleted


def sync(table, records, existing, carrier):
    inserted = updated = 0
    to_insert = [r for r in records if r["id"] not in existing]
    to_update = [r for r in records if r["id"] in existing]
    for i in range(0, len(to_insert), BATCH_SIZE):
        inserted += write_batch(table, to_insert[i:i + BATCH_SIZE], "POST")
    for i in range(0, len(to_update), BATCH_SIZE):
        updated += write_batch(table, to_update[i:i + BATCH_SIZE], "PATCH")
    log(f"{inserted} inserted, {updated} updated", carrier=carrier)
    return inserted, updated


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_carrier(db, name, mode, threshold):
    cfg = CARRIERS[name]
    table = cfg["table"]
    full = mode in ("full", "rebuild")
    start = time.time()

    df = load_units(db, cfg, full, threshold)
    records = map_records(df, cfg)
    # One MaStR number may appear twice if a unit sits in two sources; keep the last.
    records = list({r["id"]: r for r in records}.values())
    log(f"{len(records)} units loaded ({mode})", carrier=name)

    deleted = 0
    existing = set(fetch_all_ids(table))
    if mode == "rebuild":
        deleted = delete_ids(table, list(existing))
        existing = set()
    elif mode == "full" and records:
        stale = sorted(existing - {r["id"] for r in records})
        if len(stale) > 0.1 * len(existing):
            # Guard against a broken/partial download wiping the table.
            raise RuntimeError(f"{len(stale)} of {len(existing)} units would be pruned; aborting")
        if stale:
            deleted = delete_ids(table, stale)
            existing -= set(stale)
            log(f"{deleted} units no longer in MaStR deleted", carrier=name)

    inserted, updated = sync(table, records, existing, name) if records else (0, 0)
    duration = round(time.time() - start)
    slack_log(
        f"MaStR {cfg['label']}-Sync abgeschlossen in {duration}s\n"
        f"- Modus: {mode}\n- Geladen: {len(records)}\n- Eingefuegt: {inserted}\n"
        f"- Aktualisiert: {updated}\n- Geloescht: {deleted}",
        level="SUCCESS",
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--carrier", default="biomass,hydro", help=f"comma list of {', '.join(CARRIERS)}")
    parser.add_argument("--full", action="store_true")
    parser.add_argument("--rebuild", action="store_true")
    parser.add_argument("--skip-download", action="store_true")
    args = parser.parse_args()

    carriers = [c.strip() for c in args.carrier.split(",") if c.strip()]
    unknown = [c for c in carriers if c not in CARRIERS]
    if unknown:
        parser.error(f"unknown carrier(s): {', '.join(unknown)}")

    mode = "rebuild" if args.rebuild else ("full" if args.full else "incremental")
    threshold = (datetime.today() - timedelta(days=UPDATE_DAYS_BACK)).strftime("%Y-%m-%d")

    db = Mastr()
    if not args.skip_download:
        data_types = sorted({d for c in carriers for d in CARRIERS[c]["download"]})
        only_unit_files(data_types)
        log(f"Downloading MaStR data: {data_types}")
        db.download(data=data_types, bulk_cleansing=True)

    failed = []
    for c in carriers:
        try:
            run_carrier(db, c, mode, threshold)
        except Exception as e:
            failed.append(c)
            log(f"Error: {e}", level="ERROR", carrier=c)
            slack_log(f"Fehler beim MaStR {CARRIERS[c]['label']}-Sync: {e}", level="ERROR")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
