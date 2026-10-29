"""
Ladesäulenregister Sync (Bundesnetzagentur).

Syncs the public charging stations list of the Bundesnetzagentur into the
Directus table mobility_charging_stations (one row per Ladeeinrichtung).

Source: "Liste der Ladesäulen" (CSV and XLSX), linked from
https://www.bundesnetzagentur.de/DE/Fachthemen/ElektrizitaetundGas/E-Mobilitaet/start.html
Licence CC BY 4.0, attribution "Bundesnetzagentur.de". Updated about monthly;
the CSV and XLSX are not always published on the same day, so the newer of
the two is used. The list only contains stations currently in operation, so a
station that disappears gets shutdown_date = date of the first list it is
missing from (rows are never deleted).

The list has no municipality key. Each station is assigned to a municipality
in our regions table (region = regions.id, region_code = 8-digit AGS):
  1. point-in-polygon on the municipality outlines, if that municipality lies
     in the Kreis the register names                 -> "coordinates"
  2. otherwise the register's Ort within that Kreis  -> "address"
  3. otherwise a unique postcode match in that Kreis -> "postcode"
  4. otherwise the polygon hit, even though its Kreis disagrees
                                                     -> "coordinates_mismatch"
  5. points outside all outlines (coast, borders): nearest municipality
     within NEAREST_MAX_DEG                          -> "coordinates_nearest"

Usage:
    python bnetza-ladesaeulen-sync.py            # import if BNetzA published a newer list
    python bnetza-ladesaeulen-sync.py --force    # import the current list even if already imported
    python bnetza-ladesaeulen-sync.py --file X   # import a local CSV/XLSX (implies --force)

Requires: pandas, openpyxl, shapely, requests, python-dotenv
Cron (daily, skips quietly when nothing new): 30 6 * * * python3 /home/automated/bnetza-ladesaeulen-sync.py
"""

import io
import os
import re
import sys
import time
from datetime import datetime

import numpy as np
import pandas as pd
import requests
from dotenv import load_dotenv
from shapely import STRtree, points
from shapely.geometry import shape

try:
    from slack_logger import slack_log
except ImportError:
    def slack_log(msg, level="INFO"):
        print(f"[SLACK {level}] {msg}")

load_dotenv()

DIRECTUS_URL = os.getenv("DIRECTUS_API_URL")
DIRECTUS_TOKEN = os.getenv("DIRECTUS_API_TOKEN")
TABLE_NAME = "mobility_charging_stations"
BATCH_SIZE = int(os.getenv("DIRECTUS_BATCH_SIZE", 1000))
FORCE = "--force" in sys.argv or "--file" in sys.argv
LOCAL_FILE = sys.argv[sys.argv.index("--file") + 1] if "--file" in sys.argv else None

SOURCE_PAGE = "https://www.bundesnetzagentur.de/DE/Fachthemen/ElektrizitaetundGas/E-Mobilitaet/start.html"
LIST_LINK_RE = re.compile(
    r"https://data\.bundesnetzagentur\.de/[^\"']*Ladesaeulenregister_BNetzA_(\d{4}-\d{2}-\d{2})\.(csv|xlsx)"
)
USER_AGENT = "Mozilla/5.0 (compatible; klimadashboard-flow; +https://klimadashboard.org)"
HEADER_ROWS = 10  # disclaimer lines + "1. Ladepunkt" group row above the column header
NEAREST_MAX_DEG = 0.03

HEADERS = {
    "Authorization": f"Bearer {DIRECTUS_TOKEN}",
    "Content-Type": "application/json",
}

DISTRICT_PREFIX_RE = re.compile(
    r"^(Kreisfreie Stadt|Landkreis|Kreis|Stadtkreis|Regionalverband|Städteregion)\s+"
)
CITY_DISTRICT_PREFIXES = ("Kreisfreie Stadt", "Stadtkreis")


def log(msg, level="INFO"):
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} [ladesaeulen-sync] {level}: {msg}")


# ---------------------------------------------------------------------------
# Source
# ---------------------------------------------------------------------------

def find_latest_list():
    """Return (url, date) of the newest list linked on the BNetzA page (CSV preferred on a tie)."""
    r = requests.get(SOURCE_PAGE, headers={"User-Agent": USER_AGENT}, timeout=60)
    r.raise_for_status()
    links = {(m.group(1), m.group(2)): m.group(0) for m in LIST_LINK_RE.finditer(r.text)}
    if not links:
        raise RuntimeError(f"No Ladesäulenregister CSV/XLSX link found on {SOURCE_PAGE}")
    date, ext = max(links, key=lambda k: (k[0], k[1] == "csv"))
    return links[(date, ext)], date


def read_list(content, is_xlsx):
    """Parse the CSV/XLSX into a DataFrame of strings plus the list date ("Letzte Aktualisierung")."""
    if is_xlsx:
        raw = pd.read_excel(io.BytesIO(content), header=None, dtype=str, nrows=HEADER_ROWS)
        df = pd.read_excel(io.BytesIO(content), header=HEADER_ROWS, dtype=object)
        # Dates arrive as datetimes, numbers as numbers: normalise to the CSV's text format
        df["Inbetriebnahmedatum"] = pd.to_datetime(df["Inbetriebnahmedatum"], errors="coerce").dt.strftime("%d.%m.%Y")
        df = df.astype(object).where(df.notna(), None)
        df = df.map(lambda v: None if v is None else (str(int(v)) if isinstance(v, float) and v.is_integer() else str(v)))
    else:
        text = content.decode("utf-8-sig")
        raw = pd.read_csv(io.StringIO(text), sep=";", header=None, dtype=str, nrows=HEADER_ROWS, on_bad_lines="skip")
        df = pd.read_csv(io.StringIO(text), sep=";", skiprows=HEADER_ROWS, dtype=str, low_memory=False)
        df = df.astype(object).where(df.notna(), None)

    list_date = None
    for cell in raw[0].dropna():
        m = re.search(r"Letzte Aktualisierung vom:\s*(\d{2})\.(\d{2})\.(\d{4})", str(cell))
        if m:
            list_date = f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
    df = df[df["Ladeeinrichtungs-ID"].notna()].copy()
    return df, list_date


def to_float(v):
    if v is None:
        return None
    try:
        return float(str(v).replace(",", "."))
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Region mapping
# ---------------------------------------------------------------------------

def fetch_regions(layer, fields):
    rows, offset = [], 0
    while True:
        r = requests.get(
            f"{DIRECTUS_URL}/items/regions",
            headers=HEADERS,
            params={"filter[country][_eq]": "DE", "filter[layer][_eq]": layer,
                    "fields": fields, "limit": 2000, "offset": offset},
            timeout=300,
        )
        r.raise_for_status()
        data = r.json()["data"]
        rows += data
        if len(data) < 2000:
            return rows
        offset += 2000


def norm_name(s):
    return re.sub(r"[\s.\-/]+", "", str(s)).lower()


class RegionMatcher:
    def __init__(self):
        log("Loading municipality outlines and districts from Directus...")
        munis = fetch_regions("municipality", "id,code_short,name,postcodes,outline")
        districts = fetch_regions("district", "code,name")
        log(f"Loaded {len(munis):,} municipalities, {len(districts):,} districts.")

        with_outline = [m for m in munis if m.get("outline") and m.get("code_short")]
        self.ids = np.array([m["id"] for m in with_outline])
        self.codes = np.array([m["code_short"] for m in with_outline])
        self.tree = STRtree([shape(m["outline"]) for m in with_outline])

        self.munis_by_district = {}
        for m in munis:
            if m.get("code_short"):
                self.munis_by_district.setdefault(m["code_short"][:5], []).append(m)

        # District names repeat for a kreisfreie Stadt and the Landkreis around it
        # (Ansbach, Fürth, ...). The kreisfreie Stadt is the one with a single municipality.
        self.districts_by_name = {}
        for d in districts:
            self.districts_by_name.setdefault(d["name"], []).append(d["code"])
        self._district_cache = {}

    def district_code(self, register_name):
        if not register_name:
            return None
        if register_name in self._district_cache:
            return self._district_cache[register_name]
        candidates = self.districts_by_name.get(DISTRICT_PREFIX_RE.sub("", register_name).strip(), [])
        if len(candidates) > 1:
            is_city = register_name.startswith(CITY_DISTRICT_PREFIXES)
            candidates = [c for c in candidates
                          if (len(self.munis_by_district.get(c, [])) == 1) == is_city] or candidates
        code = candidates[0] if len(candidates) == 1 else None
        self._district_cache[register_name] = code
        return code

    def by_address(self, district, city, postcode):
        munis = self.munis_by_district.get(district, [])
        if city:
            c = norm_name(city)
            hits = [m for m in munis if norm_name(m["name"]) == c]
            if not hits:
                # Ort often carries a suffix ("Neumarkt i.d.OPf.", "Erlangen-Bruck")
                hits = [m for m in munis if c.startswith(norm_name(m["name"]))]
                hits = sorted(hits, key=lambda m: -len(m["name"]))[:1]
            if len(hits) == 1:
                return hits[0], "address"
        if postcode:
            hits = [m for m in munis if postcode in (m.get("postcodes") or [])]
            if len(hits) == 1:
                return hits[0], "postcode"
        return None, None

    def assign(self, df):
        lat = df["Breitengrad"].map(to_float)
        lon = df["Längengrad"].map(to_float)
        valid = (lat.notna() & lon.notna()).values
        pts = points(lon.fillna(0).values, lat.fillna(0).values)

        pip_idx = np.full(len(df), -1)
        hit = self.tree.query(pts[valid], predicate="within")
        valid_pos = np.flatnonzero(valid)
        pip_idx[valid_pos[hit[0]]] = hit[1]

        outside = np.flatnonzero(valid & (pip_idx < 0))
        near_idx = np.full(len(df), -1)
        if len(outside):
            near = self.tree.query_nearest(pts[outside], max_distance=NEAREST_MAX_DEG)
            near_idx[outside[near[0]]] = near[1]

        region, region_code, source = [], [], []
        for i, (district_name, city, postcode) in enumerate(
            zip(df["Kreis/kreisfreie Stadt"], df["Ort"], df["Postleitzahl"])
        ):
            district = self.district_code(district_name)
            j = pip_idx[i]
            if j >= 0 and (district is None or self.codes[j][:5] == district):
                region.append(self.ids[j]); region_code.append(self.codes[j]); source.append("coordinates")
                continue
            m, how = self.by_address(district, city, postcode) if district else (None, None)
            if m:
                region.append(m["id"]); region_code.append(m["code_short"]); source.append(how)
            elif j >= 0:
                region.append(self.ids[j]); region_code.append(self.codes[j]); source.append("coordinates_mismatch")
            elif near_idx[i] >= 0:
                k = near_idx[i]
                region.append(self.ids[k]); region_code.append(self.codes[k]); source.append("coordinates_nearest")
            else:
                region.append(None); region_code.append(None); source.append(None)
        return region, region_code, source


# ---------------------------------------------------------------------------
# Map to Directus schema
# ---------------------------------------------------------------------------

TYPE_CODES = {"Normalladeeinrichtung": "normal", "Schnellladeeinrichtung": "fast"}


def build_records(df, matcher):
    region, region_code, source = matcher.assign(df)
    commissioning = pd.to_datetime(df["Inbetriebnahmedatum"], format="%d.%m.%Y", errors="coerce")

    records = []
    for i, row in enumerate(df.to_dict("records")):
        connectors = []
        for n in range(1, 7):
            if row.get(f"Steckertypen{n}"):
                connectors.append({
                    "type": row[f"Steckertypen{n}"],
                    "power_kw": to_float(row.get(f"Nennleistung Stecker{n}")),
                    "evse_id": row.get(f"EVSE-ID{n}"),
                })
        opening = row.get("Öffnungszeiten")
        if opening == "247":
            opening = "24/7"
        elif row.get("Öffnungszeiten: Wochentage"):
            opening = f"{opening} ({row['Öffnungszeiten: Wochentage']}: {row.get('Öffnungszeiten: Tageszeiten') or ''})"
        points_ = row.get("Anzahl Ladepunkte")
        records.append({
            "id": str(row["Ladeeinrichtungs-ID"]).strip(),
            "operator": row.get("Betreiber"),
            "display_name": row.get("Anzeigename (Karte)"),
            "status": row.get("Status"),
            "type": TYPE_CODES.get(row.get("Art der Ladeeinrichtung"), row.get("Art der Ladeeinrichtung")),
            "charging_points": int(float(points_)) if points_ else None,
            "power_kw": to_float(row.get("Nennleistung Ladeeinrichtung [kW]")),
            "commissioning_date": commissioning.iloc[i].strftime("%Y-%m-%d") if pd.notna(commissioning.iloc[i]) else None,
            "street": row.get("Straße"),
            "house_number": row.get("Hausnummer"),
            "postcode": row.get("Postleitzahl"),
            "city": row.get("Ort"),
            "district": row.get("Kreis/kreisfreie Stadt"),
            "federal_state": row.get("Bundesland"),
            "lat": to_float(row.get("Breitengrad")),
            "lon": to_float(row.get("Längengrad")),
            "region": region[i],
            "region_code": region_code[i],
            "region_source": source[i],
            "connectors": connectors,
            "parking": row.get("Informationen zum Parkraum"),
            "payment": row.get("Bezahlsysteme"),
            "opening_hours": opening,
            "country": "DE",
        })

    n_before = len(records)
    records = list({r["id"]: r for r in records}.values())
    if len(records) < n_before:
        log(f"Dropped {n_before - len(records):,} duplicate IDs")
    return records


# ---------------------------------------------------------------------------
# Directus API helpers
# ---------------------------------------------------------------------------

def get_all_directus_meta():
    """Return {id: {first_seen, shutdown_date}} for every record in Directus."""
    meta, page_size, offset = {}, 10000, 0
    while True:
        r = requests.get(
            f"{DIRECTUS_URL}/items/{TABLE_NAME}",
            headers=HEADERS,
            params={"fields": "id,first_seen,shutdown_date", "limit": page_size, "offset": offset},
            timeout=120,
        )
        r.raise_for_status()
        data = r.json().get("data", [])
        for item in data:
            meta[item["id"]] = item
        if len(data) < page_size:
            return meta
        offset += page_size


def get_last_imported_date():
    r = requests.get(
        f"{DIRECTUS_URL}/items/{TABLE_NAME}",
        headers=HEADERS,
        params={"aggregate[max]": "last_seen"},
        timeout=60,
    )
    r.raise_for_status()
    data = r.json().get("data") or [{}]
    return (data[0].get("max") or {}).get("last_seen")


def batch_request(method, payload, max_retries=3):
    if not payload:
        return 0
    for attempt in range(max_retries):
        try:
            r = requests.request(method, f"{DIRECTUS_URL}/items/{TABLE_NAME}", json=payload, headers=HEADERS, timeout=180)
            if r.status_code in [200, 201, 204]:
                return len(payload) if isinstance(payload, list) else len(payload["keys"])
            if r.status_code == 503:
                time.sleep(2 ** attempt)
                continue
            log(f"Batch {method} error: {r.status_code} - {r.text[:200]}", level="ERROR")
            return 0
        except Exception as e:
            log(f"Batch {method} exception (attempt {attempt + 1}): {e}", level="WARNING")
            time.sleep(2 ** attempt)
    return 0


def sync_to_directus(records, list_date):
    existing = get_all_directus_meta()
    log(f"Directus has {len(existing):,} records.")

    to_insert, to_update = [], []
    for r in records:
        r["last_seen"] = list_date
        r["shutdown_date"] = None
        if r["id"] in existing:
            to_update.append(r)
        else:
            r["first_seen"] = list_date
            to_insert.append(r)

    listed = {r["id"] for r in records}
    vanished = [i for i, m in existing.items() if i not in listed and not m.get("shutdown_date")]
    log(f"{len(to_insert):,} new, {len(to_update):,} still listed, {len(vanished):,} no longer listed.")

    inserted = updated = shut_down = 0
    for i in range(0, len(to_insert), BATCH_SIZE):
        inserted += batch_request("POST", to_insert[i:i + BATCH_SIZE])
        log(f"Inserted {inserted:,}/{len(to_insert):,}")
    for i in range(0, len(to_update), BATCH_SIZE):
        updated += batch_request("PATCH", to_update[i:i + BATCH_SIZE])
        log(f"Updated {updated:,}/{len(to_update):,}")
    for i in range(0, len(vanished), BATCH_SIZE):
        shut_down += batch_request("PATCH", {"keys": vanished[i:i + BATCH_SIZE], "data": {"shutdown_date": list_date}})
    if vanished:
        log(f"Marked {shut_down:,} stations as shut down ({list_date}).")
    return inserted, updated, shut_down


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    start_time = time.time()

    if LOCAL_FILE:
        url, file_date = LOCAL_FILE, None
        with open(LOCAL_FILE, "rb") as fh:
            content = fh.read()
    else:
        url, file_date = find_latest_list()
        last_imported = get_last_imported_date()
        if not FORCE and last_imported and file_date <= last_imported:
            log(f"Latest list ({file_date}) already imported (last_seen {last_imported}). Nothing to do.")
            return
        content = None

    slack_log(f"Ladesäulen-Sync gestartet ({os.path.basename(url)}).", level="INFO")
    try:
        if content is None:
            log(f"Downloading {url} ...")
            r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=600)
            r.raise_for_status()
            content = r.content

        df, list_date = read_list(content, url.lower().endswith(".xlsx"))
        list_date = list_date or file_date
        if not list_date:
            raise RuntimeError("Could not determine the list date ('Letzte Aktualisierung vom').")
        log(f"Read {len(df):,} stations, list date {list_date}.")

        records = build_records(df, RegionMatcher())
        sources = pd.Series([r["region_source"] for r in records]).value_counts(dropna=False).to_dict()
        log(f"Region assignment: {sources}")

        inserted, updated, shut_down = sync_to_directus(records, list_date)
        unassigned = sum(1 for r in records if not r["region"])
        points_total = sum(r["charging_points"] or 0 for r in records)
        duration = round(time.time() - start_time)
        failed = len(records) - inserted - updated
        slack_log(
            f"Ladesäulen-Sync abgeschlossen in {duration}s (Stand {list_date})\n"
            f"- Ladeeinrichtungen: {len(records):,} ({points_total:,} Ladepunkte)\n"
            f"- Eingefuegt: {inserted:,}\n"
            f"- Aktualisiert: {updated:,}\n"
            f"- Nicht mehr gelistet: {shut_down:,}\n"
            f"- Ohne Gemeinde: {unassigned:,}"
            + (f"\n- Fehlgeschlagen: {failed:,}" if failed else ""),
            level="ERROR" if failed else "SUCCESS",
        )
        log(f"Done in {duration}s. Inserted {inserted:,}, updated {updated:,}, shut down {shut_down:,}, failed {failed:,}.")
        if failed:
            sys.exit(1)

    except Exception as e:
        slack_log(f"Fehler beim Ladesäulen-Sync: {e}", level="ERROR")
        log(f"Error: {e}", level="ERROR")
        raise


if __name__ == "__main__":
    main()
