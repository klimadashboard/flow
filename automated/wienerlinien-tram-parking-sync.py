"""
Wiener Linien "Falschparker" tram/bus disruption sync.

Polls the public Wiener Linien Realtime API (trafficInfoList), keeps only
disruptions caused by illegally parked cars ("Falschparker"), extracts and
geocodes the incident location and upserts the result into the Directus collection
`mobility_tram_parking`.

Dedup key: incident_id (= Wiener Linien trafficInfos.name with any trailing
"-F01"/"-F02"/... phase suffix stripped -- WL reissues the same real-world
incident under a new name each time its description moves to a new phase, so
all phases of one incident collapse onto a single row; see
canonical_incident_id/build_merged_record).

date_fix is deliberately not filled in from the API's time.resume (that's just
WL's rolling ETA, revised by opening a new phase) -- it's only set once the
whole incident stops appearing in the feed at all, which is the closest thing
to a confirmed "actually resolved" signal available; see resolve_absent_incidents.

Rows are tagged with `import_status`:
  - 'auto'     -> written/maintained by this script
  - 'reviewed' -> a human checked/corrected it; script never touches it again
  - 'manual'   -> pre-existing historical row; script never touches it
"""

import argparse
import json
import os
import re
import sys
import time
from pathlib import Path

import requests
from dotenv import load_dotenv
from geopy.extra.rate_limiter import RateLimiter
from geopy.geocoders import Nominatim
from shapely.geometry import Point, shape

try:
    from slack_logger import slack_log
except ImportError:
    def slack_log(msg, level="INFO"):
        print(f"[SLACK {level}] {msg}")

load_dotenv()

DIRECTUS_URL = os.getenv("DIRECTUS_API_URL")
DIRECTUS_TOKEN = os.getenv("DIRECTUS_API_TOKEN")
COLLECTION = "mobility_tram_parking"

HEADERS = {
    "Authorization": f"Bearer {DIRECTUS_TOKEN}",
    "Content-Type": "application/json",
}

WIENERLINIEN_TRAFFICINFO_URL = "http://www.wienerlinien.at/ogd_realtime/trafficInfoList"

STRASSENGRAPH_WFS_URL = (
    "https://data.wien.gv.at/daten/geo?service=WFS&request=GetFeature&version=1.1.0"
    "&typeName=ogdwien:STRASSENGRAPHOGD&outputFormat=json&srsName=EPSG:4326"
)
STRASSENKNOTEN_WFS_URL = (
    "https://data.wien.gv.at/daten/geo?service=WFS&request=GetFeature&version=1.1.0"
    "&typeName=ogdwien:STRASSENKNOTENOGD&outputFormat=json&srsName=EPSG:4326"
)
HALTESTELLEN_CSV_URL = "https://data.wien.gv.at/csv/wienerlinien-ogd-haltestellen.csv"
DISTRICTS_ASSET_URL = "https://base.klimadashboard.org/assets/bffb703f-85ba-4c75-b471-833da8f4c3ac"

CACHE_DIR = Path(__file__).parent / ".cache" / "tram_parking"
STRASSENGRAPH_CACHE = CACHE_DIR / "strassengraph.json"
STRASSENKNOTEN_CACHE = CACHE_DIR / "strassenknoten.json"
HALTESTELLEN_CACHE = CACHE_DIR / "haltestellen.csv"
DISTRICTS_CACHE = CACHE_DIR / "districts.json"
NOMINATIM_CACHE_FILE = CACHE_DIR / "nominatim_geocode_cache.json"

# GIP street-graph data is republished roughly every 2 months; the stops list
# roughly every 6h. Re-download once the cache is older than these.
STRASSENGRAPH_MAX_AGE_DAYS = 30
HALTESTELLEN_MAX_AGE_HOURS = 24
DISTRICTS_MAX_AGE_DAYS = 30

FALSCHPARKER_KEYWORDS = ["falschparker"]

DRY_RUN = "--dry-run" in sys.argv


def log(msg, level="INFO"):
    print(f"[{level}] {msg}")


# ---------------------------------------------------------------------------
# Caching helpers
# ---------------------------------------------------------------------------

def _is_stale(path, max_age_seconds):
    if not path.exists():
        return True
    return (time.time() - path.stat().st_mtime) > max_age_seconds


def _download(url, dest, headers=None):
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    r = requests.get(url, headers=headers, timeout=120)
    r.raise_for_status()
    dest.write_bytes(r.content)


def load_strassengraph():
    if _is_stale(STRASSENGRAPH_CACHE, STRASSENGRAPH_MAX_AGE_DAYS * 86400):
        log("Downloading Vienna Strassengraph (edges)...")
        _download(STRASSENGRAPH_WFS_URL, STRASSENGRAPH_CACHE)
    if _is_stale(STRASSENKNOTEN_CACHE, STRASSENGRAPH_MAX_AGE_DAYS * 86400):
        log("Downloading Vienna Strassenknoten (nodes)...")
        _download(STRASSENKNOTEN_WFS_URL, STRASSENKNOTEN_CACHE)

    graph = json.loads(STRASSENGRAPH_CACHE.read_text(encoding="utf-8"))["features"]
    knoten = json.loads(STRASSENKNOTEN_CACHE.read_text(encoding="utf-8"))["features"]
    return graph, knoten


def load_haltestellen():
    if _is_stale(HALTESTELLEN_CACHE, HALTESTELLEN_MAX_AGE_HOURS * 3600):
        log("Downloading Wiener Linien Haltestellen list...")
        _download(HALTESTELLEN_CSV_URL, HALTESTELLEN_CACHE)

    import csv
    coords = {}
    with open(HALTESTELLEN_CACHE, encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            name = (row.get("NAME") or "").strip().lower()
            try:
                lat, lon = float(row["WGS84_LAT"]), float(row["WGS84_LON"])
            except (KeyError, TypeError, ValueError):
                continue
            if name:
                coords[name] = (lat, lon)
    return coords


def load_districts():
    if _is_stale(DISTRICTS_CACHE, DISTRICTS_MAX_AGE_DAYS * 86400):
        log("Downloading Vienna district polygons from Directus asset...")
        _download(DISTRICTS_ASSET_URL, DISTRICTS_CACHE, headers=HEADERS)

    features = json.loads(DISTRICTS_CACHE.read_text(encoding="utf-8"))["features"]
    polygons = []
    for feat in features:
        try:
            polygons.append((shape(feat["geometry"]), feat["properties"].get("number")))
        except (KeyError, ValueError):
            continue
    return polygons


def load_nominatim_cache():
    if NOMINATIM_CACHE_FILE.exists():
        return json.loads(NOMINATIM_CACHE_FILE.read_text(encoding="utf-8"))
    return {}


def save_nominatim_cache(cache):
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    NOMINATIM_CACHE_FILE.write_text(json.dumps(cache, ensure_ascii=False), encoding="utf-8")


# ---------------------------------------------------------------------------
# Location extraction (ported from historische_daten_aufbereiten.py)
# ---------------------------------------------------------------------------

REGEX_HALTESTELLE = (
    r"im Haltestellenbereich (.*?)"
    r"(?:\s*\.|ist die Linie|sind die Linien|ist ein Betrieb|ist ein Fahrbetrieb|fährt die|wird die|kann die)"
)

ENDE_MUSTER = (
    r"(?:\s*\.|ist die Linie|sind die Linien|sind die Linie|werden die Linien|wird die Linie|"
    r"fährt die Linie|fahren die Linien|wird die Straßenbahnlinie|können die Linien|"
    r"ist ein Betrieb|ist ein Fahrbetrieb|fährt die|kann die|sind die Züge|werden die Züge|"
    r"wird die Autobuslinie|sind die Autobuslinien|werden die Busse der|ist derzeit ein Betrieb|"
    r"kann derzeit die Haltestelle|halten die Züge)"
)

REGEX_STUFEN = [
    rf"(?:im Bereich) (.*?){ENDE_MUSTER}",
    rf"(?:in der) (.*?){ENDE_MUSTER}",
    rf"(?:eines Falschparkers am) (.*?){ENDE_MUSTER}",
    rf"(?:Falschparkers(?: in)?) (.*?){ENDE_MUSTER}",
    rf"(?:in) (.*?){ENDE_MUSTER}",
]

KORREKTUREN = {
    "Hormayergasse": "Hormayrgasse",
    "Esterhazygasse": "Esterházygasse",
}

# Manual overrides for specific crossings the street-graph lookup gets wrong.
KREUZUNG_KORREKTUREN = {
    "Roschegasse # Pantucekgasse": (48.15311392, 16.45424858),
    "Pantucekgasse # Roschegasse": (48.15311392, 16.45424858),
}

STRASSENTYP_PATTERN = r"(gasse|stra(?:ß|ss)e|platz)\b"


def extract_location(description):
    if not description:
        return None, False

    match = re.search(REGEX_HALTESTELLE, description)
    if match:
        return match.group(1).strip(), True

    for pattern in REGEX_STUFEN:
        match = re.search(pattern, description)
        if match:
            return match.group(1).strip(), False

    return None, False


def korrigiere_ort(ort):
    if not ort:
        return ort
    for falsch, richtig in KORREKTUREN.items():
        ort = re.sub(falsch, richtig, ort, flags=re.IGNORECASE)
    return ort


def clean_false_hash(ort):
    if not ort or "#" not in ort:
        return ort
    parts = ort.split("#", 1)
    if len(parts) < 2:
        return ort
    nach_hash = parts[1].strip()
    if not re.search(STRASSENTYP_PATTERN, nach_hash, re.IGNORECASE):
        return parts[0].strip()
    return ort


def check_kreuzung_2(ort):
    if not ort or "#" in ort:
        return False
    matches = list(re.finditer(STRASSENTYP_PATTERN, ort, re.IGNORECASE))
    if len(matches) < 2:
        return False
    abstand = matches[1].start() - matches[0].end()
    return abstand <= 40


def insert_hash(ort):
    if not ort or "#" in ort:
        return ort
    pattern = rf"(.*?(?:gasse|stra(?:ß|ss)e|platz))\s+"
    match = re.match(pattern, ort, re.IGNORECASE)
    if match:
        return match.group(1) + " # " + ort[match.end():].strip()
    return ort


def kategorisiere_ort(ort, ist_haltestelle):
    if not ort:
        return None
    if ist_haltestelle:
        return "haltestelle"
    if "#" in ort:
        return "kreuzung"
    if ort.lower().endswith("platz") and not re.search(r"\d", ort):
        return "platz"
    if re.search(r"\d", ort):
        return "strasse_hausnummer"
    return "nur_strasse_ohne_nr"


def extract_and_categorize(description):
    """Returns (address, address_category) for a Falschparker description, or (None, None)."""
    ort, ist_haltestelle = extract_location(description)
    if not ort:
        return None, None

    ort = ort.strip()
    ort = korrigiere_ort(ort)
    ort = clean_false_hash(ort)

    is_kreuzung_2 = check_kreuzung_2(ort) and not ist_haltestelle
    if is_kreuzung_2:
        ort = insert_hash(ort)

    category = kategorisiere_ort(ort, ist_haltestelle)
    if is_kreuzung_2:
        category = "kreuzung_2"

    if category == "haltestelle":
        # Disruption text appends S-Bahn/U-Bahn markers ("S", "U", "SU") to stop
        # names that the canonical Haltestellen list doesn't carry.
        ort = re.sub(r"\s+[SU]+$", "", ort)

    if category == "nur_strasse_ohne_nr" and re.search(r"\bRichtung(en)?\b", ort, re.IGNORECASE):
        return None, None

    return ort, category


# ---------------------------------------------------------------------------
# Geocoding
# ---------------------------------------------------------------------------

def geocode_kreuzung(ort, graph, knoten):
    if not ort or "#" not in ort:
        return None, None
    strassen = [s.strip() for s in ort.split("#")]
    if len(strassen) != 2:
        return None, None
    strasse1, strasse2 = strassen

    def matching_node_ids(strasse):
        node_ids = set()
        for feat in graph:
            props = feat.get("properties", {})
            name = (props.get("FEATURENAME") or "")
            if strasse.lower() in name.lower():
                if props.get("NODEFROM_OBJECTID") is not None:
                    node_ids.add(props["NODEFROM_OBJECTID"])
                if props.get("NODETO_OBJECTID") is not None:
                    node_ids.add(props["NODETO_OBJECTID"])
        return node_ids

    nodes_1 = matching_node_ids(strasse1)
    nodes_2 = matching_node_ids(strasse2)
    kreuzung_ids = nodes_1 & nodes_2
    if not kreuzung_ids:
        return None, None

    for feat in knoten:
        if feat.get("properties", {}).get("GIP_OBJECTID") in kreuzung_ids:
            coords = feat["geometry"]["coordinates"]  # [lon, lat]
            return coords[1], coords[0]
    return None, None


def geocode_haltestelle(ort, haltestellen_coords):
    if not ort:
        return None, None
    return haltestellen_coords.get(ort.strip().lower(), (None, None))


def make_nominatim_geocoder():
    geolocator = Nominatim(user_agent="klimadashboard_tram_parking_sync", timeout=10)
    return RateLimiter(geolocator.geocode, min_delay_seconds=1.1, max_retries=3, error_wait_seconds=5.0)


def geocode_address(ort, geocode_fn, cache):
    if not ort:
        return None, None
    full_address = f"{ort}, Wien, Österreich"
    if full_address in cache:
        cached = cache[full_address]
        return tuple(cached) if cached else (None, None)

    try:
        location = geocode_fn(full_address)
    except Exception as e:
        log(f"Nominatim error for '{full_address}': {e}", level="WARNING")
        location = None

    result = (location.latitude, location.longitude) if location else None
    cache[full_address] = list(result) if result else None
    return result if result else (None, None)


def geocode(ort, category, graph, knoten, haltestellen_coords, nominatim_geocode, nominatim_cache):
    full_address = None
    if category in ("kreuzung", "kreuzung_2"):
        if ort in KREUZUNG_KORREKTUREN:
            lat, lon = KREUZUNG_KORREKTUREN[ort]
        else:
            lat, lon = geocode_kreuzung(ort, graph, knoten)
    elif category == "haltestelle":
        lat, lon = geocode_haltestelle(ort, haltestellen_coords)
    elif category in ("strasse_hausnummer", "platz"):
        lat, lon = geocode_address(ort, nominatim_geocode, nominatim_cache)
        full_address = f"{ort}, Wien, Österreich" if lat is not None else None
    else:
        lat, lon = None, None
    return lat, lon, full_address


def _same_value(a, b):
    """Compares two datetime-ish strings by instant, not by string representation
    -- Directus and the Wiener Linien API serialize the same timestamp differently
    (e.g. trailing 'Z' vs '+0200' offset)."""
    if a == b:
        return True
    if not a or not b:
        return False
    try:
        from datetime import datetime
        return datetime.fromisoformat(a.replace("Z", "+00:00")) == datetime.fromisoformat(b.replace("Z", "+00:00"))
    except ValueError:
        return False


def lookup_district(lat, lon, district_polygons):
    if lat is None or lon is None:
        return None
    point = Point(lon, lat)
    for polygon, number in district_polygons:
        if polygon.contains(point):
            return number
    return None


# ---------------------------------------------------------------------------
# Wiener Linien API
# ---------------------------------------------------------------------------

def fetch_traffic_infos():
    params = {"name": ["stoerunglang", "stoerungkurz"]}
    r = requests.get(WIENERLINIEN_TRAFFICINFO_URL, params=params, timeout=30,
                      headers={"Accept": "application/json"})
    r.raise_for_status()
    data = r.json()

    message = data.get("data", {}).get("message", {})
    if message.get("messageCode") not in (1, None):
        raise RuntimeError(f"Wiener Linien API error {message.get('messageCode')}: {message.get('value')}")

    return data.get("data", {}).get("trafficInfos", []) or []


def is_falschparker(entry):
    text = f"{entry.get('title') or ''} {entry.get('description') or ''}".lower()
    return any(keyword in text for keyword in FALSCHPARKER_KEYWORDS)


# Wiener Linien re-issues the same real-world incident under a new `name` each
# time its description moves to a new "phase" (e.g. from the specific
# "Falschparker at <address>" text to a generic "irregular intervals"
# follow-up) -- the old phase is closed out with status='resolved' and a new
# one appears with suffix "-F01", "-F02", etc. All phases sharing a base name
# are the same physical incident and must collapse onto one Directus row.
PHASE_SUFFIX_PATTERN = re.compile(r"-F(\d+)$")


def canonical_incident_id(name):
    return PHASE_SUFFIX_PATTERN.sub("", name)


def phase_number(name):
    match = PHASE_SUFFIX_PATTERN.search(name)
    return int(match.group(1)) if match else 0


def _parse_dt(value):
    if not value:
        return None
    try:
        from datetime import datetime
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def group_by_incident(entries):
    groups = {}
    for entry in entries:
        groups.setdefault(canonical_incident_id(entry["name"]), []).append(entry)
    for group in groups.values():
        group.sort(key=lambda e: phase_number(e["name"]))
    return groups


def build_merged_record(canonical_id, phases):
    """Merges every known phase of one real-world incident into a single record.

    time.resume/time.end are Wiener Linien's rolling ETA, not a confirmed fix --
    each new phase just revises the estimate. So date_end always reflects the
    latest phase's estimate, but date_fix is deliberately left unset here; it's
    only filled in once the whole chain stops appearing in the feed at all (see
    resolve_absent_incidents), which is the closest thing to a confirmed "it's
    actually over" signal available from this API.
    """
    latest = phases[-1]
    time_values = [(p.get("time") or {}) for p in phases]

    starts = [dt for dt in (_parse_dt(t.get("start")) for t in time_values) if dt]
    ends = [(dt, t.get("end")) for t, dt in ((t, _parse_dt(t.get("end"))) for t in time_values) if dt]

    date_start = min(starts).isoformat() if starts else (time_values[0].get("start") if time_values else None)
    date_end = max(ends, key=lambda pair: pair[0])[1] if ends else (latest.get("time") or {}).get("end")

    lines = sorted({l for p in phases for l in (p.get("relatedLines") or [])})
    stops = sorted({str(s) for p in phases for s in (p.get("relatedStops") or [])})

    address = category = None
    for phase in phases:  # earliest phase first -- keeps the first specific location ever reported
        candidate_addr, candidate_cat = extract_and_categorize(phase.get("description"))
        if candidate_addr:
            address, category = candidate_addr, candidate_cat
            break

    return {
        "incident_id": canonical_id,
        "title": latest.get("title"),
        "description": latest.get("description"),
        "date_start": date_start,
        "date_end": date_end,
        "lines": ",".join(lines),
        "stops": ",".join(stops),
        "_address_hint": address,
        "_category_hint": category,
    }


# ---------------------------------------------------------------------------
# Directus sync
# ---------------------------------------------------------------------------

def get_open_auto_incidents():
    """All rows this script still considers unresolved (import_status='auto',
    date_fix still null) -- used to detect incidents that quietly stopped
    appearing in the feed at all, which is treated as confirmation they're over."""
    rows = []
    offset = 0
    while True:
        r = requests.get(
            f"{DIRECTUS_URL}/items/{COLLECTION}",
            headers=HEADERS,
            params={
                "filter[import_status][_eq]": "auto",
                "filter[date_fix][_null]": "true",
                "fields": "id,incident_id,date_end",
                "limit": 1000,
                "offset": offset,
            },
            timeout=60,
        )
        r.raise_for_status()
        batch = r.json().get("data", [])
        rows.extend(batch)
        if len(batch) < 1000:
            break
        offset += 1000
    return rows


def resolve_absent_incidents(current_canonical_ids):
    resolved = 0
    for row in get_open_auto_incidents():
        if row["incident_id"] in current_canonical_ids:
            continue
        if update_record(row["id"], {"date_fix": row.get("date_end")}):
            resolved += 1
    return resolved


def get_existing_by_incident_id(incident_ids):
    existing = {}
    for i in range(0, len(incident_ids), 100):
        chunk = incident_ids[i:i + 100]
        r = requests.get(
            f"{DIRECTUS_URL}/items/{COLLECTION}",
            headers=HEADERS,
            params={
                "filter[incident_id][_in]": ",".join(chunk),
                "fields": "id,incident_id,import_status,lat,lon,date_end,date_fix,description",
                "limit": -1,
            },
            timeout=60,
        )
        r.raise_for_status()
        for item in r.json().get("data", []):
            existing[item["incident_id"]] = item
    return existing


def insert_records(records):
    if not records or DRY_RUN:
        return len(records) if DRY_RUN else 0
    r = requests.post(f"{DIRECTUS_URL}/items/{COLLECTION}", headers=HEADERS, json=records, timeout=120)
    if r.status_code not in (200, 201, 204):
        log(f"Insert failed: {r.status_code} - {r.text[:300]}", level="ERROR")
        return 0
    return len(records)


def update_record(record_id, payload):
    if DRY_RUN:
        return True
    r = requests.patch(f"{DIRECTUS_URL}/items/{COLLECTION}/{record_id}", headers=HEADERS, json=payload, timeout=60)
    if r.status_code not in (200, 204):
        log(f"Update failed for id={record_id}: {r.status_code} - {r.text[:300]}", level="ERROR")
        return False
    return True


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    start_time = time.time()
    mode = " (dry run)" if DRY_RUN else ""
    slack_log(f"🅿️ Start Wiener Linien Falschparker-Sync{mode}", level="INFO")

    if not DIRECTUS_URL or not DIRECTUS_TOKEN:
        slack_log("❌ DIRECTUS_API_URL / DIRECTUS_API_TOKEN nicht gesetzt.", level="ERROR")
        return

    try:
        traffic_infos = fetch_traffic_infos()
    except Exception as e:
        slack_log(f"❌ Fehler beim Abrufen der Wiener Linien API: {e}", level="ERROR")
        return

    falschparker_entries = [e for e in traffic_infos if is_falschparker(e)]
    log(f"Fetched {len(traffic_infos)} disruptions, {len(falschparker_entries)} are Falschparker-related.")

    if not falschparker_entries:
        slack_log("ℹ️ Keine Falschparker-Störungen in dieser Abfrage.", level="INFO")
        return

    graph, knoten = load_strassengraph()
    haltestellen_coords = load_haltestellen()
    district_polygons = load_districts()
    nominatim_geocode = make_nominatim_geocoder()
    nominatim_cache = load_nominatim_cache()

    groups = group_by_incident(falschparker_entries)
    multi_phase_groups = sum(1 for phases in groups.values() if len(phases) > 1)

    records = []
    geocode_failures = 0
    for canonical_id, phases in groups.items():
        record = build_merged_record(canonical_id, phases)
        address, category = record.pop("_address_hint"), record.pop("_category_hint")
        lat, lon, address_full = geocode(
            address, category, graph, knoten, haltestellen_coords, nominatim_geocode, nominatim_cache
        )
        if address and lat is None:
            geocode_failures += 1

        record.update({
            "address": address,
            "address_category": category,
            "address_full": address_full,
            "lat": lat,
            "lon": lon,
            "district": lookup_district(lat, lon, district_polygons),
        })
        records.append(record)

    save_nominatim_cache(nominatim_cache)

    existing = get_existing_by_incident_id([r["incident_id"] for r in records])

    to_insert = []
    to_update = []
    skipped = 0

    for record in records:
        existing_row = existing.get(record["incident_id"])
        if existing_row is None:
            record["import_status"] = "auto"
            to_insert.append(record)
            continue

        if existing_row.get("import_status") != "auto":
            skipped += 1
            continue

        payload = {}
        for field in ("date_end", "description"):
            if not _same_value(existing_row.get(field), record[field]):
                payload[field] = record[field]
        if existing_row.get("date_fix") is not None:
            # It reappeared in the feed after we'd confirmed it resolved by
            # absence -- treat it as reopened rather than leaving a stale fix time.
            payload["date_fix"] = None
        if existing_row.get("lat") is None and record["lat"] is not None:
            payload.update({
                "address": record["address"],
                "address_category": record["address_category"],
                "address_full": record["address_full"],
                "lat": record["lat"],
                "lon": record["lon"],
                "district": record["district"],
            })
        if payload:
            to_update.append((existing_row["id"], payload))

    inserted = insert_records(to_insert)
    updated = sum(1 for record_id, payload in to_update if update_record(record_id, payload))
    resolved_by_absence = resolve_absent_incidents({r["incident_id"] for r in records})

    duration = round(time.time() - start_time)
    summary = (
        f"✅ Falschparker-Sync abgeschlossen in {duration}s{mode}\n"
        f"- Abgerufen: {len(traffic_infos)}\n"
        f"- Falschparker-Meldungen erkannt: {len(falschparker_entries)} ({len(groups)} Vorfälle, "
        f"{multi_phase_groups} davon aus mehreren Phasen zusammengeführt)\n"
        f"- Neu eingefügt: {inserted}\n"
        f"- Aktualisiert: {updated}\n"
        f"- Als abgeschlossen bestätigt (nicht mehr in der Störungsliste): {resolved_by_absence}\n"
        f"- Übersprungen (bereits geprüft/manuell): {skipped}\n"
        f"- Geocoding fehlgeschlagen: {geocode_failures}"
    )
    log(summary)
    slack_log(summary, level="SUCCESS")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        slack_log(f"❌ Unerwarteter Fehler im Falschparker-Sync: {e}", level="ERROR")
        raise
