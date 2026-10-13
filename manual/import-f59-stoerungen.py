"""
Fills gaps in `mobility_tram_parking` from the public f59.at disruption archive
(https://f59.at/stoerungen/index_alle.php -- every Wiener Linien disruption
since 2026-01-01, recorded from the same WL OpenData feed).

f59 is the same source the historical rows came from (the f59 xlsx used by
import-falschparker-2026-gap.py matches this page: same incident ids, same
start times, Verkehrsaufnahme identical for 755 of 767 incidents).

For every Falschparker incident in the given date range:
  - missing in Directus            -> inserted (import_status 'manual', like the gap import)
  - existing 'auto'/'reviewed' row -> description/title/date_fix/date_end replaced by
                                      f59's (complete) values, address re-extracted and
                                      geocoded from the full text, import_status -> 'reviewed'
                                      so the live sync never touches it again
  - existing 'manual' row          -> left alone

date_fix = f59 "Verkehrsaufnahme" (nulled if it precedes the start). Times on
the page are naive Europe/Vienna and converted to UTC.

Address extraction / geocoding / district lookup reuse the live sync's code.
An existing geocoded location is never replaced by a non-geocoded one.

Run on the server (needs the street graph cache and .env):
    python3 manual/import-f59-stoerungen.py --from 2026-08-27 --to 2026-10-02 --dry-run
    python3 manual/import-f59-stoerungen.py --from 2026-08-27 --to 2026-10-02
The dry run writes a preview CSV next to this script.
"""

import argparse
import csv
import html
import importlib.util
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv

load_dotenv()

DIRECTUS_URL = os.getenv("DIRECTUS_API_URL")
DIRECTUS_TOKEN = os.getenv("DIRECTUS_API_TOKEN")
COLLECTION = "mobility_tram_parking"
HEADERS = {"Authorization": f"Bearer {DIRECTUS_TOKEN}", "Content-Type": "application/json"}

F59_URL = "https://f59.at/stoerungen/index_alle.php"
VIENNA_TZ = ZoneInfo("Europe/Vienna")
UTC = ZoneInfo("UTC")

_sync_spec = importlib.util.spec_from_file_location(
    "sync_mod", Path(__file__).parent.parent / "automated" / "wienerlinien-tram-parking-sync.py"
)
sync_mod = importlib.util.module_from_spec(_sync_spec)
_sync_spec.loader.exec_module(sync_mod)


def log(msg):
    print(f"[import-f59] {msg}")


# ---------------------------------------------------------------------------
# f59 page
# ---------------------------------------------------------------------------

ROW_PATTERN = re.compile(r"<tr onclick=\"window.location='detail\.php\?id=([^']+)';\"[^>]*>(.*?)</tr>", re.S)


def fetch_f59_rows():
    r = requests.get(F59_URL, timeout=180, headers={"User-Agent": "klimadashboard.org data import"})
    r.raise_for_status()
    rows = []
    for m in ROW_PATTERN.finditer(r.text):
        # cells are <td valign="top"> without closing tags
        cells = [
            html.unescape(re.sub(r"<[^>]+>", "", c)).replace("\xa0", " ").strip()
            for c in re.split(r'<td valign="top">', m.group(2))[1:]
        ]
        if len(cells) < 6:
            continue
        rows.append({
            "id": m.group(1), "title": cells[0], "description": cells[1],
            "start": cells[2] or None, "resume": cells[3] or None, "end": cells[4] or None,
            "lines": cells[5],
        })
    return rows


# Phase suffixes can stack ("I20260304-0013-F01-F01"); handled here directly so
# the import doesn't depend on which version of the live sync is deployed.
PHASE_SUFFIXES = re.compile(r"(?:-F\d+)+$")


def canonical_id(name):
    return PHASE_SUFFIXES.sub("", name)


def phase_key(name):
    match = PHASE_SUFFIXES.search(name)
    return tuple(int(n) for n in re.findall(r"-F(\d+)", match.group(0))) if match else ()


def local_to_utc(value):
    if not value:
        return None
    return datetime.fromisoformat(value).replace(tzinfo=VIENNA_TZ).astimezone(UTC)


def iso(dt):
    return dt.isoformat().replace("+00:00", "Z") if dt else None


def merge_chain(chain):
    """f59 mostly lists one row per incident; an occasional -F0N phase row is
    merged the same way the live sync / gap import do it."""
    chain = sorted(chain, key=lambda r: phase_key(r["id"]))
    starts = [local_to_utc(r["start"]) for r in chain if r["start"]]
    with_end = [r for r in chain if r["end"]]
    end_row = max(with_end, key=lambda r: r["end"]) if with_end else chain[-1]
    date_start = min(starts) if starts else None
    date_fix = local_to_utc(end_row["resume"]) or next((local_to_utc(r["resume"]) for r in chain if r["resume"]), None)
    if date_fix and date_start and date_fix < date_start:
        date_fix = None
    lines = sorted({l.strip() for r in chain for l in r["lines"].split(",") if l.strip()})
    description = "\n".join(dict.fromkeys(r["description"] for r in chain if r["description"]))
    return {
        "title": chain[-1]["title"],
        "description": description,
        "date_start": date_start,
        "date_fix": date_fix,
        "date_end": local_to_utc(end_row["end"]),
        "lines": ",".join(lines),
    }


# ---------------------------------------------------------------------------
# Directus
# ---------------------------------------------------------------------------

def fetch_existing(incident_ids):
    existing = {}
    ids = sorted(incident_ids)
    for i in range(0, len(ids), 100):
        chunk = ids[i:i + 100]
        variants = chunk + [f"bms_{x}" for x in chunk]
        r = requests.get(
            f"{DIRECTUS_URL}/items/{COLLECTION}", headers=HEADERS, timeout=120,
            params={"filter[incident_id][_in]": ",".join(variants), "limit": -1,
                    "fields": "id,incident_id,import_status,address,address_category,lat,lon,district,date_fix"},
        )
        r.raise_for_status()
        for row in r.json()["data"]:
            existing[re.sub(r"^bms_", "", row["incident_id"])] = row
    return existing


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="date_from", required=True, help="YYYY-MM-DD (Vienna, inclusive)")
    ap.add_argument("--to", dest="date_to", required=True, help="YYYY-MM-DD (Vienna, inclusive)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if not DIRECTUS_URL or not DIRECTUS_TOKEN:
        log("ERROR: DIRECTUS_API_URL / DIRECTUS_API_TOKEN not set")
        sys.exit(1)

    rows = fetch_f59_rows()
    log(f"f59 page: {len(rows)} disruptions")
    chains = {}
    for r in rows:
        if not sync_mod.is_falschparker(r):
            continue
        if not (r["start"] and args.date_from <= r["start"][:10] <= args.date_to):
            continue
        chains.setdefault(canonical_id(r["id"]), []).append(r)
    log(f"Falschparker incidents {args.date_from}..{args.date_to}: {len(chains)}")

    existing = fetch_existing(chains.keys())

    graph, knoten = sync_mod.load_strassengraph()
    haltestellen = sync_mod.load_haltestellen()
    districts = sync_mod.load_districts()
    nominatim = sync_mod.make_nominatim_geocoder()
    nominatim_cache = sync_mod.load_nominatim_cache()

    inserts, updates, skipped_manual, preview = [], [], 0, []
    for incident_id, chain in sorted(chains.items()):
        rec = merge_chain(chain)
        current = existing.get(incident_id)
        if current and current["import_status"] == "manual":
            skipped_manual += 1
            continue

        address, category = sync_mod.extract_and_categorize(rec["description"])
        lat, lon, address_full = sync_mod.geocode(
            address, category, graph, knoten, haltestellen, nominatim, nominatim_cache
        ) if address else (None, None, None)
        location = {
            "address": address, "address_category": category, "address_full": address_full,
            "lat": lat, "lon": lon, "district": sync_mod.lookup_district(lat, lon, districts),
        }
        # never trade a geocoded location for an ungeocoded one
        if current and current.get("lat") is not None and lat is None:
            location = {}

        fields = {
            "title": rec["title"], "description": rec["description"],
            "date_start": iso(rec["date_start"]), "date_fix": iso(rec["date_fix"]),
            "date_end": iso(rec["date_end"]), "lines": rec["lines"], **location,
        }
        action = "update" if current else "insert"
        if current:
            updates.append((current["id"], {**fields, "import_status": "reviewed"}))
        else:
            inserts.append({"incident_id": incident_id, **fields, "import_status": "manual"})
        preview.append({
            "action": action, "incident_id": incident_id, "directus_id": current["id"] if current else "",
            "date_start": fields["date_start"], "date_fix": fields["date_fix"],
            "minutes_to_fix": round((rec["date_fix"] - rec["date_start"]).total_seconds() / 60)
            if rec["date_fix"] and rec["date_start"] else "",
            "address_before": current.get("address") if current else "",
            "address": fields.get("address", current.get("address") if current else None),
            "category": fields.get("address_category", current.get("address_category") if current else None),
            "geocoded": (fields.get("lat") if "lat" in fields else (current or {}).get("lat")) is not None,
        })

    sync_mod.save_nominatim_cache(nominatim_cache)
    log(f"to insert: {len(inserts)}, to update: {len(updates)}, skipped (manual): {skipped_manual}")

    out = Path(__file__).with_name(f"f59-import-preview-{args.date_from}_{args.date_to}.csv")
    with open(out, "w", newline="", encoding="utf-8-sig") as fh:
        w = csv.DictWriter(fh, fieldnames=list(preview[0].keys()) if preview else ["action"], delimiter=";")
        w.writeheader()
        w.writerows(preview)
    log(f"preview written to {out}")

    if args.dry_run:
        log("DRY RUN -- nothing written")
        return

    if inserts:
        r = requests.post(f"{DIRECTUS_URL}/items/{COLLECTION}", headers=HEADERS, json=inserts, timeout=180)
        if r.status_code not in (200, 201, 204):
            log(f"INSERT FAILED: {r.status_code} {r.text[:300]}")
            sys.exit(1)
        log(f"inserted {len(inserts)}")
    if updates:
        payload = [{"id": rid, **fields} for rid, fields in updates]
        r = requests.patch(f"{DIRECTUS_URL}/items/{COLLECTION}", headers=HEADERS, json=payload, timeout=180)
        if r.status_code not in (200, 204):
            log(f"UPDATE FAILED: {r.status_code} {r.text[:300]}")
            sys.exit(1)
        log(f"updated {len(updates)}")


if __name__ == "__main__":
    main()
