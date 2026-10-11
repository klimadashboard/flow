"""
LNG send-out into the German gas grid per terminal (GWh/day), from the ENTSOG
Transparency Platform (indicator "Physical Flow", entry direction, gas day).

Writes de/energy/fossil/lng_imports.csv on data.klimadashboard.org
(chart: lngImportsDE). Replaces the Datawrapper CSV that was uploaded by hand.

Columns: periodFrom (gas day start, "YYYY-MM-DD 06:00"), brunsbuettel,
wilhelmshaven, balticEnergyGate (GWh/d; empty = no flow reported).

ENTSOG points
  Brunsbüttel        LNG-00060
  Wilhelmshaven      LNG-00083   (reported from 2025-04; no earlier point exists)
  Baltic Energy Gate LNG-00075 (Lubmin, until 2024-04) + LNG-00079 (Mukran, from 2024-03).
                     Both are summed; they overlap in 03-04/2024 and carry different
                     flows. LNG-00079 is reported by two TSOs (DE-TSO-0001/0018) with the
                     same flow, so only one value per day is used.
  Stade LNG-00078 returns no data yet.

Without an existing output file the whole history (2022-12-22 ..) is fetched,
otherwise only the last REFRESH_DAYS are re-fetched and merged (values of recent
days get revised). The current gas day is skipped because it is incomplete.

Usage:
  python3 automated/lng-imports-de.py [--out /var/www/data/de/energy/fossil/lng_imports.csv]
Cron (daily): 15 8 * * * python3 /home/automated/lng-imports-de.py
"""
import argparse
import csv
import datetime as dt
import os
import time
import requests

API = "https://transparency.entsog.eu/api/v1/operationaldatas"
DEFAULT_OUT = "/var/www/data/de/energy/fossil/lng_imports.csv"
START = dt.date(2022, 12, 22)
REFRESH_DAYS = 21
COLUMNS = ["brunsbuettel", "wilhelmshaven", "balticEnergyGate"]
POINTS = {"LNG-00060": "brunsbuettel", "LNG-00083": "wilhelmshaven",
          "LNG-00075": "balticEnergyGate", "LNG-00079": "balticEnergyGate"}
PREFERRED_TSO = "DE-TSO-0001"


def fetch_point(point, start, end):
    """Return {day: GWh/d} for one ENTSOG point; chunked because the API is slow."""
    per_day = {}
    s = start
    while s <= end:
        e = min(s + dt.timedelta(days=179), end)
        for attempt in range(4):
            try:
                r = requests.get(API, params={
                    "pointKey": point, "indicator": "Physical Flow", "periodType": "day",
                    "from": s.isoformat(), "to": e.isoformat(), "directionKey": "entry",
                    "limit": -1, "timezone": "CET"}, timeout=90)
                if r.status_code == 404:  # "No result found" = empty window
                    break
                r.raise_for_status()
                for row in r.json()["operationaldatas"]:
                    if row["value"] is None:
                        continue
                    per_day.setdefault(row["periodFrom"][:10], {})[row["operatorKey"]] = row["value"] / 1e6
                break
            except requests.RequestException:
                if attempt == 3:
                    raise
                time.sleep(3)
        s = e + dt.timedelta(days=1)
    # one value per day: preferred TSO if present, else the max of what is reported
    return {d: v.get(PREFERRED_TSO, max(v.values())) for d, v in per_day.items()}


def read_existing(path):
    if not os.path.exists(path):
        return {}
    with open(path, newline="") as f:
        return {r["periodFrom"][:10]: r for r in csv.DictReader(f)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=DEFAULT_OUT)
    out = ap.parse_args().out

    existing = read_existing(out)
    last_complete = dt.date.today() - dt.timedelta(days=1)
    start = max(START, last_complete - dt.timedelta(days=REFRESH_DAYS)) if existing else START

    series = {c: {} for c in COLUMNS}
    for point, col in POINTS.items():
        for day, v in fetch_point(point, start, last_complete).items():
            series[col][day] = series[col].get(day, 0.0) + v

    days = sorted(set(existing) | {d for c in COLUMNS for d in series[c]})
    days = [d for d in days if START.isoformat() <= d <= last_complete.isoformat()]
    tmp = out + ".tmp"
    with open(tmp, "w", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(["periodFrom"] + COLUMNS)
        for d in days:
            row = [f"{d} 06:00"]
            for c in COLUMNS:
                if d in series[c]:
                    row.append(round(series[c][d], 6))
                else:  # keep older value outside the refresh window
                    row.append(existing.get(d, {}).get(c, ""))
            w.writerow(row)
    os.replace(tmp, out)
    print(f"{out}: {len(days)} days, {days[0]} .. {days[-1]}")


if __name__ == "__main__":
    main()
