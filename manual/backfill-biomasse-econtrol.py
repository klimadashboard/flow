"""
One-off backfill of Biomasse (e-control "Summe Biogene Brennstoffe") for
ee_produktion, covering the gap Jan 2024 - Dec 2025 that automated/ee-econtrol.py
did not fill because it only reads the current year's block of the e-control file.

Same conventions as ee-econtrol.py: GWh -> TWh, DateTime = first of the month
AFTER the data month, Jahresproduktion = rolling 12 months on a gapless index.
Existing rows (same DateTime + Type) are never touched.

Usage:
    python3 manual/backfill-biomasse-econtrol.py            # dry run
    python3 manual/backfill-biomasse-econtrol.py --upload   # write to Directus
"""
import os
import sys
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
BASE_URL = os.getenv("DIRECTUS_API_URL")
TOKEN = os.getenv("DIRECTUS_API_TOKEN")
if not BASE_URL or not TOKEN:
    raise EnvironmentError("Missing Directus credentials")
headers = {"Authorization": f"Bearer {TOKEN}"}

URL = "https://www.e-control.at/documents/1785851/13428376/MoMeGes_Bilanz_2026.xlsx"
LABEL = "Summe \nBiogene\nBrenn-\nstoffe"
YEARS = {2024, 2025}

resp = requests.get(URL, timeout=60)
resp.raise_for_status()
path = "MoMeGes_Bilanz_2026.xlsx"
with open(path, "wb") as f:
    f.write(resp.content)

df = pd.read_excel(path, sheet_name="Erzeugung", header=None)
os.remove(path)
col = df.iloc[7].tolist().index(LABEL)

# Each year block starts at an "Einheit" row with unit GWh followed by 12 month rows.
# Dates are taken by position, because the source has typos in single date cells
# (e.g. 2028-03-01 inside the 2024 block).
rows = []
for r in df.index[(df[0] == "Einheit") & (df[col] == "GWh")]:
    first = pd.to_datetime(df.iat[r + 1, 0], errors="coerce")
    if pd.isnull(first) or first.year not in YEARS:
        continue
    for m in range(12):
        v = pd.to_numeric(df.iat[r + 1 + m, col], errors="coerce")
        if pd.notnull(v):
            data_month = pd.Timestamp(year=first.year, month=m + 1, day=1)
            rows.append({"Date": data_month + pd.DateOffset(months=1), "value": v / 1000.0})
new = pd.DataFrame(rows)
print(f"Source rows: {len(new)} ({new['Date'].min().date()} .. {new['Date'].max().date()})")

res = requests.get(
    f"{BASE_URL}/items/ee_produktion",
    headers=headers,
    params={"filter[Country][_eq]": "AT", "filter[Type][_eq]": "biomasse",
            "limit": -1, "fields": "DateTime,value"},
    timeout=60,
)
res.raise_for_status()
existing = pd.DataFrame(res.json().get("data", res.json()))
existing["Date"] = pd.to_datetime(existing["DateTime"]).dt.normalize()
existing["value"] = pd.to_numeric(existing["value"])

combined = (pd.concat([existing[["Date", "value"]], new], ignore_index=True)
            .drop_duplicates(subset="Date").sort_values("Date"))
jahr = combined.set_index("Date")["value"].asfreq("MS").rolling(12, min_periods=12).sum()

payloads = []
for _, row in new[~new["Date"].isin(existing["Date"])].sort_values("Date").iterrows():
    j = jahr.get(row["Date"])
    payloads.append({
        "DateTime": row["Date"].strftime("%Y-%m-%dT00:00:00"),
        "value": round(float(row["value"]), 6),
        "Jahresproduktion": round(float(j), 6) if pd.notnull(j) else None,
        "Country": "AT",
        "Type": ["biomasse"],
        "unit": "TWh",
    })

for p in payloads:
    print(p["DateTime"][:10], p["value"], p["Jahresproduktion"])
print(f"{len(payloads)} new rows, {len(new) - len(payloads)} already present")

if "--upload" not in sys.argv:
    print("Dry run. Re-run with --upload to write.")
    sys.exit(0)

ok = fail = 0
for p in payloads:
    r = requests.post(f"{BASE_URL}/items/ee_produktion", headers=headers, json=p, timeout=60)
    if r.status_code in (200, 201):
        ok += 1
    else:
        fail += 1
        print("FAILED", p["DateTime"], r.text)
print(f"Uploaded {ok}, failed {fail}")
