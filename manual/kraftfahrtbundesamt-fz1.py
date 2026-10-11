import argparse
import pandas as pd
import json

# ----------------------------------------
# Config
# ----------------------------------------
# Usage: python3 manual/kraftfahrtbundesamt-fz1.py fz1_2026.xlsx 2026
# FZ 1 is published by the KBA once a year (Bestand am 1. Januar), e.g.
# https://www.kba.de/SharedDocs/Downloads/DE/Statistik/Fahrzeuge/FZ1/fz1_2026.xlsx?__blob=publicationFile&v=1
parser = argparse.ArgumentParser()
parser.add_argument("input_file", nargs="?", default="fz1_2025.xlsx")
parser.add_argument("period", nargs="?", default="2025")
args = parser.parse_args()

input_file = args.input_file
sheet_name = "FZ1.2"
period = args.period
output_file = f"mobility_cars_{period}.json"
source = "Kraftfahrt-Bundesamt (Tabelle FZ 1.2)"
country = "DE"

# ----------------------------------------
# Load and clean data
# ----------------------------------------
df = pd.read_excel(input_file, sheet_name=sheet_name, skiprows=8)

# Extract region code from first column
df["region"] = df.iloc[:, 3].astype(str).str.extract(r"^(\d{5})")
df = df[df["region"].notna()]

# Rename Excel columns to clean names
df = df.rename(columns={
    "Benzin": "Benzin",
    "Diesel": "Diesel",
    "Gas\n(einschl.\nbivalent)": "Gas",
    "Hybrid \ninsgesamt": "Hybrid_insgesamt",
    "darunter\nPlug-in-Hybrid": "Plug-in-Hybrid",
    "Elektro (BEV)": "Elektro",
    "sonstige": "Sonstige Kraftstoffarten"
})

# Compute Hybrid (ohne Plug-in)
df["Hybrid (ohne Plug-in)"] = df["Hybrid_insgesamt"] - df["Plug-in-Hybrid"]

# Compute Insgesamt
df["Insgesamt"] = df[
    ["Benzin", "Diesel", "Gas", "Elektro", "Plug-in-Hybrid", "Hybrid (ohne Plug-in)", "Sonstige Kraftstoffarten"]
].sum(axis=1).astype(int)

# Final mapping: JSON category name → column name in df
export_map = {
    "Benzin": "Benzin",
    "Diesel": "Diesel",
    "Gas": "Gas",
    "Elektro": "Elektro",
    "Plug-in-Hybrid": "Plug-in-Hybrid",
    "Hybrid (ohne Plug-in)": "Hybrid (ohne Plug-in)",
    "Sonstige Kraftstoffarten": "Sonstige Kraftstoffarten",
    "Insgesamt": "Insgesamt"
}

# ----------------------------------------
# Convert to final JSON format
# ----------------------------------------
records = []
for category, column in export_map.items():
    for region, value in zip(df["region"], df[column]):
        if pd.notna(value):
            records.append({
                "region": region,
                "category": category,
                "value": int(value),
                "period": period,
                "source": source,
                "country": country
            })

# ----------------------------------------
# Export
# ----------------------------------------
with open(output_file, "w", encoding="utf-8") as f:
    json.dump(records, f, ensure_ascii=False, indent=2)

print(f"✅ Exported {len(records)} rows to {output_file}")
