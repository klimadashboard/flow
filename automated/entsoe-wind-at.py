import os
import datetime
import requests
import pandas as pd
from entsoe import EntsoePandasClient
from dotenv import load_dotenv
import logging

# Setup Logging
logging.basicConfig(level=logging.INFO)

print("📦 Starte Skript...")

# --- Lade Umgebungsvariablen ---
print("🔑 Lade .env Variablen...")
load_dotenv()
directus_url = os.getenv("DIRECTUS_API_URL")
directus_token = os.getenv("DIRECTUS_API_TOKEN")
entsoe_token = os.getenv("ENTSOE_API_TOKEN")

if not directus_url or not directus_token or not entsoe_token:
    print("❌ Fehlende Umgebungsvariablen! Bitte .env prüfen.")
    exit(1)

client = EntsoePandasClient(api_key=entsoe_token)
client.session.timeout = 10

headers = {
    "Authorization": f"Bearer {directus_token}",
    "Content-Type": "application/json"
}

# --- Zeitraum: 730 Tage für verlässliche Jahresproduktion ---
print("📆 Lade Daten für 730 Tage...")
end_date = datetime.date.today()
start_date = end_date - datetime.timedelta(days=730)
start = pd.Timestamp(str(start_date), tz="Europe/Vienna")
end = pd.Timestamp(str(end_date), tz="Europe/Vienna")

# --- Lade Daten in Monatsblöcken ---
print("📡 Lade Winddaten in Monatsblöcken von ENTSO-E...")
chunks = []
current = start

while current < end:
    next_chunk = min(current + pd.Timedelta(days=30), end)
    print(f"🔹 Hole {current.date()} bis {next_chunk.date()}")

    try:
        part = client.query_generation(country_code="AT", start=current, end=next_chunk)
        part_df = pd.DataFrame(part["Wind Onshore"]["Actual Aggregated"] * 0.25 / 10**6)
        part_df = part_df.rename(columns={"Actual Aggregated": "Wind"})
        chunks.append(part_df)
    except Exception as e:
        print(f"❌ Fehler bei {current.date()} bis {next_chunk.date()}: {e}")

    current = next_chunk

if not chunks:
    print("❌ Keine Daten erhalten, Abbruch.")
    exit(1)

# --- Daten zusammenführen und vorbereiten ---
print("📊 Verarbeite Zeitreihe...")
df = pd.concat(chunks)
# Remove duplicate timestamps at chunk boundaries before summing
df = df[~df.index.duplicated(keep="first")]
df.index.name = "DateTime"
df = df.resample("D").sum()
df["Jahresproduktion"] = df["Wind"].rolling("365D").sum()

# Zeitzonenhandling in UTC für Directus
df.index = pd.to_datetime(df.index)
df.index = df.index.tz_convert("UTC")

# Behalte nur die letzten 365 Tage mit vollständiger Jahresproduktion
cutoff = df.index.max() - pd.Timedelta(days=365)
df = df.loc[df.index >= cutoff]

print(f"🧮 {len(df)} Tage (inkl. Jahresproduktion) vorbereitet zur Synchronisierung")

# --- Bestehende Directus-Einträge abrufen ---
print("🌐 Lade vorhandene Directus-Einträge...")
try:
    existing_resp = requests.get(
        f"{directus_url}/items/ee_produktion?filter[Country][_eq]=AT&filter[Type][_contains]=windkraft"
        f"&limit=-1&fields=id,DateTime",
        headers=headers
    )
    existing_resp.raise_for_status()
except Exception as e:
    print(f"❌ Fehler beim Laden von bestehenden Directus-Daten: {e}")
    exit(1)

# Nur Datumsteil als Schlüssel (YYYY-MM-DD)
existing_data = {
    pd.to_datetime(item["DateTime"]).date(): item["id"]
    for item in existing_resp.json().get("data", [])
}
print(f"📁 {len(existing_data)} vorhandene Tages-Einträge geladen")

# --- Update oder Insert pro Tag ---
print("🔁 Starte Abgleich mit Directus...")
for timestamp, row in df.iterrows():
    ts_date = timestamp.date()
    iso_timestamp = timestamp.isoformat()

    print(f"🔸 Bearbeite {ts_date}...")

    payload = {
        "DateTime": iso_timestamp,
        "Country": "AT",
        "Type": ["windkraft"],
        "unit": "TWh",
        "value": round(row["Wind"], 6),
        "Jahresproduktion": round(row["Jahresproduktion"], 6) if pd.notnull(row["Jahresproduktion"]) else None
    }

    try:
        if ts_date in existing_data:
            entry_id = existing_data[ts_date]
            res = requests.patch(f"{directus_url}/items/ee_produktion/{entry_id}", json=payload, headers=headers)
            action = "🔁 Aktualisiert"
        else:
            res = requests.post(f"{directus_url}/items/ee_produktion", json=payload, headers=headers)
            action = "➕ Hinzugefügt"

        if res.status_code not in [200, 201]:
            print(f"❌ Fehler bei {ts_date}: {res.status_code} - {res.text}")
        else:
            print(f"{action}: {ts_date}")

    except Exception as e:
        print(f"❌ Netzwerkfehler bei {ts_date}: {e}")

print("✅ ✔️ Synchronisierung abgeschlossen.")
