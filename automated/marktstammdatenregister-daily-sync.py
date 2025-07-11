import os
import zipfile
import shutil
import xml.etree.ElementTree as ET
from pathlib import Path
import subprocess
import requests
import math
import time
from datetime import datetime, timedelta
from dateutil.parser import parse as parse_date
from dotenv import load_dotenv
from slack_logger import slack_log

# Load environment variables
load_dotenv()

# Configuration
DIRECTUS_URL = os.getenv("DIRECTUS_API_URL")
DIRECTUS_TOKEN = os.getenv("DIRECTUS_API_TOKEN")
LOG_FILE = "/var/log/marktstammdatenregister-daily-sync.log"
ZIP_NAME = "Gesamtdatenexport.zip"
EXTRACT_DIR = "marktstamm_tmp"
TABLE_NAME = "energy_solar_units"
ENTRY_TAG = "EinheitSolar"
BATCH_SIZE = int(os.getenv("DIRECTUS_BATCH_SIZE", 2000))
UPDATE_DAYS_BACK = int(os.getenv("UPDATE_DAYS_BACK", 7))
HEADERS = {
    "Authorization": f"Bearer {DIRECTUS_TOKEN}",
    "Content-Type": "application/json"
}

def log(msg, level="INFO"):
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {level}: {msg}")

THRESHOLD_DATE = datetime.today() - timedelta(days=UPDATE_DAYS_BACK)
today = datetime.today().strftime("%Y%m%d")
ZIP_URL = f"https://download.marktstammdatenregister.de/Gesamtdatenexport_{today}_25.1.zip"
MAX_DOWNLOAD_RETRIES = 5
DOWNLOAD_RETRY_DELAY = 600  # 10 minutes

def download_zip():
    if os.path.exists(ZIP_NAME):
        os.remove(ZIP_NAME)
    if os.path.exists(EXTRACT_DIR):
        shutil.rmtree(EXTRACT_DIR)

    log("🚀 Lade ZIP-Datei mit aria2c...")

    for attempt in range(1, MAX_DOWNLOAD_RETRIES + 1):
        try:
            result = subprocess.run([
                "aria2c", "-x", "16", "-s", "16", "-o", ZIP_NAME, ZIP_URL
            ], check=False)

            if result.returncode == 0 and os.path.exists(ZIP_NAME):
                log("✅ Download erfolgreich.")
                return
            else:
                log(f"⚠️ Versuch {attempt} fehlgeschlagen. Warte {DOWNLOAD_RETRY_DELAY} Sekunden.")
        except Exception as e:
            log(f"❌ Ausnahme beim Download-Versuch {attempt}: {e}")
        time.sleep(DOWNLOAD_RETRY_DELAY)

    raise Exception("❌ ZIP-Download nach mehreren Versuchen fehlgeschlagen.")

if os.path.exists(LOG_FILE) and os.path.getsize(LOG_FILE) > 10 * 1024 * 1024:
    with open(LOG_FILE, "w") as f:
        f.write("")

def extract_needed_files():
    if os.path.exists(EXTRACT_DIR):
        shutil.rmtree(EXTRACT_DIR)
    os.makedirs(EXTRACT_DIR, exist_ok=True)
    with zipfile.ZipFile(ZIP_NAME, 'r') as zip_ref:
        for member in zip_ref.infolist():
            if member.filename.startswith("EinheitenSolar") and member.filename.endswith(".xml"):
                zip_ref.extract(member, EXTRACT_DIR)

def process_batch(batch, max_retries=3, retry_delay=5):
    if not batch:
        return

    try:
        response = requests.post(
            f"{DIRECTUS_URL}/items/{TABLE_NAME}",
            json=batch,
            headers=HEADERS,
            timeout=120
        )
        if response.status_code in [200, 201]:
            log(f"✅ {len(batch)} Einträge erfolgreich eingefügt.")
            return
        elif response.status_code == 400 and "RECORD_NOT_UNIQUE" in response.text:
            log(f"↪️ Duplikate gefunden, führe Updates für {len(batch)} Einträge durch.")
            failed_updates = []
            for item in batch:
                item_id = item.get("id")
                if not item_id:
                    continue

                success = False
                for attempt in range(max_retries):
                    try:
                        patch = requests.patch(
                            f"{DIRECTUS_URL}/items/{TABLE_NAME}/{item_id}",
                            json=item,
                            headers=HEADERS,
                            timeout=60
                        )
                        if patch.status_code in [200, 204]:
                            success = True
                            break
                        elif patch.status_code == 503:
                            time.sleep(retry_delay * (2 ** attempt))
                        else:
                            log(f"⚠️ Update fehlgeschlagen für {item_id}: {patch.text}", level="WARNING")
                            break
                    except Exception as e:
                        log(f"❌ Ausnahme bei Update-Versuch {attempt + 1} für {item_id}: {e}", level="WARNING")
                        time.sleep(retry_delay * (2 ** attempt))

                if not success:
                    failed_updates.append(item_id)

                time.sleep(0.1)

            if failed_updates:
                log(f"⚠️ {len(failed_updates)} Updates endgültig fehlgeschlagen: {failed_updates}", level="WARNING")
        else:
            log(f"❌ Fehler beim Insert: {response.status_code} - {response.text}", level="ERROR")
    except Exception as e:
        log(f"❌ Ausnahme beim Insert: {e}", level="ERROR")

def parse_files():
    found = 0
    written = 0
    failed = 0

    for file in sorted(Path(EXTRACT_DIR).rglob("EinheitenSolar*.xml")):
        log(f"🔍 Verarbeite {file.name}")
        context = ET.iterparse(file, events=("end",))
        batch = []

        for event, elem in context:
            if elem.tag == ENTRY_TAG:
                try:
                    last_update_raw = elem.findtext("DatumLetzteAktualisierung")
                    if not last_update_raw or parse_date(last_update_raw) < THRESHOLD_DATE:
                        elem.clear()
                        continue

                    mastr_id = elem.findtext("EinheitMastrNummer")
                    if not mastr_id or mastr_id.strip() == "":
                        continue
                    mastr_id = mastr_id.strip()

                    bruttoleistung = elem.findtext("Bruttoleistung")
                    module_count = elem.findtext("AnzahlModule")

                    unit = {
                        "id": mastr_id,
                        "name": elem.findtext("NameStromerzeugungseinheit"),
                        "status": elem.findtext("EinheitBetriebsstatus"),
                        "commissioning_date": elem.findtext("Inbetriebnahmedatum"),
                        "last_update": last_update_raw,
                        "shutdown_date": elem.findtext("DatumEndgueltigeStilllegung"),
                        "orientation": elem.findtext("Hauptausrichtung"),
                        "type": elem.findtext("ArtDerSolaranlage"),
                        "power_kw": float(bruttoleistung.replace(",", ".")) if bruttoleistung else None,
                        "module_count": math.floor(float(module_count.replace(",", "."))) if module_count else None,
                        "storage_installed": elem.findtext("SpeicherAmGleichenOrt") == "1",
                        "federal_state": elem.findtext("Bundesland"),
                        "district": elem.findtext("Landkreis"),
                        "municipality": elem.findtext("Gemeinde"),
                        "region": elem.findtext("Gemeindeschluessel"),
                        "country": "DE"
                    }

                    batch.append(unit)
                    found += 1

                    if len(batch) >= BATCH_SIZE:
                        process_batch(batch)
                        written += len(batch)
                        batch.clear()
                except Exception as e:
                    log(f"⚠️ Fehler beim Parsen eines Eintrags: {e}")
                    failed += 1
                finally:
                    elem.clear()

        if batch:
            process_batch(batch)
            written += len(batch)

    log(f"📊 Verarbeitet: {found}, Eingefügt/Aktualisiert: {written}")
    return found, written, failed

def cleanup():
    log("🧹 Lösche temporäre Dateien...")
    if os.path.exists(EXTRACT_DIR):
        shutil.rmtree(EXTRACT_DIR)
    if os.path.exists(ZIP_NAME):
        os.remove(ZIP_NAME)

if __name__ == "__main__":
    start_time = time.time()

    slack_log("📥 Sync des Marktstammdatenregisters gestartet.", level="INFO")

    total_found = 0
    total_written = 0
    total_failed = 0

    try:
        download_zip()
        extract_needed_files()
        found, written, failed = parse_files()
        total_found += found
        total_written += written
        total_failed += failed

        duration = round(time.time() - start_time)
        slack_log(
            f"✅ Sync abgeschlossen in {duration}s\n"
            f"- Gefunden: {total_found}\n"
            f"- Eingefügt/Aktualisiert: {total_written}\n"
            f"- Fehlerhaft: {total_failed}",
            level="SUCCESS"
        )
    except Exception as e:
        slack_log(f"❌ Fehler beim Marktstammdatenregister Sync: {e}", level="ERROR")
    finally:
        cleanup()
