"""
Marktstammdatenregister Daily Sync — parent orchestrator.

Downloads the full MaStR bulk export once (solar, storage, biomass, hydro,
gsgk), then invokes the solar, storage and generic units (biomass + hydro)
sync subscripts sequentially.  Each subscript handles
its own Directus sync and Slack logging.  This script owns the download and
the final zip cleanup.

Usage:
    python marktstammdatenregister-daily-sync.py           # incremental (last N days)
    python marktstammdatenregister-daily-sync.py --full    # full reload, skip unchanged
    python marktstammdatenregister-daily-sync.py --rebuild # clear tables, re-insert everything
"""

import os
import sys
import subprocess
import time
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import text
from open_mastr import Mastr
from open_mastr.utils.constants import BULK_INCLUDE_TABLES_MAP

try:
    from slack_logger import slack_log
except ImportError:
    def slack_log(msg, level="INFO"):
        print(f"[SLACK {level}] {msg}")

load_dotenv()

REBUILD = "--rebuild" in sys.argv
FULL_LOAD = "--full" in sys.argv or REBUILD

DOWNLOAD_ATTEMPTS = int(os.getenv("MASTR_DOWNLOAD_ATTEMPTS", 3))
DOWNLOAD_RETRY_WAIT = int(os.getenv("MASTR_DOWNLOAD_RETRY_WAIT", 300))

# Incremental window for the subscripts (they default to 10 days on their own).
os.environ.setdefault("UPDATE_DAYS_BACK", "5")

# The subscripts only read solar_extended, storage_extended, storage_units,
# biomass_extended, hydro_extended and gsgk_extended.
# Skipping the EEG files (AnlagenEeg*) saves both
# download volume and ~20 min of XML parsing per run. open_mastr's download
# and write steps share this dict, so trimming it here affects both.
UNUSED_BULK_FILES = {
    "anlageneegsolar", "anlageneegspeicher", "anlageneegbiomasse",
    "anlageneegwasser", "anlageneeggeothermiegrubengasdruckentspannung",
}
UNUSED_TABLES = ["solar_eeg", "storage_eeg", "biomass_eeg", "hydro_eeg", "gsgk_eeg"]
DOWNLOAD_TYPES = ["solar", "storage", "storage_units", "biomass", "hydro", "gsgk"]


def log(msg, level="INFO"):
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} [daily-sync] {level}: {msg}")


def skip_unused_bulk_files():
    for key in ("solar", "storage", "biomass", "hydro", "gsgk"):
        BULK_INCLUDE_TABLES_MAP[key] = [
            f for f in BULK_INCLUDE_TABLES_MAP[key] if f not in UNUSED_BULK_FILES
        ]


def drop_unused_tables(db):
    # Tables from earlier runs would otherwise linger with stale data.
    with db.engine.begin() as con:
        for table in UNUSED_TABLES:
            con.execute(text(f"DROP TABLE IF EXISTS {table}"))


def delete_todays_zip():
    mastr_dir = Path.home() / ".open-MaStR" / "data" / "xml_download"
    zip_file = mastr_dir / f"Gesamtdatenexport_{datetime.today().strftime('%Y%m%d')}.zip"
    if zip_file.exists():
        zip_file.unlink()
        log(f"Deleted possibly incomplete {zip_file.name}.")


def download_mastr_data():
    log("Initializing open-mastr...")
    db = Mastr()
    skip_unused_bulk_files()
    drop_unused_tables(db)
    log(f"Downloading {DOWNLOAD_TYPES} from MaStR (single bulk export)...")
    # "storage_units" is a separate open_mastr type (AnlagenStromSpeicher) that
    # carries NutzbareSpeicherkapazitaet. It is not included in "storage".
    # The MaStR server occasionally resets connections, so retry a few times.
    for attempt in range(1, DOWNLOAD_ATTEMPTS + 1):
        try:
            db.download(data=DOWNLOAD_TYPES, bulk_cleansing=True)
            break
        except Exception as e:
            if attempt == DOWNLOAD_ATTEMPTS:
                raise
            log(
                f"Download attempt {attempt}/{DOWNLOAD_ATTEMPTS} failed: {e}. "
                f"Retrying in {DOWNLOAD_RETRY_WAIT}s...",
                level="WARNING",
            )
            delete_todays_zip()
            time.sleep(DOWNLOAD_RETRY_WAIT)
    log("MaStR download complete.")


def run_subscript(script_name, *extra_args):
    here = os.path.dirname(os.path.abspath(__file__))
    args = [sys.executable, os.path.join(here, script_name), "--skip-download", *extra_args]
    if REBUILD:
        args.append("--rebuild")
    elif FULL_LOAD:
        args.append("--full")
    log(f"--- Starting {script_name} ---")
    # Output streams directly to stdout so each subscript's logs appear in line
    result = subprocess.run(args)
    log(f"--- {script_name} finished (exit {result.returncode}) ---")
    return result.returncode


def cleanup():
    mastr_dir = Path.home() / ".open-MaStR" / "data" / "xml_download"
    if not mastr_dir.exists():
        return
    today = datetime.today().date()
    for zip_file in mastr_dir.glob("Gesamtdatenexport_*.zip"):
        file_date = datetime.fromtimestamp(zip_file.stat().st_mtime).date()
        if (FULL_LOAD or REBUILD) and file_date == today:
            log(f"Keeping {zip_file.name} (reusable today).")
            continue
        try:
            zip_file.unlink()
            log(f"Deleted {zip_file.name}.")
        except Exception as e:
            log(f"Could not delete {zip_file.name}: {e}", level="WARNING")


def main():
    start_time = time.time()
    mode = "rebuild" if REBUILD else ("full" if FULL_LOAD else "incremental")
    slack_log(f"MaStR Daily Sync gestartet (Modus: {mode}).", level="INFO")

    solar_rc = storage_rc = units_rc = -1
    try:
        download_mastr_data()

        solar_rc = run_subscript("marktstammdatenregister-solar-sync.py")
        storage_rc = run_subscript("marktstammdatenregister-storage-sync.py")
        units_rc = run_subscript("marktstammdatenregister-units-sync.py", "--carrier", "biomass,hydro")

        duration = round(time.time() - start_time)
        all_ok = solar_rc == 0 and storage_rc == 0 and units_rc == 0

        if all_ok:
            slack_log(
                f"MaStR Daily Sync erfolgreich abgeschlossen in {duration}s (Solar + Speicher + Biomasse/Wasser, Modus: {mode}).",
                level="SUCCESS",
            )
        else:
            slack_log(
                f"MaStR Daily Sync in {duration}s abgeschlossen (mit Fehlern)\n"
                f"- Solar:   {'OK' if solar_rc == 0 else f'FEHLER (exit {solar_rc})'}\n"
                f"- Speicher: {'OK' if storage_rc == 0 else f'FEHLER (exit {storage_rc})'}\n"
                f"- Biomasse/Wasser: {'OK' if units_rc == 0 else f'FEHLER (exit {units_rc})'}",
                level="ERROR",
            )

        log(f"Done in {duration}s. solar_rc={solar_rc}, storage_rc={storage_rc}, units_rc={units_rc}")

    except Exception as e:
        slack_log(f"Fehler beim MaStR Daily Sync: {e}", level="ERROR")
        log(f"Error: {e}", level="ERROR")
        raise

    finally:
        cleanup()

    if solar_rc != 0 or storage_rc != 0 or units_rc != 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
