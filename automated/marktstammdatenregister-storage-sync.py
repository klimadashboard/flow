"""
Marktstammdatenregister Storage Sync using open-mastr library.

Downloads all storage unit data (Batterie, Pumpspeicher, Schwungrad,
Druckluft, Wasserstoffspeicher) from MaStR and syncs to Directus.

Supports both full initial load and incremental daily sync.

Usage:
    pip install open-mastr pandas python-dotenv requests
    python storage-sync.py              # incremental (last N days)
    python storage-sync.py --full       # full load (all entries)


Plausibility Checks
===================
MaStR data is entered manually by operators and frequently contains errors.
Before syncing to our database, every battery storage entry is run through
a three-step plausibility pipeline based on the methodology described by
Battery-Charts.de / OET (https://battery-charts.de).

Non-battery technologies (Pumpspeicher, Druckluft, Schwungrad,
Wasserstoffspeicher) are passed through without plausibility filtering.

Step 1 – Consistency Filter
----------------------------
Only battery entries that satisfy ALL of the following criteria are kept:

  - Usable storage capacity (NutzbareSpeicherkapazitaet) > 0.3 kWh
  - Gross power (Bruttoleistung) > 0.3 kW
  - Battery technology (Batterietechnologie) is defined (incl. "Sonstige")
  - Commissioning date (Inbetriebnahmedatum) is present and valid
  - Energy-to-power ratio (E/P = capacity / power, representing the
    charge/discharge duration at full load) lies between 6 minutes
    (0.1 h) and 12 hours

Step 2 – Market Segment Categorisation
----------------------------------------
Valid entries are classified into three market segments using BOTH capacity
AND power thresholds:

  Heimspeicher (residential):
      capacity < 30 kWh  AND  power < 30 kW

  Gewerbespeicher (commercial):
      Everything between the Heimspeicher and Grossspeicher thresholds,
      i.e. entries that are neither Heim nor Gross.  Concretely this is
      the union of:
        a) Exactly one of capacity < 30 kWh / power < 30 kW is true
           (XOR – one dimension still in Heim range, the other not),
           AND both < 1000
        b) Both capacity >= 30 kWh and power >= 30 kW, but at least one
           is < 1000

  Grossspeicher (utility-scale):
      capacity >= 1000 kWh  AND  power >= 1000 kW
      with two additional safeguards:
        - Must NOT be operated by a natural person (natuerliche Person).
          Private individuals frequently confuse kW and W, leading to
          entries that appear to be megawatt-class but are actually small
          residential systems.
        - If the grid connection is at low voltage (Niederspannung), the
          entry is only accepted when the grid operator has already
          reviewed it (NetzbetreiberpruefungStatus = "Geprueft").

  Entries that meet the Grossspeicher capacity/power thresholds but fail
  the operator-type or voltage-level checks are "demoted" – moved to the
  filtered set for correction in Step 3.

Step 3 – Correction
---------------------
ALL entries that were filtered out by Step 1 or demoted by Step 2 are
re-added to the dataset so that every registered storage unit appears in
the statistics.  The correction works as follows:

  Category assignment:
    - If the operator is a natural person  -> Heimspeicher
    - Otherwise                            -> Gewerbespeicher
    (Grossspeicher are assumed to always be entered correctly by
     professional operators, so no entry is corrected to Grossspeicher.)

  Dimension correction:
    The original (likely erroneous) capacity and power values are replaced
    by the average capacity and power of all valid entries in the same
    category for the same commissioning month (YYYY-MM).  If no month-
    specific average is available, the overall category average is used as
    a fallback.

Full-Mode Deletion
==================
When running with --full, after syncing all plausibility-checked records,
the script also deletes any records from Directus whose IDs are no longer
present in the raw MaStR data.  This removes units that have been fully
retracted from the registry.

Data Source & Licence
=====================
The underlying data is published by the Bundesnetzagentur under the
dl-de/by-2-0 licence at https://www.marktstammdatenregister.de/MaStR.
"""

import os
import sys
import math
import time
import requests
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from open_mastr import Mastr

try:
    from slack_logger import slack_log
except ImportError:
    def slack_log(msg, level="INFO"):
        print(f"[SLACK {level}] {msg}")

load_dotenv()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DIRECTUS_URL = os.getenv("DIRECTUS_API_URL")
DIRECTUS_TOKEN = os.getenv("DIRECTUS_API_TOKEN")
TABLE_NAME = "energy_storage_units"             # Directus collection name
BATCH_SIZE = int(os.getenv("DIRECTUS_BATCH_SIZE", 1000))  # records per API call
UPDATE_DAYS_BACK = int(os.getenv("UPDATE_DAYS_BACK", 10)) # look-back window for incremental sync
FULL_LOAD = "--full" in sys.argv                # full mode: reload everything + delete stale
MAX_WORKERS = int(os.getenv("SYNC_WORKERS", 4)) # concurrent API threads for full mode

HEADERS = {
    "Authorization": f"Bearer {DIRECTUS_TOKEN}",
    "Content-Type": "application/json"
}

# In incremental mode, only entries updated after this date are processed.
THRESHOLD_DATE = datetime.today() - timedelta(days=UPDATE_DAYS_BACK)


def log(msg, level="INFO"):
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {level}: {msg}")


def download_storage_data():
    """
    Download storage data from MaStR via the open-mastr library.

    Returns:
        (df, db) – a pandas DataFrame with one row per storage generation
        unit, and the Mastr database handle (for potential re-use).

    The open-mastr library stores its results in a local SQLite database
    with (among others) two tables relevant to us:

      storage_extended  – one row per generation unit (Erzeugungseinheit)
                          with technical details, location, operator, etc.
      storage_units     – one row per storage plant (Speicheranlage) that
                          holds NutzbareSpeicherkapazitaet (usable capacity
                          in kWh).

    Since capacity lives at the *plant* level we merge it onto the unit
    rows via SpeMastrNummer.

    Additionally, we join operator person-type (Personenart) from the
    market_actors table so that the plausibility checks can distinguish
    natural persons from organisations (needed for the Grossspeicher
    operator-type guard).
    """
    log("Initializing open-mastr...")
    db = Mastr()

    log("Downloading storage data from MaStR...")
    db.download(data=["storage"], bulk_cleansing=True)

    log("Reading storage data from database...")

    # Check available columns
    test_query = "SELECT * FROM storage_extended LIMIT 1"
    test_df = pd.read_sql(sql=test_query, con=db.engine)
    columns = test_df.columns.tolist()
    log(f"storage_extended columns: {columns}")

    # Determine date column for filtering
    date_column = None
    for col in ['DatumLetzteAktualisierung', 'date_last_update', 'DateLastUpdate', 'LastUpdate']:
        if col in columns:
            date_column = col
            break

    if FULL_LOAD:
        log("Full load mode: loading all storage entries...")
        df = pd.read_sql(sql="storage_extended", con=db.engine)
    elif date_column:
        threshold_str = THRESHOLD_DATE.strftime('%Y-%m-%d')
        log(f"Incremental mode: filtering by {date_column} >= {threshold_str}")
        query = f"SELECT * FROM storage_extended WHERE {date_column} >= '{threshold_str}'"
        df = pd.read_sql(sql=query, con=db.engine)
    else:
        log("Could not find last update column. Loading all entries.", level="WARNING")
        df = pd.read_sql(sql="storage_extended", con=db.engine)

    log(f"Loaded {len(df)} storage units from storage_extended.")

    # Load storage_units table to get NutzbareSpeicherkapazitaet
    # The capacity is stored at the plant level (SpeMastrNummer / MastrNummer)
    try:
        storage_units = pd.read_sql(
            sql="SELECT MastrNummer, NutzbareSpeicherkapazitaet FROM storage_units",
            con=db.engine
        )
        log(f"Loaded {len(storage_units)} entries from storage_units.")

        # Check if storage_extended already has NutzbareSpeicherkapazitaet
        if 'NutzbareSpeicherkapazitaet' in df.columns:
            # Drop it to avoid conflict on merge, we'll use the one from storage_units
            df = df.drop(columns=['NutzbareSpeicherkapazitaet'], errors='ignore')

        # Merge capacity from storage_units onto storage_extended via SpeMastrNummer
        if 'SpeMastrNummer' in df.columns:
            df = df.merge(
                storage_units,
                left_on='SpeMastrNummer',
                right_on='MastrNummer',
                how='left',
                suffixes=('', '_plant')
            )
            log("Merged NutzbareSpeicherkapazitaet from storage_units.")
        else:
            log("SpeMastrNummer not found in storage_extended, skipping capacity merge.", level="WARNING")

    except Exception as e:
        log(f"Could not load storage_units table: {e}", level="WARNING")

    # ------------------------------------------------------------------
    # Merge operator person type from market_actors
    # ------------------------------------------------------------------
    # The Personenart column tells us whether an operator is a
    # "natuerlichePerson" (natural person / private individual) or a
    # "juristischePerson" (legal entity / organisation).  This is
    # required by the plausibility checks: Grossspeicher operated by
    # natural persons are demoted because private individuals frequently
    # confuse kW with W.
    try:
        if 'Personenart' not in df.columns:
            actors = pd.read_sql(
                sql="SELECT MastrNummer, Personenart FROM market_actors",
                con=db.engine
            )
            df = df.merge(
                actors,
                left_on='AnlagenbetreiberMastrNummer',
                right_on='MastrNummer',
                how='left',
                suffixes=('', '_actor')
            )
            log(f"Merged Personenart from market_actors ({len(actors)} actors).")
        else:
            log("Personenart already available in storage_extended.")
    except Exception as e:
        log(f"Could not get operator person types: {e}", level="WARNING")

    return df, db


def map_to_directus_schema(df):
    """
    Map the open-mastr storage DataFrame to the Directus collection schema.

    Returns a list of dicts, one per storage unit, with Directus field names
    and properly typed values.

    In addition to the "real" Directus fields, each record is enriched with
    two *internal* fields (prefixed with ``_``) that are consumed by the
    plausibility pipeline and stripped before the actual API sync:

      _voltage_level       – string, raw Spannungsebene value
      _is_natural_person   – bool, True when the operator is a natural person

    The ``category`` field is NOT set here.  It is determined later by
    ``apply_plausibility_checks()`` which implements the full Battery-Charts
    methodology (consistency filter -> categorisation -> correction).
    """
    STATUS_CODES = {
        "In Betrieb": "35",
        "Voruebergehend stillgelegt": "37",
        "Endgueltig stillgelegt": "38",
        "In Planung": "31",
    }

    # Column mapping: open-mastr column -> Directus field
    column_mapping = {
        'EinheitMastrNummer': 'id',

        'NameStromerzeugungseinheit': 'name',

        'EinheitBetriebsstatus': 'status',

        'Inbetriebnahmedatum': 'commissioning_date',

        'DatumLetzteAktualisierung': 'last_update',

        'DatumEndgueltigeStilllegung': 'shutdown_date',

        'GeplantesInbetriebnahmedatum': 'planned_commissioning_date',

        'Energietraeger': 'energy_source',

        'Technologie': 'technology',

        'Batterietechnologie': 'battery_technology',

        'Pumpspeichertechnologie': 'pumped_storage_technology',

        'Bruttoleistung': 'power_kw',

        'Nettonennleistung': 'net_power_kw',

        'NutzbareSpeicherkapazitaet': 'usable_storage_capacity_kwh',

        'PumpbetriebLeistungsaufnahme': 'pump_power_kw',

        'ZugeordneteWirkleistungWechselrichter': 'inverter_power_kw',

        'AcDcKoppelung': 'ac_dc_coupling',

        'AnschlussAnHoechstOderHochSpannung': 'high_voltage_connection',

        'Einspeisungsart': 'feed_in_type',

        'AnlagenbetreiberMastrNummer': 'operator_name',

        'GemeinsamRegistrierteSolareinheitMastrNummer': 'co_registered_solar_unit',

        'Einsatzort': 'deployment_location',

        'Bundesland': 'federal_state',

        'Landkreis': 'district',

        'Gemeinde': 'municipality',

        'Gemeindeschluessel': 'region',

        'Postleitzahl': 'postcode',

        'Breitengrad': 'lat',

        'Laengengrad': 'lon',

        'NetzbetreiberpruefungStatus': 'grid_operator_review_status',

        'SpeMastrNummer': 'storage_mastr_number',
    }

    # ------------------------------------------------------------------
    # Build working DataFrame with renamed columns (vectorized)
    # ------------------------------------------------------------------
    # Instead of iterating row-by-row with df.iterrows() (very slow for
    # large DataFrames), we rename columns, coerce types in bulk, and
    # call to_dict('records') at the end.

    # Determine which source columns are actually present
    actual_mapping = {}
    for source_col, target_col in column_mapping.items():
        if source_col in df.columns and target_col not in actual_mapping.values():
            actual_mapping[source_col] = target_col

    log(f"Mapped columns: {actual_mapping}")

    # Rename source -> target in one shot
    mapped = df[list(actual_mapping.keys())].rename(columns=actual_mapping).copy()

    # Ensure we have an ID column; try fallbacks if EinheitMastrNummer
    # was not in the DataFrame.
    if 'id' not in mapped.columns:
        for col in ['mastr_id', 'unit_registration_id']:
            if col in df.columns:
                mapped['id'] = df[col]
                break
        else:
            log("No ID column found in data!", level="ERROR")
            return []

    mapped['id'] = mapped['id'].astype(str).str.strip()
    # Drop rows with missing / invalid IDs
    mapped = mapped[mapped['id'].notna() & ~mapped['id'].isin(['', 'nan', 'None'])].copy()

    mapped['country'] = 'DE'

    # ------------------------------------------------------------------
    # Vectorized type coercions
    # ------------------------------------------------------------------
    float_fields = {'power_kw', 'net_power_kw', 'usable_storage_capacity_kwh',
                    'pump_power_kw', 'inverter_power_kw', 'lat', 'lon'}
    for col in float_fields & set(mapped.columns):
        # Handle European comma decimals ("1,5" -> "1.5"), then coerce
        mapped[col] = pd.to_numeric(
            mapped[col].astype(str).str.replace(',', '.', regex=False),
            errors='coerce'
        )

    date_fields = {'commissioning_date', 'shutdown_date', 'planned_commissioning_date'}
    for col in date_fields & set(mapped.columns):
        ts = pd.to_datetime(mapped[col], errors='coerce')
        # NaT.strftime would produce NaN – use .where to keep None instead
        mapped[col] = ts.dt.strftime('%Y-%m-%d').where(ts.notna(), None)

    datetime_fields = {'last_update'}
    for col in datetime_fields & set(mapped.columns):
        ts = pd.to_datetime(mapped[col], errors='coerce')
        mapped[col] = ts.dt.strftime('%Y-%m-%dT%H:%M:%S').where(ts.notna(), None)

    boolean_fields = {'high_voltage_connection'}
    for col in boolean_fields & set(mapped.columns):
        na_mask = mapped[col].isna()
        mapped[col] = mapped[col].astype(str).isin(['1', 'true', 'True', 'ja', 'Ja'])
        mapped.loc[na_mask, col] = None

    if 'status' in mapped.columns:
        mapped['status'] = (
            mapped['status'].astype(str)
            .map(lambda v: STATUS_CODES.get(v, v))
            .where(mapped['status'].notna(), None)
        )

    # String fields: ensure str type, keep None for NaN
    string_fields = {'technology', 'battery_technology', 'pumped_storage_technology',
                     'ac_dc_coupling', 'deployment_location', 'federal_state',
                     'district', 'municipality', 'grid_operator_review_status',
                     'co_registered_solar_unit', 'storage_mastr_number',
                     'region', 'postcode', 'name', 'energy_source',
                     'feed_in_type', 'operator_name'}
    for col in string_fields & set(mapped.columns):
        na_mask = mapped[col].isna()
        mapped.loc[~na_mask, col] = mapped.loc[~na_mask, col].astype(str)
        mapped.loc[na_mask, col] = None

    # ------------------------------------------------------------------
    # Internal fields for plausibility checks
    # ------------------------------------------------------------------
    # These are prefixed with "_" and will be stripped before the records
    # are sent to Directus.
    #
    #   _voltage_level      – Spannungsebene (e.g. "Niederspannung")
    #   _is_natural_person  – True if operator Personenart is "natuerlich"

    if 'Spannungsebene' in df.columns:
        mapped['_voltage_level'] = (
            df.loc[mapped.index, 'Spannungsebene'].astype(str)
            .where(df.loc[mapped.index, 'Spannungsebene'].notna(), None)
        )
    else:
        mapped['_voltage_level'] = None

    if 'Personenart' in df.columns:
        pa = df.loc[mapped.index, 'Personenart'].astype(str).str.lower()
        mapped['_is_natural_person'] = pa.str.contains('natürlich|natuerlich', na=False)
    else:
        mapped['_is_natural_person'] = False

    # ------------------------------------------------------------------
    # Convert NaN -> None and produce list of dicts
    # ------------------------------------------------------------------
    # astype(object) allows None values; where(notna) replaces NaN.
    mapped = mapped.astype(object).where(mapped.notna(), None)

    # NOTE: category is NOT set here.  It is determined later by
    # apply_plausibility_checks() after the full three-step pipeline.

    records = mapped.to_dict('records')
    log(f"Mapped {len(records)} records (vectorized)")
    return records


# ============================================================================
# Plausibility Pipeline  (Battery-Charts / OET methodology)
# ============================================================================
# The three functions below implement the three steps described in the
# module docstring.  They are orchestrated by apply_plausibility_checks().
# ============================================================================


def passes_consistency_check(record):
    """
    Battery-Charts Step 1 – Consistency Filter.

    Returns True if the battery record passes every basic quality gate:

      1. Usable storage capacity must be present and > 0.3 kWh.
         Very small or zero values indicate a data-entry error or
         placeholder.

      2. Gross power must be present and > 0.3 kW.
         Same rationale as capacity.

      3. Battery technology (Batterietechnologie) must be defined.
         All real entries have a technology label, including "Sonstige"
         for unspecified chemistries.  A missing value indicates an
         incomplete registration.

      4. Commissioning date (Inbetriebnahmedatum) must be present.
         Without a date the entry cannot be placed on a timeline.

      5. The energy-to-power ratio (E/P) must lie between 0.1 h and 12 h.
         E/P = capacity_kWh / power_kW and represents the theoretical
         full-load charge/discharge duration.
           - < 6 min  (0.1 h): physically implausible for batteries
           - > 12 h:           likely a data-entry error (e.g. capacity
                               entered in Wh instead of kWh)

    Entries that fail any of these checks are passed to the correction
    step (Step 3) where they are re-added with averaged dimensions.
    """
    capacity = record.get('usable_storage_capacity_kwh')
    power = record.get('power_kw')
    bat_tech = record.get('battery_technology')
    date = record.get('commissioning_date')

    # Gate 1: capacity present and above minimum
    if capacity is None or capacity <= 0.3:
        return False

    # Gate 2: power present and above minimum
    if power is None or power <= 0.3:
        return False

    # Gate 3: battery technology defined
    if not bat_tech:
        return False

    # Gate 4: valid commissioning date
    if not date:
        return False

    # Gate 5: E/P ratio between 6 min (0.1 h) and 12 h
    try:
        ep_ratio = capacity / power
        if ep_ratio < 0.1 or ep_ratio > 12:
            return False
    except (ZeroDivisionError, TypeError):
        return False

    return True


def categorize_battery(record):
    """
    Battery-Charts Step 2 – Market Segment Categorisation.

    Classifies a *consistent* (Step-1-passed) battery entry into one of
    three market segments using BOTH capacity and power:

      Heimspeicher (residential)
          capacity < 30 kWh  AND  power < 30 kW

      Grossspeicher (utility-scale)
          capacity >= 1000 kWh  AND  power >= 1000 kW
          Subject to two additional safeguards:
            a) The operator must NOT be a natural person.  Private
               individuals often confuse kW with W, making a 10 kW
               system appear as 10 000 kW.  Such entries are demoted
               (returned as None) so that Step 3 can correct them.
            b) If the grid connection is at low voltage
               (Niederspannung), the entry is only accepted if the
               grid operator has already reviewed it
               (NetzbetreiberpruefungStatus contains "Geprueft").
               Unreviewed low-voltage Grossspeicher are likely errors.

      Gewerbespeicher (commercial)
          Everything that is neither Heim nor Gross.  This covers:
            - XOR cases: one dimension < 30, the other >= 30
            - Mid-range: both >= 30 but at least one < 1000

    Returns:
        Category string ('Heimspeicher', 'Gewerbespeicher', or
        'Grossspeicher'), or None if the entry should be demoted to
        the filtered set for correction in Step 3.
    """
    capacity = record.get('usable_storage_capacity_kwh')
    power = record.get('power_kw')

    if capacity is None or power is None:
        return None

    # --- Heimspeicher: both dimensions below 30 ---
    if capacity < 30 and power < 30:
        return 'Heimspeicher'

    # --- Grossspeicher candidate: both dimensions at or above 1000 ---
    if capacity >= 1000 and power >= 1000:

        # Guard (a): natural persons are very unlikely to own a
        # megawatt-class storage – almost certainly a unit mix-up.
        if record.get('_is_natural_person', False):
            return None  # demote for correction

        # Guard (b): Grossspeicher on low-voltage grid only if the
        # grid operator has already confirmed the entry.
        voltage = str(record.get('_voltage_level') or '').lower()
        if 'niederspannung' in voltage:
            review = str(record.get('grid_operator_review_status') or '').lower()
            if 'geprueft' not in review and 'geprüft' not in review:
                return None  # demote for correction

        return 'Grossspeicher'

    # --- Gewerbespeicher: everything in between ---
    return 'Gewerbespeicher'


def correct_filtered_entries(filtered, valid):
    """
    Battery-Charts Step 3 – Correction of Filtered Entries.

    Every entry that was rejected by the consistency filter (Step 1) or
    demoted by the categorisation safeguards (Step 2) is re-added to the
    dataset.  This ensures that every storage unit registered in MaStR is
    represented in our statistics, even if the original values were
    clearly wrong.

    Category assignment:
      - Natural person (Personenart = natuerlichePerson)
            -> Heimspeicher
               Rationale: a private individual almost certainly owns a
               residential battery.
      - Organisation / legal entity
            -> Gewerbespeicher
               Rationale: we assume that truly utility-scale storage
               (Grossspeicher) is always registered correctly by
               professional operators.

    Dimension correction:
      The (likely erroneous) capacity and power values are replaced by
      the mean capacity and mean power of all *valid* entries in the
      same category for the same commissioning month (YYYY-MM).

      Fallback hierarchy:
        1. Monthly average of the assigned category
        2. Overall average of the assigned category (all months)
        3. Keep original values as-is (last resort, should be rare)

    Args:
        filtered: list of record dicts that failed Step 1 or 2.
        valid:    list of record dicts that passed Steps 1 + 2 (already
                  have their ``category`` set).

    Returns:
        List of corrected record dicts, ready to be merged back.
    """
    if not filtered:
        return []

    # ------------------------------------------------------------------
    # Build reference tables of average capacity / power from valid
    # entries, keyed by (category, YYYY-MM) and by category alone.
    # ------------------------------------------------------------------
    monthly_sums = {}   # (category, "YYYY-MM") -> {cap, pwr, n}
    category_sums = {}  # category              -> {cap, pwr, n}

    for r in valid:
        cat = r.get('category')
        cap = r.get('usable_storage_capacity_kwh')
        pwr = r.get('power_kw')
        date_str = r.get('commissioning_date', '')
        if not cat or cap is None or pwr is None:
            continue

        # Extract "YYYY-MM" from the date string (e.g. "2024-03-15" -> "2024-03")
        month = date_str[:7] if date_str and len(date_str) >= 7 else None

        if month:
            key = (cat, month)
            if key not in monthly_sums:
                monthly_sums[key] = {'cap': 0, 'pwr': 0, 'n': 0}
            monthly_sums[key]['cap'] += cap
            monthly_sums[key]['pwr'] += pwr
            monthly_sums[key]['n'] += 1

        if cat not in category_sums:
            category_sums[cat] = {'cap': 0, 'pwr': 0, 'n': 0}
        category_sums[cat]['cap'] += cap
        category_sums[cat]['pwr'] += pwr
        category_sums[cat]['n'] += 1

    # Pre-compute averages for fast lookup
    monthly_avgs = {
        k: {'cap': v['cap'] / v['n'], 'pwr': v['pwr'] / v['n']}
        for k, v in monthly_sums.items() if v['n'] > 0
    }
    category_avgs = {
        k: {'cap': v['cap'] / v['n'], 'pwr': v['pwr'] / v['n']}
        for k, v in category_sums.items() if v['n'] > 0
    }

    # ------------------------------------------------------------------
    # Correct each filtered entry
    # ------------------------------------------------------------------
    corrected = []
    for r in filtered:
        r = r.copy()  # don't mutate the original

        # Assign category based on operator type
        if r.get('_is_natural_person', False):
            r['category'] = 'Heimspeicher'
        else:
            r['category'] = 'Gewerbespeicher'

        # Replace capacity and power with the monthly average of the
        # assigned category.  Fall back to overall category average.
        cat = r['category']
        date_str = r.get('commissioning_date', '')
        month = date_str[:7] if date_str and len(date_str) >= 7 else None

        key = (cat, month) if month else None
        if key and key in monthly_avgs:
            r['usable_storage_capacity_kwh'] = round(monthly_avgs[key]['cap'], 2)
            r['power_kw'] = round(monthly_avgs[key]['pwr'], 2)
        elif cat in category_avgs:
            # Fallback: overall category average (all months combined)
            r['usable_storage_capacity_kwh'] = round(category_avgs[cat]['cap'], 2)
            r['power_kw'] = round(category_avgs[cat]['pwr'], 2)
        # else: keep original values as last resort – this can only happen
        # when there are zero valid entries in the assigned category.

        corrected.append(r)

    return corrected


def apply_plausibility_checks(records):
    """
    Orchestrate the full Battery-Charts / OET plausibility pipeline.

    This is the main entry point that runs Steps 1-3 on battery entries
    and passes non-battery technologies through with a simple category
    assignment.

    Flow:
      1. Separate batteries from non-batteries.
      2. Non-batteries get ``category = technology_name`` (no filtering).
      3. Batteries go through:
           Step 1 – passes_consistency_check()
           Step 2 – categorize_battery()
           Step 3 – correct_filtered_entries()
      4. The three lists (non-batteries, valid batteries, corrected
         batteries) are merged into a single result list.

    After this function returns, every record in the list has a
    ``category`` field set.  Internal fields (``_voltage_level``,
    ``_is_natural_person``) are still present – they are stripped
    by the caller before syncing to Directus.

    Args:
        records: list of record dicts produced by map_to_directus_schema().

    Returns:
        Cleaned and categorised list of record dicts.
    """
    # ------------------------------------------------------------------
    # Partition records by technology type
    # ------------------------------------------------------------------
    batteries = [r for r in records if r.get('technology') == 'Batterie']
    non_batteries = [r for r in records if r.get('technology') != 'Batterie']

    # Non-battery technologies (Pumpspeicher, Druckluft, etc.) are not
    # subject to the plausibility pipeline – they simply get their
    # technology name as category.
    TECH_MAP = {
        'Pumpspeicher': 'Pumpspeicher',
        'Druckluft': 'Druckluft',
        'Wasserstoffspeicher': 'Wasserstoffspeicher',
        'Schwungrad': 'Schwungrad',
    }
    for r in non_batteries:
        tech = r.get('technology')
        r['category'] = TECH_MAP.get(tech, tech) if tech else None

    if not batteries:
        log("No battery entries found, skipping battery plausibility checks.")
        return non_batteries

    # ------------------------------------------------------------------
    # Step 1: Consistency filter
    # ------------------------------------------------------------------
    # Split batteries into those that pass all quality gates and those
    # that don't.  Filtered entries will be corrected in Step 3.
    valid = []
    filtered = []
    for r in batteries:
        if passes_consistency_check(r):
            valid.append(r)
        else:
            filtered.append(r)

    log(f"Consistency filter: {len(valid)} passed, {len(filtered)} filtered out of {len(batteries)} batteries")

    # ------------------------------------------------------------------
    # Step 2: Categorisation
    # ------------------------------------------------------------------
    # Entries that pass the consistency filter are classified into market
    # segments.  categorize_battery() returns None for entries that meet
    # the Grossspeicher thresholds but fail the operator-type or voltage
    # safeguards – these are "demoted" and join the filtered set.
    demoted = []
    for r in valid:
        cat = categorize_battery(r)
        if cat:
            r['category'] = cat
        else:
            # Demoted: passed consistency but failed a categorisation guard
            demoted.append(r)

    valid = [r for r in valid if r.get('category')]
    filtered.extend(demoted)

    heim = sum(1 for r in valid if r['category'] == 'Heimspeicher')
    gewerbe = sum(1 for r in valid if r['category'] == 'Gewerbespeicher')
    gross = sum(1 for r in valid if r['category'] == 'Grossspeicher')
    log(f"Categorization: {heim} Heim, {gewerbe} Gewerbe, {gross} Gross, {len(demoted)} demoted")

    # ------------------------------------------------------------------
    # Step 3: Correction
    # ------------------------------------------------------------------
    # Every filtered/demoted entry is re-added with a plausible category
    # and averaged dimensions so that no registered storage is lost.
    corrected = correct_filtered_entries(filtered, valid)
    heim_c = sum(1 for r in corrected if r['category'] == 'Heimspeicher')
    gewerbe_c = sum(1 for r in corrected if r['category'] == 'Gewerbespeicher')
    log(f"Correction: {len(corrected)} entries re-added ({heim_c} Heim, {gewerbe_c} Gewerbe)")

    # ------------------------------------------------------------------
    # Merge all three groups into the final result
    # ------------------------------------------------------------------
    result = non_batteries + valid + corrected
    log(f"Plausibility complete: {len(result)} total records")
    return result


# ============================================================================
# Directus API Helpers
# ============================================================================


def get_existing_ids(ids):
    """Query Directus to find which of the given IDs already exist."""
    existing = set()
    chunk_size = 100
    for i in range(0, len(ids), chunk_size):
        chunk = ids[i:i + chunk_size]
        try:
            response = requests.get(
                f"{DIRECTUS_URL}/items/{TABLE_NAME}",
                headers=HEADERS,
                params={
                    "filter[id][_in]": ",".join(chunk),
                    "fields": "id",
                    "limit": -1
                },
                timeout=60
            )
            if response.status_code == 200:
                data = response.json().get("data", [])
                existing.update(item["id"] for item in data)
            else:
                log(f"Error querying existing IDs: {response.status_code} - {response.text[:200]}", level="WARNING")
        except Exception as e:
            log(f"Error checking existing IDs: {e}", level="WARNING")

    log(f"Found {len(existing)} existing IDs out of {len(ids)} checked.")
    return existing


def batch_insert(batch, max_retries=3, retry_delay=2):
    """Insert multiple items in a single POST request."""
    if not batch:
        return 0

    for attempt in range(max_retries):
        try:
            response = requests.post(
                f"{DIRECTUS_URL}/items/{TABLE_NAME}",
                json=batch,
                headers=HEADERS,
                timeout=120
            )
            if response.status_code in [200, 201]:
                return len(batch)
            elif response.status_code == 503:
                log(f"Server unavailable, retry {attempt + 1}/{max_retries}...", level="WARNING")
                time.sleep(retry_delay * (2 ** attempt))
            else:
                log(f"Batch insert error: {response.status_code} - {response.text[:500]}", level="ERROR")
                return 0
        except Exception as e:
            log(f"Exception on batch insert attempt {attempt + 1}: {e}", level="WARNING")
            time.sleep(retry_delay * (2 ** attempt))

    return 0


def batch_update(batch, max_retries=3, retry_delay=2):
    """Update multiple items in a single PATCH request."""
    if not batch:
        return 0

    for attempt in range(max_retries):
        try:
            response = requests.patch(
                f"{DIRECTUS_URL}/items/{TABLE_NAME}",
                json=batch,
                headers=HEADERS,
                timeout=120
            )
            if response.status_code in [200, 204]:
                return len(batch)
            elif response.status_code == 503:
                log(f"Server unavailable, retry {attempt + 1}/{max_retries}...", level="WARNING")
                time.sleep(retry_delay * (2 ** attempt))
            else:
                log(f"Batch update error: {response.status_code} - {response.text[:500]}", level="ERROR")
                return 0
        except Exception as e:
            log(f"Exception on batch update attempt {attempt + 1}: {e}", level="WARNING")
            time.sleep(retry_delay * (2 ** attempt))

    return 0


def process_batch(batch, max_retries=3, retry_delay=2):
    """Process a batch: check which exist, then insert new and update existing."""
    if not batch:
        return 0, 0

    batch_ids = [item["id"] for item in batch]
    existing_ids = get_existing_ids(batch_ids)

    to_insert = [item for item in batch if item["id"] not in existing_ids]
    to_update = [item for item in batch if item["id"] in existing_ids]

    inserted = 0
    updated = 0

    if to_insert:
        inserted = batch_insert(to_insert, max_retries, retry_delay)
        if inserted > 0:
            log(f"Inserted {inserted} new entries.")

    if to_update:
        updated = batch_update(to_update, max_retries, retry_delay)
        if updated > 0:
            log(f"Updated {updated} existing entries.")

    return inserted, updated


def sync_to_directus(records, existing_ids=None):
    """
    Sync records to Directus.

    If ``existing_ids`` is provided (full mode), the insert/update partition
    is computed once upfront and all batches are submitted concurrently via
    a thread pool.  This avoids thousands of per-batch existence-check API
    calls and enables parallel uploads.

    If ``existing_ids`` is None (incremental mode), falls back to the
    sequential per-batch approach that checks existence on each batch.
    """
    total_inserted = 0
    total_updated = 0

    log(f"Starting sync of {len(records)} records to Directus...")

    if existing_ids is not None:
        # -- Fast path (full mode): partition once, upload concurrently --
        to_insert = [r for r in records if r['id'] not in existing_ids]
        to_update = [r for r in records if r['id'] in existing_ids]
        log(f"Pre-partitioned: {len(to_insert)} new, {len(to_update)} existing")

        # Build (operation, batch) task list
        tasks = []
        for i in range(0, len(to_insert), BATCH_SIZE):
            tasks.append(('insert', to_insert[i:i + BATCH_SIZE]))
        for i in range(0, len(to_update), BATCH_SIZE):
            tasks.append(('update', to_update[i:i + BATCH_SIZE]))

        log(f"Submitting {len(tasks)} batches to {MAX_WORKERS} workers...")

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {}
            for op, batch in tasks:
                fn = batch_insert if op == 'insert' else batch_update
                futures[pool.submit(fn, batch)] = op

            done = 0
            for future in as_completed(futures):
                op = futures[future]
                try:
                    count = future.result()
                    if op == 'insert':
                        total_inserted += count
                    else:
                        total_updated += count
                except Exception as e:
                    log(f"Batch {op} failed: {e}", level="ERROR")
                done += 1
                if done % 20 == 0 or done == len(tasks):
                    log(f"Progress: {done}/{len(tasks)} batches done")
    else:
        # -- Incremental mode: sequential with per-batch existence checks --
        for i in range(0, len(records), BATCH_SIZE):
            batch = records[i:i + BATCH_SIZE]
            batch_num = (i // BATCH_SIZE) + 1
            total_batches = math.ceil(len(records) / BATCH_SIZE)

            log(f"Processing batch {batch_num}/{total_batches} ({len(batch)} records)...")

            inserted, updated = process_batch(batch)
            total_inserted += inserted
            total_updated += updated

    return total_inserted, total_updated


def get_all_directus_ids():
    """Fetch every ID currently stored in the Directus table (paginated)."""
    all_ids = set()
    page_size = 10000
    offset = 0

    while True:
        try:
            response = requests.get(
                f"{DIRECTUS_URL}/items/{TABLE_NAME}",
                headers=HEADERS,
                params={"fields": "id", "limit": page_size, "offset": offset},
                timeout=120
            )
            if response.status_code == 200:
                data = response.json().get("data", [])
                if not data:
                    break
                all_ids.update(item["id"] for item in data)
                if len(data) < page_size:
                    break
                offset += page_size
            else:
                log(f"Error fetching IDs (offset {offset}): {response.status_code}", level="WARNING")
                break
        except Exception as e:
            log(f"Error fetching IDs: {e}", level="WARNING")
            break

    return all_ids


def delete_stale_records_by_ids(stale_ids):
    """
    Delete a pre-computed set of stale IDs from Directus in batches.

    Used when the caller has already determined which IDs are stale
    (e.g. by reusing a pre-fetched ID set from the sync step).
    """
    log(f"Deleting {len(stale_ids)} stale records...")

    deleted = 0
    stale_list = list(stale_ids)

    for i in range(0, len(stale_list), BATCH_SIZE):
        batch = stale_list[i:i + BATCH_SIZE]
        try:
            response = requests.delete(
                f"{DIRECTUS_URL}/items/{TABLE_NAME}",
                headers=HEADERS,
                json=batch,
                timeout=120
            )
            if response.status_code in [200, 204]:
                deleted += len(batch)
                log(f"Deleted batch of {len(batch)} stale records.")
            else:
                log(f"Error deleting batch: {response.status_code} - {response.text[:200]}", level="ERROR")
        except Exception as e:
            log(f"Error deleting stale records: {e}", level="ERROR")

    log(f"Deleted {deleted} stale records total.")
    return deleted


def cleanup():
    """Cleanup downloaded zip files to save disk space."""
    from pathlib import Path

    mastr_dir = Path.home() / ".open-MaStR" / "data" / "xml_download"
    if mastr_dir.exists():
        zip_files = list(mastr_dir.glob("Gesamtdatenexport_*.zip"))
        for zip_file in zip_files:
            try:
                zip_file.unlink()
                log(f"Deleted {zip_file.name} to save disk space.")
            except Exception as e:
                log(f"Could not delete {zip_file.name}: {e}", level="WARNING")

    log("Cleanup complete (database retained for future runs).")


def main():
    """
    Main entry point – orchestrates the full sync pipeline:

      1. Download raw storage data from MaStR via open-mastr
      2. Map raw columns to the Directus schema
      3. Run the Battery-Charts plausibility pipeline (filter, categorise, correct)
      4. Upsert cleaned records to Directus
      5. (Full mode only) Delete stale records from Directus
    """
    start_time = time.time()
    mode = "full" if FULL_LOAD else "incremental"

    slack_log(f"Storage-Sync gestartet (Modus: {mode}).", level="INFO")

    try:
        # Step 1: Download raw storage data from MaStR.
        # Returns a DataFrame with one row per generation unit, plus the
        # open-mastr DB handle.  In incremental mode only recently
        # updated entries are loaded.
        df, db = download_storage_data()

        if len(df) == 0:
            log("No entries to sync.")
            slack_log("Keine Speicher-Eintraege zum Synchronisieren.", level="INFO")
            return

        # Step 2: Translate raw MaStR column names to Directus field
        # names and coerce types.  Also attaches internal fields
        # (_voltage_level, _is_natural_person) for the plausibility step.
        records = map_to_directus_schema(df)
        log(f"Prepared {len(records)} records for Directus sync.")

        # Step 3: Run the three-step Battery-Charts plausibility pipeline.
        # After this call every record has a 'category' field.
        records = apply_plausibility_checks(records)

        # Remove internal fields (prefixed with _) that were only needed
        # for the plausibility checks – Directus doesn't have these columns.
        for record in records:
            for key in [k for k in record if k.startswith('_')]:
                del record[key]

        # Step 4: Upsert records to Directus (insert new, update existing).
        # In full mode, pre-fetch all existing IDs once to avoid thousands
        # of per-batch existence-check API calls and enable concurrent uploads.
        existing_ids = None
        if FULL_LOAD:
            existing_ids = get_all_directus_ids()
            log(f"Pre-fetched {len(existing_ids)} existing IDs from Directus.")

        total_inserted, total_updated = sync_to_directus(records, existing_ids=existing_ids)

        # Step 5 (full mode only): Delete records from Directus whose IDs
        # are no longer present in MaStR.  We reuse the pre-fetched ID set
        # to avoid another full table scan.
        total_deleted = 0
        if FULL_LOAD:
            raw_ids = set(r['id'] for r in records if r.get('id'))
            # Stale = IDs in Directus before sync that are not in the raw data.
            # (Newly inserted IDs are a subset of raw_ids, so the math works.)
            stale_ids = existing_ids - raw_ids
            if stale_ids:
                total_deleted = delete_stale_records_by_ids(stale_ids)
            else:
                log("No stale records to delete.")

        duration = round(time.time() - start_time)

        slack_log(
            f"Storage-Sync abgeschlossen in {duration}s\n"
            f"- Modus: {mode}\n"
            f"- Geladen: {len(df)}\n"
            f"- Nach Plausibilitaet: {len(records)}\n"
            f"- Eingefuegt: {total_inserted}\n"
            f"- Aktualisiert: {total_updated}"
            + (f"\n- Geloescht: {total_deleted}" if FULL_LOAD else ""),
            level="SUCCESS"
        )

        log(f"Sync completed in {duration}s. Inserted: {total_inserted}, Updated: {total_updated}"
            + (f", Deleted: {total_deleted}" if FULL_LOAD else ""))

    except Exception as e:
        slack_log(f"Fehler beim Storage-Sync: {e}", level="ERROR")
        log(f"Error: {e}", level="ERROR")
        raise

    finally:
        cleanup()


if __name__ == "__main__":
    main()
