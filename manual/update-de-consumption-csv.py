"""
Rebuild de/emissions/emissions_consumption_based.csv (chart: consumptionBasedEmissions, DE).

  production_based  <- UBA, Treibhausgasemissionen nach UNFCCC, CO2, TOTAL_WITHOUT_LULUCF
                       (the UBA data-portal export used by uba-de-ghg-import.py,
                       e.g. "UBA,DF_CLIMATE_EMISSIONS_GHG_TRENDS,2026.1+all.csv")
  consumption_based <- Our World in Data, "Consumption-based CO2 emissions"
                       (Global Carbon Budget), Germany; https://ourworldindata.org/grapher/consumption-co2-emissions
                       Years OWID does not have yet stay empty.

Both in tonnes CO2, semicolon separated: year;unit;consumption_based;production_based.
Every year is rewritten, because both sources revise earlier years.

Usage:
  python3 manual/update-de-consumption-csv.py --uba <uba export csv> \
      --owid <owid csv, e.g. ...consumption-co2-emissions.csv?csvType=full&useColumnShortNames=true> \
      --out emissions_consumption_based.csv
"""
import argparse
import csv
import pandas as pd

p = argparse.ArgumentParser()
p.add_argument("--uba", required=True)
p.add_argument("--owid", required=True)
p.add_argument("--out", required=True)
a = p.parse_args()

uba = pd.read_csv(a.uba, low_memory=False)
prod = (uba[(uba.D_SUBSTANCES == "CO2") & (uba.D_SOURCE_CATEGORIES == "TOTAL_WITHOUT_LULUCF")
            & (uba.D_UNIT == "MT")].set_index("TIME_PERIOD")["OBS_VALUE"] * 1e6)
owid = pd.read_csv(a.owid)
cons = owid[owid.code == "DEU"].set_index("year")["consumption_emissions"]

with open(a.out, "w", newline="") as f:
    w = csv.writer(f, delimiter=";", lineterminator="\n")
    w.writerow(["year", "unit", "consumption_based", "production_based"])
    for y in sorted(prod.index):
        w.writerow([y, "CO2",
                    int(round(cons[y])) if y in cons.index else "",
                    int(round(prod[y]))])
print(f"{a.out}: {prod.index.min()}-{prod.index.max()}, consumption to {cons.index.max()}")
