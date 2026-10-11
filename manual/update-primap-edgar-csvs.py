"""
Rebuild the two static CSVs on data.klimadashboard.org that have no automated
update path, from the original sources:

  1. {AT,DE}_Historical-Emissions_PIK-PRIMAP.csv   (charts: historicalEmissions,
     co2budgetHistorical)  <- PRIMAP-hist, Zenodo 10.5281/zenodo.22876287 (v2.8)
  2. global/emissions/emissions_global.csv          (chart: globalEmissions)
     <- EDGAR GHG booklet, sheet GHG_per_capita_by_country

The script only writes files to --out; it does not upload anything. The data
server is the source of truth for data.klimadashboard.org (it syncs *to* the
GitHub repo), so the generated files have to be copied there.

Conventions kept from the files currently online:
  PRIMAP: scenario HISTCR, entity KYOTOGHG (AR6GWP100), Gg -> Mt, 3 significant
          digits, comma separated, columns
          year,energy_ipc1_Mt_CO2e,industrial_processes_and_product_use_ipc2_Mt_CO2e,
          waste_ipc4_Mt_CO2e,other_ipc5_Mt_CO2e,total_AR6_Mt_CO2e,
          agriculture_and_livestock_ipcmag_Mt_CO2e
          (total = category M.0.EL, agriculture = M.AG)
  EDGAR:  semicolon separated, state_iso;state_name;year;co2e_percapita,
          EU27 -> EU, GLOBAL TOTAL -> WORLD; ISO2 codes and row order are taken
          from the currently online file, new countries are appended.

Usage:
  python3 manual/update-primap-edgar-csvs.py \
      --primap Guetschow_et_al_2026-PRIMAP-hist_v2.8_final_22-Sep-2026.csv \
      --area AUT --primap-out AT_Historical-Emissions_PIK-PRIMAP.csv
  python3 manual/update-primap-edgar-csvs.py \
      --edgar EDGAR_2026_GHG_booklet_2026.xlsx \
      --edgar-current emissions_global.csv --edgar-out emissions_global.csv
"""
import argparse
import csv
import pandas as pd

PRIMAP_COLUMNS = [
    ("energy_ipc1_Mt_CO2e", "1"),
    ("industrial_processes_and_product_use_ipc2_Mt_CO2e", "2"),
    ("waste_ipc4_Mt_CO2e", "4"),
    ("other_ipc5_Mt_CO2e", "5"),
    ("total_AR6_Mt_CO2e", "M.0.EL"),
    ("agriculture_and_livestock_ipcmag_Mt_CO2e", "M.AG"),
]


def sig3(x: float) -> str:
    return f"{float(f'{x:.3g}'):g}"


def build_primap(src: str, area: str, out: str) -> None:
    df = pd.read_csv(src)
    sel = df[
        (df["area (ISO3)"] == area)
        & (df["entity"] == "KYOTOGHG (AR6GWP100)")
        & (df["scenario (PRIMAP-hist)"] == "HISTCR")
    ].set_index("category (IPCC2006_PRIMAP)")
    years = [c for c in sel.columns if c.isdigit()]
    with open(out, "w", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(["year"] + [name for name, _ in PRIMAP_COLUMNS])
        for y in years:
            vals = [sel.loc[cat, y] / 1000 for _, cat in PRIMAP_COLUMNS]  # Gg -> Mt
            if any(pd.isnull(v) for v in vals):
                continue
            w.writerow([y] + [sig3(v) for v in vals])
    print(f"PRIMAP {area}: {years[0]}-{years[-1]} -> {out}")


def build_edgar(src: str, current: str, out: str) -> None:
    sheet = pd.read_excel(src, sheet_name="GHG_per_capita_by_country")
    year = max(c for c in sheet.columns if isinstance(c, int))
    sheet = sheet.dropna(subset=["Country"])
    values = dict(zip(sheet["Country"], sheet[year]))
    values["World"] = values.pop("GLOBAL TOTAL")

    cur = pd.read_csv(current, sep=";", keep_default_na=False)  # "NA" is Namibia
    rows, seen = [], set()
    for _, r in cur.iterrows():
        name = r["state_name"]
        if name in values and pd.notnull(values[name]):
            rows.append((r["state_iso"], name, values[name]))
            seen.add(name)
        else:
            print(f"  dropped (no value in new booklet): {name}")
    for name, v in values.items():
        if name not in seen and pd.notnull(v):
            print(f"  NEW country without ISO2 mapping, add by hand: {name}")
    with open(out, "w", newline="") as f:
        w = csv.writer(f, delimiter=";", lineterminator="\n")
        w.writerow(["state_iso", "state_name", "year", "co2e_percapita"])
        for iso, name, v in rows:
            w.writerow([iso, name, year, v])
    print(f"EDGAR: {len(rows)} rows, year {year} -> {out}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--primap"); p.add_argument("--area"); p.add_argument("--primap-out")
    p.add_argument("--edgar"); p.add_argument("--edgar-current"); p.add_argument("--edgar-out")
    a = p.parse_args()
    if a.primap:
        build_primap(a.primap, a.area, a.primap_out)
    if a.edgar:
        build_edgar(a.edgar, a.edgar_current, a.edgar_out)
