"""Build the harmonised tropical-cyclone population-exposure workbook (xlsx).

Self-contained: pulls live from the dev database via `src.source_exposure`
(the same `build_exposure` builders the book chapters use) and writes a styled
five-sheet workbook (README, storms, adm0_exposure, adm1_exposure, caveats). No
artefact dependency.

Run:
    uv run python scripts/build_workbook.py

Output (gitignored):
    src/source_exposure/out/historical_tropical_cyclone_pop_exposure_estimates_AL_EP_basins.xlsx
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.source_exposure import queries as q  # noqa: E402
from src.source_exposure import style  # noqa: E402
from src.source_exposure.workbook import (  # noqa: E402
    MIN_SEASON,
    _country_resolver,
    build_caveats,
    build_exposure,
    build_storms_tab,
)

OUT = Path(__file__).resolve().parents[1] / "src" / "source_exposure" / "out"


def _readme_blocks(storms, adm0, adm1, caveats, generated):
    B = lambda k, t: (k, t)  # noqa: E731 (terse block builder)
    return [
        B("title", "Storm Exposure Source Comparison"),
        B("subtitle", "CHD vs GDACS vs ADAM — population exposure by storm, "
          "admin 0 & admin 1"),
        B("meta", f"OCHA Data Science Unit  ·  generated {generated}  ·  dev "
          f"database  ·  NHC seasons {MIN_SEASON}–2026"),
        B("gap", ""),
        B("h2", "What this workbook is"),
        B("body", "Three independent estimates of storm-driven population "
          "exposure, harmonised onto common storm IDs and FieldMaps admin "
          "units, compared on each storm's final (“last-ballot”) "
          "estimate."),
        B("gap", ""),
        B("h2", "Tabs"),
        B("bullet", f"storms — every NHC storm from {MIN_SEASON} on "
          f"({len(storms)} storms): name, season, basin, GDACS/ADAM cross-ids, "
          f"which sources report exposure, which have the storm on record, and "
          f"a note (note_gdacs_adam) explaining every GDACS/ADAM gap. The storm "
          f"key on every tab is atcf_id, the NHC ATCF identifier (e.g. "
          f"AL132025)."),
        B("bullet", f"adm0_exposure — country level ({len(adm0):,} rows): one "
          f"row per storm × country × wind threshold."),
        B("bullet", f"adm1_exposure — subnational FieldMaps units "
          f"({len(adm1):,} rows), plus GDACS/ADAM source-side admin names for "
          f"match QA."),
        B("bullet", f"caveats — adm1 alignment policy + reviewer notes "
          f"({len(caveats):,} rows) from the FieldMaps lookup's own caveat "
          f"system: which countries are national-only (subnational from CHD), "
          f"plus the reviewer's detailed `note` per unit. PROVISIONAL — the "
          f"lookup notes are not yet fully reviewed."),
        B("gap", ""),
        B("h2", "The three sources"),
        B("bullet", "CHD — our NHC-derived estimate: final observed exposure "
          "(the realized track footprint, latest valid_time per unit)."),
        B("bullet", "GDACS — JRC global disaster footprint."),
        B("bullet", "ADAM — WFP; ingests GDACS upstream, so ADAM ≈ GDACS by "
          "construction (adam_eventid = gdacs_eventid)."),
        B("bullet", "All three are each storm's FINAL converged estimate — the "
          "value ramps as the footprint establishes, then plateaus at the "
          "realized value. CHD is the observed track footprint (no forecast "
          "term added); GDACS/ADAM are their final per-episode footprint. "
          "Like-for-like: final assessed exposure, no forecast component on any "
          "side."),
        B("gap", ""),
        B("h2", "Reading the exposure values  (zero vs blank)"),
        B("bullet", "CHD is never blank — it is our NHC database, so a missing "
          "value for a storm we have is a true 0."),
        B("bullet", "GDACS / ADAM show 0 only when the source reported the "
          "storm (has exposure for it). Where a source is blank, the storms "
          "tab's note_gdacs_adam says WHY: reported zero, final footprint "
          "unservable (GDACS server error on the final episode), WFP access "
          "denied (403), or no record at all."),
        B("bullet", "sources lists a source when it has a non-blank value here "
          "(a reported source at 0 counts; a blank source does not)."),
        B("gap", ""),
        B("h2", "Wind thresholds"),
        B("bullet", "34 and 64 kt are common to all three; 50 kt exists only "
          "for CHD and ADAM (GDACS has no 50 kt)."),
        B("gap", ""),
        B("h2", "Caveats"),
        B("bullet", "A small share of admin-1 units could not be matched to "
          "FieldMaps boundaries. This and other harmonisation notes are "
          "recorded in the adm1_caveat column of the adm1_exposure tab."),
    ]


_STORM_W = {"storm_name": 18, "basin": 15, "gdacs_eventid": 14,
            "adam_eventid": 14, "sources_with_record": 22,
            "sources_reporting_exposure": 24, "note_gdacs_adam": 64}
_EXP_W = {"storm_name": 18, "country_name": 22, "admin_name": 26,
          "sources": 17, "chd_exposure": 14, "gdacs_exposure": 14,
          "adam_exposure": 14, "gdacs_admin1_name": 34, "adam_admin1_name": 34,
          "alt_adm1_name": 40, "adm1_caveat": 30}
_MONEY = ["chd_exposure", "gdacs_exposure", "adam_exposure"]


def main():
    OUT.mkdir(exist_ok=True)
    engine = q.get_engine("dev")
    aids = q.all_nhc_storms(engine, MIN_SEASON)["atcf_id"].tolist()
    print(f"master storms (season>={MIN_SEASON}): {len(aids)}")

    chd_rep, gdacs_rep, adam_rep = q.storms_with_source_exposure(engine, aids)
    print(f"reported storms — chd:{len(chd_rep)} gdacs:{len(gdacs_rep)} "
          f"adam:{len(adam_rep)}")

    resolve = _country_resolver(engine)
    adm0 = build_exposure(engine, 0, aids, gdacs_rep, adam_rep, resolve)
    adm1 = build_exposure(engine, 1, aids, gdacs_rep, adam_rep, resolve)
    storms = build_storms_tab(engine, chd_rep, gdacs_rep, adam_rep)
    caveats = build_caveats(engine, resolve)

    path = OUT / "historical_tropical_cyclone_pop_exposure_estimates_AL_EP_basins.xlsx"
    with pd.ExcelWriter(path, engine="openpyxl") as xl:
        storms.to_excel(xl, sheet_name="storms", index=False)
        adm0.to_excel(xl, sheet_name="adm0_exposure", index=False)
        adm1.to_excel(xl, sheet_name="adm1_exposure", index=False)
        caveats.to_excel(xl, sheet_name="caveats", index=False)

        wb = xl.book
        readme = wb.create_sheet("README", 0)
        generated = datetime.now().strftime("%Y-%m-%d")
        style.build_readme(
            readme, _readme_blocks(storms, adm0, adm1, caveats, generated))

        style.style_data_sheet(
            wb["storms"], plain_cols=["gdacs_eventid", "adam_eventid", "season"],
            widths=_STORM_W,
            # Visible: atcf_id, storm_name, season, basin, sources_with_record,
            # note_gdacs_adam. Everything else kept for filtering but hidden.
            hidden=["gdacs_eventid", "adam_eventid", "sources_reporting_exposure",
                    "has_chd_exposure", "has_gdacs_exposure",
                    "has_adam_exposure", "gdacs_status", "adam_status"])
        style.style_data_sheet(
            wb["adm0_exposure"], money_cols=_MONEY,
            plain_cols=["season", "wind_speed_kt"], widths=_EXP_W,
            hidden=["admin_level"])
        style.style_data_sheet(
            wb["adm1_exposure"], money_cols=_MONEY,
            plain_cols=["season", "wind_speed_kt"], widths=_EXP_W,
            hidden=["admin_level", "gdacs_admin1_name", "adam_admin1_name"])
        style.style_data_sheet(
            wb["caveats"], widths={"country_name": 22, "scope": 24,
                                   "adm1_alignment": 40, "caveat_kind": 22,
                                   "caveat_note": 60, "note": 90})
        wb.active = 0

    print(f"  storms tab : {len(storms)} rows")
    print(f"  adm0 tab   : {len(adm0)} rows | adm1 tab: {len(adm1)} rows")
    print(f"  caveats    : {len(caveats)} country rows | "
          f"{dict(caveats['adm1_alignment'].value_counts())}")
    print(f"→ {path}")


if __name__ == "__main__":
    main()
