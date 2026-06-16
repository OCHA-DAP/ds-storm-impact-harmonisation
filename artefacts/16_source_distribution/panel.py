"""Build the harmonized three-source exposure panel (adm0 + adm1).

Pipeline per admin level:
  1. fetch CHD / GDACS / ADAM storm-final exposure (queries.py).
  2. split orphan rows (unit IS NULL, adm1 only) out for reporting.
  3. outer-merge the matched rows on (atcf_id, iso3, unit, wind_speed_kt).
  4. record per-source presence (did the source have a row *before* the
     fill?) so downstream can tell a computed-0 from a join-filled-0.
  5. fillna(0.0) on the three pop columns — the agreed zero policy:
     wherever ANY source reports a unit, sources missing it get 0.
  6. attach FM names (adm1) and season.

Outputs (gitignored `out/`):
  panel_adm0.parquet, panel_adm1.parquet  — the merged panels.
  orphans_adm1.parquet                     — unmatched GDACS/ADAM rows.

Run:  python panel.py   (needs DSCI_AZ_* env + dev DB access)

Key columns in each panel:
  atcf_id, iso3, unit, wind_speed_kt, season,
  chd_pop, gdacs_pop, adam_pop                 (zero-filled),
  chd_present, gdacs_present, adam_present     (bool, pre-fill),
  n_gdacs_admins, n_adam_admins, gdacs_caveat, adam_caveat,
  fm_name (adm1; adm0 uses iso3 as the unit name).
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

import queries as q

OUT = Path(__file__).parent / "out"
KEY = ["atcf_id", "iso3", "unit", "wind_speed_kt"]
POP = ["chd_pop", "gdacs_pop", "adam_pop"]


def build_panel(engine, level: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return (panel, orphans) for one admin level."""
    aids = q.qualifying_storms(engine)
    season = aids.set_index("atcf_id")["season"]
    aid_list = aids["atcf_id"].tolist()

    chd = q.fetch_chd(engine, aid_list, level)
    gd = q.fetch_gdacs(engine, aid_list, level)
    ad = q.fetch_adam(engine, aid_list, level)

    # adm1 orphans: source rows whose admin didn't match an FM unit. They
    # carry real population we must report, but have no unit to merge on.
    orphans = pd.concat([
        gd[gd["unit"].isna()].assign(source="gdacs")
          .rename(columns={"gdacs_pop": "pop_exposed"}),
        ad[ad["unit"].isna()].assign(source="adam")
          .rename(columns={"adam_pop": "pop_exposed"}),
    ], ignore_index=True)[
        ["source", "atcf_id", "iso3", "wind_speed_kt", "pop_exposed"]
    ] if level == 1 else pd.DataFrame(
        columns=["source", "atcf_id", "iso3", "wind_speed_kt", "pop_exposed"])

    gd_m = gd[gd["unit"].notna()]
    ad_m = ad[ad["unit"].notna()]

    panel = (
        chd.merge(gd_m[KEY + ["gdacs_pop", "n_gdacs_admins", "gdacs_caveat"]],
                  on=KEY, how="outer")
           .merge(ad_m[KEY + ["adam_pop", "n_adam_admins", "adam_caveat"]],
                  on=KEY, how="outer")
    )

    # Presence = had a row before fill (explicit 0 counts as present;
    # absent → NaN → present=False → will be a join-filled 0).
    for src, col in [("chd", "chd_pop"), ("gdacs", "gdacs_pop"),
                     ("adam", "adam_pop")]:
        panel[f"{src}_present"] = panel[col].notna()
    panel[POP] = panel[POP].fillna(0.0)

    panel["season"] = panel["atcf_id"].map(season)
    panel["admin_level"] = level
    if level == 1:
        panel = panel.merge(q.fm_names(engine, level), on="unit", how="left")
    else:
        panel["fm_name"] = panel["unit"]  # adm0 unit is iso3

    names = q.storm_names(engine)
    panel = panel.merge(names[["atcf_id", "storm_name"]], on="atcf_id", how="left")

    return panel, orphans


# ── tidy, user-facing export (both levels, *_exposure column names) ────

TIDY_COLS = ["storm_id", "storm_name", "season", "admin_level", "iso3",
             "admin_pcode", "admin_name", "wind_speed_kt",
             "chd_exposure", "gdacs_exposure", "adam_exposure"]


def tidy(panel: pd.DataFrame) -> pd.DataFrame:
    """Rename the analytic panel into the user-facing shape: one row per
    (storm, admin unit, wind threshold) with the three source exposures."""
    out = panel.rename(columns={
        "atcf_id": "storm_id", "unit": "admin_pcode", "fm_name": "admin_name",
        "chd_pop": "chd_exposure", "gdacs_pop": "gdacs_exposure",
        "adam_pop": "adam_exposure",
    })[TIDY_COLS]
    return out.sort_values(
        ["season", "storm_id", "iso3", "admin_pcode", "wind_speed_kt"]
    ).reset_index(drop=True)


def main():
    OUT.mkdir(exist_ok=True)
    engine = q.get_engine("dev")
    tidy_all = []
    for level in (0, 1):
        panel, orphans = build_panel(engine, level)
        panel.to_parquet(OUT / f"panel_adm{level}.parquet", index=False)
        if level == 1:
            orphans.to_parquet(OUT / "orphans_adm1.parquet", index=False)

        td = tidy(panel)
        td.to_csv(OUT / f"exposure_adm{level}.csv", index=False)
        td.to_parquet(OUT / f"exposure_adm{level}.parquet", index=False)
        tidy_all.append(td)

        c = panel[panel["wind_speed_kt"].isin(q.COMMON_KT)]
        pres = panel[["chd_present", "gdacs_present", "adam_present"]].sum()
        print(f"\nadm{level}: {len(panel)} rows | units={panel['unit'].nunique()}"
              f" countries={panel['iso3'].nunique()} storms={panel['atcf_id'].nunique()}")
        print(f"  present-rows: chd={int(pres.chd_present)}"
              f" gdacs={int(pres.gdacs_present)} adam={int(pres.adam_present)}")
        if level == 1:
            print(f"  orphan rows (reported, not merged): {len(orphans)}"
                  f" | orphan pop = {orphans['pop_exposed'].sum():,.0f}")
        print(f"  [34&64kt] {len(c)} rows; all-three-present="
              f"{int((c.chd_present & c.gdacs_present & c.adam_present).sum())}")

    combined = pd.concat(tidy_all, ignore_index=True)
    combined.to_csv(OUT / "exposure_all.csv", index=False)
    combined.to_parquet(OUT / "exposure_all.parquet", index=False)
    print(f"\nwrote panels + tidy exposure_adm{{0,1}}/all (csv+parquet) → {OUT}")


if __name__ == "__main__":
    main()
