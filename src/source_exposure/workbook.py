"""Build the 3-tab Excel workbook: storms / adm0 exposure / adm1 exposure.

Tabs:
  1. `storms`        — every NHC storm from MIN_SEASON on (operational era,
     even no-exposure ones), with name/season/basin, GDACS+ADAM cross-ids,
     and which sources actually report exposure (has_* flags + `sources`).
  2. `adm0_exposure` — one row per (storm, country, wind threshold): the three
     source exposures, zero-filled, with a `sources` column.
  3. `adm1_exposure` — same at subnational FM units, plus the source-side admin
     names (`gdacs_admin1_name`, `adam_admin1_name`) and a collapsed
     `alt_adm1_name` for match QA against the canonical FM `admin_name`.

CHD exposure here = observed-final + forecast-only-final (see queries.fetch_chd).

Run (from the repo root):  python -m src.source_exposure.workbook
  → out/historical_tropical_cyclone_pop_exposure_estimates_AL_EP_basins.xlsx
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd

from src.source_exposure import fm_matching as fm
from src.source_exposure import queries as q
from src.source_exposure import source_diagnostics as sd
from src.source_exposure import style

OUT = Path(__file__).parent / "out"
MIN_SEASON = 2001          # exposure era; widen toward 1954 for full catalog
KEY = ["atcf_id", "iso3", "unit", "wind_speed_kt"]

def _sources_label(chd: bool, gd: bool, ad: bool) -> str:
    parts = [n.upper() for n, f in
             [("chd", chd), ("gdacs", gd), ("adam", ad)] if f]
    return "+".join(parts) if parts else "none"


def _country_resolver(engine):
    """Return iso3 → country-name function: FieldMaps lookup name first
    (consistent with the in-scope set, e.g. 'United States of America'),
    then pycountry for everything else (PRI → Puerto Rico, IRL → Ireland),
    then the bare iso3 for non-standard codes (CPT, XIM, XMA)."""
    import pycountry
    fm = {r.unit: r.fm_name for r in q.fm_names(engine, 0).itertuples()
          if r.fm_name and r.fm_name != r.unit}

    def resolve(iso3):
        if iso3 in fm:
            return fm[iso3]
        c = pycountry.countries.get(alpha_3=iso3) if iso3 else None
        return c.name if c else iso3
    return resolve


def build_exposure(engine, level: int, aids: list[str],
                  gdacs_reports: set, adam_reports: set, resolve):
    """Return the tidy panel for one admin level. `gdacs_reports` /
    `adam_reports` are the sets of atcf_ids each source REPORTED (has any
    exposure for) — they drive the zero-vs-blank fill. `resolve` maps iso3
    → country name."""
    chd = q.fetch_chd(engine, aids, level)
    gd = q.fetch_gdacs(engine, aids, level)
    ad = q.fetch_adam(engine, aids, level)

    gd_m = gd[gd["unit"].notna()]
    ad_m = ad[ad["unit"].notna()]
    panel = (
        chd.merge(gd_m[KEY + ["gdacs_pop", "gdacs_admin1_name"]],
                  on=KEY, how="outer")
           .merge(ad_m[KEY + ["adam_pop", "adam_admin1_name"]],
                  on=KEY, how="outer")
    )

    meta = q.all_nhc_storms(engine, MIN_SEASON)[
        ["atcf_id", "storm_name", "season"]]
    panel = panel.merge(meta, on="atcf_id", how="left")

    # Zero-vs-NaN fill — the five-case rule, decided per (storm, threshold):
    #   CHD : own NHC DB → a missing value is a genuine 0.                 [always]
    #   ADAM: 0 for units a positive-reporting storm didn't list.
    #   GDACS: fill 0 ONLY when we can defend it, else keep NaN:
    #     1 positive value            → use it
    #     2 COMPUTED threshold,
    #       country absent            → 0  (not in footprint)
    #     3 GENUINE zero (timeline
    #       total = 0)                → 0  (storm exposed nobody)
    #     4 per-country MISSING
    #       (timeline total > 0, or
    #       an explicit -1 cell)      → NaN (GDACS has a total but no breakdown)
    #     5 storm not tracked         → NaN
    #   The timeline total is the ONLY signal separating case 3 from case 4.
    panel["chd_pop"] = panel["chd_pop"].fillna(0.0)
    ad_rep = panel["atcf_id"].isin(adam_reports)
    panel.loc[ad_rep, "adam_pop"] = panel.loc[ad_rep, "adam_pop"].fillna(0.0)

    computed = q.gdacs_computed_thresholds(engine, aids)   # (atcf, kt) positive
    null_cells = q.gdacs_null_cells(engine, aids)          # (atcf, iso3, kt) = -1
    tl = q.load_timeline_totals()                          # atcf → pop39/pop74 max
    _KTCOL = {34: "timeline_pop39_max", 64: "timeline_pop74_max"}

    def _gd_fill0(atcf, kt):
        if (atcf, int(kt)) in computed:        # case 2: absent country → 0
            return True
        col = _KTCOL.get(int(kt))              # 50 kt: GDACS has no buffer → NaN
        if col and atcf in tl.index:
            t = tl.loc[atcf, col]
            if pd.notna(t):
                return float(t) == 0.0         # case 3 → 0 ; case 4 → NaN
        return False                           # not tracked / no timeline → NaN

    fill = pd.Series(
        [_gd_fill0(a, k) for a, k in zip(panel["atcf_id"], panel["wind_speed_kt"])],
        index=panel.index)
    # case 4 at cell level: an explicit -1 stays NaN ONLY inside a COMPUTED storm
    # (a real footprint with one country's value missing). In a GENUINE-zero storm
    # the total is 0, so even a -1 cell is a true 0 — don't exclude it there.
    nullcell = pd.Series(
        [((a, i, k) in null_cells) and ((a, int(k)) in computed) for a, i, k in
         zip(panel["atcf_id"], panel["iso3"], panel["wind_speed_kt"])],
        index=panel.index)
    fill &= ~nullcell
    panel.loc[fill, "gdacs_pop"] = panel.loc[fill, "gdacs_pop"].fillna(0.0)

    # `sources` = sources with a NON-BLANK value here (a reported source at
    # 0 IS listed; a source that never reported the storm is NOT).
    panel["sources"] = [
        _sources_label(c, g, a)
        for c, g, a in zip(panel["chd_pop"].notna(), panel["gdacs_pop"].notna(),
                           panel["adam_pop"].notna())]

    panel["admin_level"] = level
    # admin_name from the FieldMaps lookup (unit name at adm1; at adm0 the
    # unit is the iso3), falling back to the pcode where the lookup has none.
    panel = panel.merge(q.fm_names(engine, level), on="unit", how="left")
    panel["fm_name"] = panel["fm_name"].fillna(panel["unit"])
    # country_name (all levels): resolved iso3 → country name.
    iso_map = {i: resolve(i) for i in panel["iso3"].dropna().unique()}
    panel["country_name"] = panel["iso3"].map(iso_map)
    if level == 0:
        panel["fm_name"] = panel["country_name"]  # adm0 admin_name = country
    if level == 1:
        panel["alt_adm1_name"] = [
            " | ".join(p for p in [
                f"ADAM: {a}" if pd.notna(a) else None,
                f"GDACS: {g}" if pd.notna(g) else None] if p) or None
            for a, g in zip(panel["adam_admin1_name"], panel["gdacs_admin1_name"])
        ]
        # Compact per-unit comparability flag, sourced from the LOOKUP per FM
        # unit (its structural caveat_kind) — NOT from the per-storm exposure
        # join — so every unit shows its true status consistently, including
        # fm_adm1_only / no_*_at_adm1 units the source never reports. Blank =
        # clean 1:1 match. Long notes live in the caveats tab.
        cav = q.adm1_unit_caveats(engine)
        panel = panel.merge(cav, on="unit", how="left")

        # Per source: if the source had NO event for this storm, that (not the
        # unit's structural caveat) is why it's blank → "no event reported".
        # Only when the source DID report the storm does the unit-level
        # structural caveat apply. Reported + clean → no caveat.
        gd_no_event = ~panel["atcf_id"].isin(gdacs_reports)
        ad_no_event = ~panel["atcf_id"].isin(adam_reports)

        def _src_cav(no_event, kind):
            if no_event:
                return "no event reported"
            return fm.label_caveat(kind)
        gd_parts = [_src_cav(ne, k)
                    for ne, k in zip(gd_no_event, panel["gdacs_caveat"])]
        ad_parts = [_src_cav(ne, k)
                    for ne, k in zip(ad_no_event, panel["adam_caveat"])]
        panel["adm1_caveat"] = [
            " | ".join(p for p in [
                f"GDACS: {g}" if g else None,
                f"ADAM: {a}" if a else None] if p) or None
            for g, a in zip(gd_parts, ad_parts)
        ]
        # Blank a source's adm1 exposure wherever the source CANNOT resolve
        # the unit, so we never report a number it can't stand behind:
        #   - aggregated_in_*  : one coarse source polygon replicated across N
        #     FM units → unattributable + double-counts on sum.
        #   - fm_adm1_only / no_*_at_adm1 : source has no comparable adm1
        #     boundary → its "0" is a reporting-fill artefact, not a measure.
        # The valid many→one direction (aggregating_from_*) and clean matches
        # keep their real values. Combined-region totals remain at adm0.
        panel.loc[panel["gdacs_caveat"].isin(fm.BLANK_KINDS_GDACS),
                  "gdacs_pop"] = float("nan")
        panel.loc[panel["adam_caveat"].isin(fm.BLANK_KINDS_ADAM),
                  "adam_pop"] = float("nan")
        # presence changed → recompute `sources` so a blanked source drops out.
        panel["sources"] = [
            _sources_label(c, g, a)
            for c, g, a in zip(panel["chd_pop"].notna(),
                               panel["gdacs_pop"].notna(),
                               panel["adam_pop"].notna())]

    panel = panel.rename(columns={
        "atcf_id": "storm_id", "unit": "admin_pcode", "fm_name": "admin_name",
        "chd_pop": "chd_exposure", "gdacs_pop": "gdacs_exposure",
        "adam_pop": "adam_exposure"})

    if level == 1:
        base = ["storm_id", "storm_name", "season", "admin_level", "iso3",
                "country_name", "admin_pcode", "admin_name", "wind_speed_kt",
                "sources", "chd_exposure", "gdacs_exposure", "adam_exposure"]
        extra = ["gdacs_admin1_name", "adam_admin1_name", "alt_adm1_name",
                 "adm1_caveat"]
    else:
        # adm0: admin_pcode == iso3, so drop it as redundant.
        base = ["storm_id", "storm_name", "season", "admin_level", "iso3",
                "admin_name", "wind_speed_kt", "sources",
                "chd_exposure", "gdacs_exposure", "adam_exposure"]
        extra = []
    panel = panel.sort_values(
        ["season", "storm_id", "iso3", "admin_pcode", "wind_speed_kt"])
    return panel[base + extra].reset_index(drop=True)


_CAVEAT_COLS = ["source", "iso3", "country_name", "scope", "adm1_alignment",
                "caveat_kind", "caveat_note", "note"]

# caveat_kind → readable adm1 alignment policy (the existing controlled
# vocabulary from ds-storms-pipeline's lookup builder).
_ALIGN = {
    "country_only": "national-only (adm1 from CHD)",
    "no_fm_source": "national-only (no FieldMaps boundary)",
    "no_adam_source": "national-only (no FieldMaps boundary)",
    "fm_adm1_only": "national-only (source has no comparable adm1)",
    "needs_manual_mapping": "partial / manual (boundary-vintage mismatch)",
}


def build_caveats(engine, resolve) -> pd.DataFrame:
    """Caveat tab straight from the lookups' own system: country-level adm1
    policy (`scope = (country policy)`) plus the per-unit adm1 rows carrying
    a reviewer `note`. `adm1_alignment` is the readable policy, `caveat_note`
    the terse note, `note` the reviewer's detailed reasoning."""
    c = q.lookup_caveats(engine)
    if c.empty:
        return pd.DataFrame(columns=_CAVEAT_COLS)
    c["country_name"] = c["iso3"].map(
        {i: resolve(i) for i in c["iso3"].unique()})
    c["adm1_alignment"] = c["caveat_kind"].map(
        lambda k: _ALIGN.get(k) or fm.CAVEAT_LABELS.get(k) or "see note")
    return c.sort_values(
        ["source", "iso3", "admin_level", "scope"]
    )[_CAVEAT_COLS].reset_index(drop=True)


def build_storms_tab(engine, chd_rep, gdacs_rep, adam_rep) -> pd.DataFrame:
    storms = q.all_nhc_storms(engine, MIN_SEASON)
    # has_*_exposure = the source produced EXPOSURE for this storm (NOT
    # "is in that database"). Every storm here is already an NHC/CHD-catalog
    # storm; this flag is False for storms that simply had no exposure footprint.
    storms["has_chd_exposure"] = storms["atcf_id"].isin(chd_rep)
    storms["has_gdacs_exposure"] = storms["atcf_id"].isin(gdacs_rep)
    storms["has_adam_exposure"] = storms["atcf_id"].isin(adam_rep)

    # Per-storm source diagnostic (source_diagnostics.py): the GDACS/ADAM
    # event-list sweep, telling us — when a source has NO exposure — WHY (zero,
    # final-footprint unservable, WFP-403, no record). A non-null raw status
    # means the source has the storm ON RECORD (its event exists). This
    # SUPERSEDES the old gdacs_linked/adam_linked proxy, which only saw events
    # we managed to link in storm_id_lookup and so read "no record" for events
    # that exist but we never matched (e.g. Irma: GDACS has it; final
    # footprint is just unservable).
    diag = sd.load_status().rename(columns={
        "gdacs_status": "gdacs_status_raw", "adam_status": "adam_status_raw"})
    storms = storms.merge(diag, on="atcf_id", how="left")

    def _label(has_exp, raw, table):
        if has_exp:
            return ""                       # we hold final exposure → no caveat
        if pd.isna(raw):
            return "no record"              # source has no event for this storm
        labels = (sd.GDACS_STATUS_LABEL if table == "GDACS"
                  else sd.ADAM_STATUS_LABEL)
        return labels.get(raw, raw)
    storms["gdacs_status"] = [_label(h, r, "GDACS") for h, r in zip(
        storms["has_gdacs_exposure"], storms["gdacs_status_raw"])]
    storms["adam_status"] = [_label(h, r, "ADAM") for h, r in zip(
        storms["has_adam_exposure"], storms["adam_status_raw"])]

    # One readable note combining both per-source statuses (the visible column;
    # gdacs_status / adam_status are kept but hidden for filtering). A source
    # with exposure has a blank status and contributes nothing — its
    # has_*_exposure flag already says so; only sources with something to
    # explain appear.
    def _note(gd, ad):
        if gd and ad and gd == ad:          # same status both → state it once
            return f"GDACS & ADAM - {gd}"
        parts = []
        if gd:
            parts.append(f"GDACS - {gd}")
        if ad:
            parts.append(f"ADAM - {ad}")
        return " | ".join(parts)
    storms["note_gdacs_adam"] = [_note(g, a) for g, a in zip(
        storms["gdacs_status"], storms["adam_status"])]

    # sources_reporting_exposure: which sources produced exposure (or "none").
    storms["sources_reporting_exposure"] = [
        _sources_label(c, g, a)
        for c, g, a in zip(storms["has_chd_exposure"],
                           storms["has_gdacs_exposure"],
                           storms["has_adam_exposure"])]
    # sources_with_record: which sources have this storm ON FILE. CHD always
    # (these rows ARE the NHC catalog); GDACS/ADAM now from the diagnostic — the
    # source has a record if it produced exposure OR its event exists in the
    # source event-list sweep (closing the old matching-bounded gap).
    gdacs_record = storms["has_gdacs_exposure"] | storms["gdacs_status_raw"].notna()
    adam_record = storms["has_adam_exposure"] | storms["adam_status_raw"].notna()
    storms["sources_with_record"] = [
        _sources_label(True, g, a) for g, a in zip(gdacs_record, adam_record)]
    return storms.rename(columns={"atcf_id": "storm_id"})[[
        "storm_id", "storm_name", "season", "basin",
        "gdacs_eventid", "adam_eventid",
        "sources_with_record", "sources_reporting_exposure",
        "has_chd_exposure", "has_gdacs_exposure", "has_adam_exposure",
        "note_gdacs_adam", "gdacs_status", "adam_status"]]


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
          f"a note (note_gdacs_adam) explaining every GDACS/ADAM gap."),
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
        B("bullet", "CHD — our NHC-derived estimate; = final observed exposure "
          "+ final forecast-only exposure (the alert pipeline's number)."),
        B("bullet", "GDACS — JRC global disaster footprint."),
        B("bullet", "ADAM — WFP; ingests GDACS upstream, so ADAM ≈ GDACS by "
          "construction (adam_eventid = gdacs_eventid)."),
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
        B("bullet", "CHD = observed + forecast-only is a sum; it can "
          "double-count a unit in both footprints (rare for completed "
          "storms)."),
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
            # Visible: storm_id, storm_name, season, basin, sources_with_record,
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
