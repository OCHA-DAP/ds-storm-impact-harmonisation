"""Vendored, forked SQL for the three-source exposure comparison.

Why this file is *vendored* rather than imported from ds-storms-alerts:
the alerts repo's `src/data.py` fetchers (Tristan's adm1 work) are
*advisory-relative* — they snap each source to a two-timestamp window
around a live NHC advisory (`valid_time IN (:t_exact, :t_prev)`),
because the email pipeline runs per advisory. A *retrospective*
distributional comparison instead wants each storm's **final** estimate
(its "last ballot"), which is each unit's own latest `valid_time` with
no advisory window. That change — plus keeping zero rows and surfacing
ADAM orphans — means forking the SQL, and once we're forking, this
analysis lives in its proper home (this repo) with its own copy.

Lineage:
  - FM-aggregation / orphan-UNION / lookup joins  → ported from the
    ds-storms-alerts preview app (`notebooks/alert_preview.py`,
    `_matching_demo`) and Tristan's `src/data.py` adm1 fetchers.
  - storm-final snap (DISTINCT ON ... ORDER BY valid_time DESC, no time
    window)                                        → the preview app's
    NHC snap, generalized to all three sources.

Join-unit contract (the key every fetcher emits as `unit`):
  - admin_level 0 → `unit = iso3`. The unit *is* the country, which
    every source carries cleanly; routing adm0 through the name/code
    lookups only manufactures spurious orphans (CHD `pcode == iso3` in
    100% of adm0 rows; ADAM has 11 placeholder rows with NULL iso3 that
    are genuinely un-attributable). So adm0 skips the lookups.
  - admin_level 1 → `unit = fm_pcode`. Here the lookups are essential;
    GDACS/ADAM admins that don't match surface as `unit = NULL` orphan
    rows (~12-15% of units but only ~3% of population — Phase-0), to be
    bucketed and reported, never silently dropped or zero-filled.

Canonical storm key is `atcf_id` (each query joins
`storms.storm_id_lookup` to resolve it).

CAVEAT from Phase-0 recon: in `storm_id_lookup`, every populated
`adam_eventid` equals its `gdacs_eventid` (ADAM ingests GDACS upstream;
the ADAM-side id enrichment hasn't run). So the ADAM↔storm link is
*inherited from GDACS*, not independently confirmed.
"""

from __future__ import annotations

import ocha_stratus as stratus
import pandas as pd
from sqlalchemy import bindparam, text

from src.source_exposure import fm_matching as fm

# Thresholds common to all three sources are {34, 64} (GDACS has no 50kt).
COMMON_KT = (34, 64)


def get_engine(stage: str = "dev"):
    return stratus.get_engine(stage=stage)


def _expand(sql: str):
    return text(sql).bindparams(bindparam("atcf_ids", expanding=True))


def _expand2(sql: str):
    # two IN-lists (obsv + fcast) bound to the same value list
    return text(sql).bindparams(
        bindparam("ids_o", expanding=True),
        bindparam("ids_f", expanding=True),
    )


# ─────────────────────────────────────────────────────────────────────
# Storm population: all-three IDs AND real exposure rows in all three.
# ─────────────────────────────────────────────────────────────────────

def qualifying_storms(engine) -> pd.DataFrame:
    """Storms usable for a three-way comparison (Phase-0: 37 of them).

    Requires a non-null id for each source *and* at least one exposure
    row in each source's table. One row per storm: the three ids, `sid`,
    and `season` (atcf year).
    """
    sql = text("""
        SELECT l.gdacs_eventid, l.atcf_id, l.adam_eventid, l.sid,
               right(l.atcf_id, 4) AS season
        FROM storms.storm_id_lookup l
        WHERE l.gdacs_eventid IS NOT NULL AND l.atcf_id IS NOT NULL
          AND l.adam_eventid IS NOT NULL
          AND EXISTS (SELECT 1 FROM storms.gdacs_exposure g
                      WHERE g.gdacs_eventid = l.gdacs_eventid)
          AND EXISTS (SELECT 1 FROM storms.nhc_tracks_obsv_exposure n
                      WHERE n.atcf_id = l.atcf_id)
          AND EXISTS (SELECT 1 FROM storms.adam_exposure a
                      WHERE a.adam_eventid = l.adam_eventid)
        ORDER BY season, l.atcf_id
    """)
    return pd.read_sql(sql, engine)


# ─────────────────────────────────────────────────────────────────────
# CHD (our NHC-derived) — final estimate = final OBSERVED + final
# FORECAST-ONLY, summed per (storm, unit, wsp). This mirrors the alert
# pipeline, which reports `obsv + fcastonly` as the storm's exposure
# (run_alert.py: "Forecast total (fcast + obsv) — can exceed either").
#   - observed-final  = latest valid_time per unit (the realized track).
#   - fcastonly-final = latest issued_time per unit (the forecast ahead
#     that hadn't been realized at the last advisory).
# `pcode` IS the FM pcode (== iso3 at adm0) in both tables.
# CAVEAT: summing can double-count a unit sitting in BOTH footprints —
# rare for completed storms, real for still-active ones.
# ─────────────────────────────────────────────────────────────────────

def fetch_chd(engine, atcf_ids: list[str], admin_level: int) -> pd.DataFrame:
    """Columns: atcf_id, iso3, unit, wind_speed_kt, chd_pop
    (chd_pop = observed-final + forecast-only-final)."""
    cols = ["atcf_id", "iso3", "unit", "wind_speed_kt", "chd_pop"]
    if not atcf_ids:
        return pd.DataFrame(columns=cols)
    sql = _expand2("""
        WITH obsv AS (
            SELECT DISTINCT ON (atcf_id, pcode, wind_speed_kt)
                atcf_id, iso3, pcode, wind_speed_kt, pop_exposed
            FROM storms.nhc_tracks_obsv_exposure
            WHERE atcf_id IN :ids_o AND admin_level = :lvl
            ORDER BY atcf_id, pcode, wind_speed_kt, valid_time DESC
        ),
        fcast AS (
            SELECT DISTINCT ON (atcf_id, pcode, wind_speed_kt)
                atcf_id, iso3, pcode, wind_speed_kt, pop_exposed
            FROM storms.nhc_tracks_fcastonly_exposure
            WHERE atcf_id IN :ids_f AND admin_level = :lvl
            ORDER BY atcf_id, pcode, wind_speed_kt, issued_time DESC
        )
        SELECT COALESCE(o.atcf_id, f.atcf_id) AS atcf_id,
               COALESCE(o.iso3, f.iso3)       AS iso3,
               COALESCE(o.pcode, f.pcode)     AS unit,
               COALESCE(o.wind_speed_kt, f.wind_speed_kt) AS wind_speed_kt,
               COALESCE(o.pop_exposed, 0) + COALESCE(f.pop_exposed, 0) AS chd_pop
        FROM obsv o
        FULL OUTER JOIN fcast f
          ON o.atcf_id = f.atcf_id AND o.pcode = f.pcode
         AND o.wind_speed_kt = f.wind_speed_kt
    """)
    return pd.read_sql(sql, engine,
                       params={"ids_o": atcf_ids, "ids_f": atcf_ids,
                               "lvl": admin_level})


# ─────────────────────────────────────────────────────────────────────
# GDACS — adm0: storm-final by iso3 (no lookup). adm1: storm-final per
# gdacs_admin_code, FM-aggregated, with orphan (unit=NULL) rows. Countries
# GDACS only covers at country level (lookup gmi_admin IS NULL) excluded
# from adm1.
# ─────────────────────────────────────────────────────────────────────

def _raw_gdacs_adm1(engine, atcf_ids: list[str]) -> pd.DataFrame:
    """Pick-by-time only: the storm-final (latest valid_time) GDACS adm1 row per
    (atcf_id, gdacs_admin_code, wind_speed_kt). FM matching is done separately by
    fm_matching.match_gdacs. Columns: atcf_id, iso3, gdacs_admin_code,
    admin_name, wind_speed_kt, pop_exposed."""
    sql = _expand("""
        SELECT DISTINCT ON (l.atcf_id, g.gdacs_admin_code, g.wind_speed_kt)
            l.atcf_id, g.iso3, g.gdacs_admin_code, g.admin_name,
            g.wind_speed_kt, g.pop_exposed
        FROM storms.gdacs_exposure g
        JOIN storms.storm_id_lookup l ON l.gdacs_eventid = g.gdacs_eventid
        WHERE l.atcf_id IN :atcf_ids AND g.admin_level = 1
        ORDER BY l.atcf_id, g.gdacs_admin_code, g.wind_speed_kt, g.valid_time DESC
    """)
    return pd.read_sql(sql, engine, params={"atcf_ids": atcf_ids})


def fetch_gdacs(engine, atcf_ids: list[str], admin_level: int) -> pd.DataFrame:
    """Columns: atcf_id, iso3, unit, wind_speed_kt, gdacs_pop,
    n_gdacs_admins, gdacs_admin1_name, gdacs_caveat.

    adm0: storm-final by iso3 (no FM lookup). adm1: pick-by-time here, then FM
    matching via the shared fm_matching module (orphans surfaced as unit=NULL)."""
    cols = ["atcf_id", "iso3", "unit", "wind_speed_kt", "gdacs_pop",
            "n_gdacs_admins", "gdacs_admin1_name", "gdacs_caveat"]
    if not atcf_ids:
        return pd.DataFrame(columns=cols)
    if admin_level == 0:
        sql = _expand("""
            SELECT DISTINCT ON (l.atcf_id, g.iso3, g.wind_speed_kt)
                l.atcf_id, g.iso3, g.iso3 AS unit, g.wind_speed_kt,
                g.pop_exposed AS gdacs_pop, 1 AS n_gdacs_admins,
                NULL::text AS gdacs_admin1_name, NULL::text AS gdacs_caveat
            FROM storms.gdacs_exposure g
            JOIN storms.storm_id_lookup l ON l.gdacs_eventid = g.gdacs_eventid
            WHERE l.atcf_id IN :atcf_ids AND g.admin_level = 0
              AND g.iso3 IS NOT NULL
            ORDER BY l.atcf_id, g.iso3, g.wind_speed_kt, g.valid_time DESC
        """)
        return pd.read_sql(sql, engine, params={"atcf_ids": atcf_ids})
    matched = fm.match_gdacs(_raw_gdacs_adm1(engine, atcf_ids),
                             fm.load_gdacs_lookup(engine))
    return matched.rename(columns={
        "fm_pcode": "unit", "pop_exposed": "gdacs_pop",
        "n_src_admins": "n_gdacs_admins", "src_admins": "gdacs_admin1_name",
        "caveat_note": "gdacs_caveat"})[cols]


# ─────────────────────────────────────────────────────────────────────
# ADAM — adm0: storm-final by iso3 (no lookup; NULL-iso3 placeholder rows
# excluded). adm1: storm-final per admin_name, FM-aggregated by name
# match, with orphan (unit=NULL) rows. DEVIATION from alerts prod, which
# drops name-match failures — we surface them so adm1 coverage is
# quantified symmetrically with GDACS.
# ─────────────────────────────────────────────────────────────────────

def _raw_adam_adm1(engine, atcf_ids: list[str]) -> pd.DataFrame:
    """Pick-by-time only: the storm-final (latest valid_time) ADAM adm1 row per
    (atcf_id, iso3, admin_name, wind_speed_kt). FM matching is done separately by
    fm_matching.match_adam. Columns: atcf_id, iso3, admin_name, wind_speed_kt,
    pop_exposed."""
    sql = _expand("""
        SELECT DISTINCT ON (l.atcf_id, a.iso3, lower(a.admin_name), a.wind_speed_kt)
            l.atcf_id, a.iso3, a.admin_name, a.wind_speed_kt, a.pop_exposed
        FROM storms.adam_exposure a
        JOIN storms.storm_id_lookup l ON l.adam_eventid = a.adam_eventid
        WHERE l.atcf_id IN :atcf_ids AND a.admin_level = 1
        ORDER BY l.atcf_id, a.iso3, lower(a.admin_name), a.wind_speed_kt,
                 a.valid_time DESC
    """)
    return pd.read_sql(sql, engine, params={"atcf_ids": atcf_ids})


def fetch_adam(engine, atcf_ids: list[str], admin_level: int) -> pd.DataFrame:
    """Columns: atcf_id, iso3, unit, wind_speed_kt, adam_pop,
    n_adam_admins, adam_admin1_name, adam_caveat.

    adm0: storm-final by iso3 (no FM lookup; NULL-iso3 placeholder rows
    excluded). adm1: pick-by-time here, then FM matching (by case-insensitive
    name) via the shared fm_matching module. DEVIATION from alerts prod, which
    drops name-match failures — we surface them (unit=NULL) so adm1 coverage is
    quantified symmetrically with GDACS."""
    cols = ["atcf_id", "iso3", "unit", "wind_speed_kt", "adam_pop",
            "n_adam_admins", "adam_admin1_name", "adam_caveat"]
    if not atcf_ids:
        return pd.DataFrame(columns=cols)
    if admin_level == 0:
        sql = _expand("""
            SELECT DISTINCT ON (l.atcf_id, a.iso3, a.wind_speed_kt)
                l.atcf_id, a.iso3, a.iso3 AS unit, a.wind_speed_kt,
                a.pop_exposed AS adam_pop, 1 AS n_adam_admins,
                NULL::text AS adam_admin1_name, NULL::text AS adam_caveat
            FROM storms.adam_exposure a
            JOIN storms.storm_id_lookup l ON l.adam_eventid = a.adam_eventid
            WHERE l.atcf_id IN :atcf_ids AND a.admin_level = 0
              AND a.iso3 IS NOT NULL
            ORDER BY l.atcf_id, a.iso3, a.wind_speed_kt, a.valid_time DESC
        """)
        return pd.read_sql(sql, engine, params={"atcf_ids": atcf_ids})
    matched = fm.match_adam(_raw_adam_adm1(engine, atcf_ids),
                            fm.load_adam_lookup(engine))
    return matched.rename(columns={
        "fm_pcode": "unit", "pop_exposed": "adam_pop",
        "n_src_admins": "n_adam_admins", "src_admins": "adam_admin1_name",
        "caveat_note": "adam_caveat"})[cols]


def lookup_caveats(engine):
    """Country-level adm1 policy (adm0 rows carrying a caveat) PLUS the
    per-unit adm1 rows that carry a reviewer `note` (the genuinely unique
    annotated cases — Artemisa, Freeport, …). Straight from the lookups'
    own caveat system. Columns: source, iso3, admin_level, scope,
    caveat_kind, caveat_note, note."""
    frames = []
    for src, t in (("GDACS", "gdacs_fm_lookup"), ("ADAM", "adam_fm_lookup")):
        df = pd.read_sql(text(f"""
            SELECT DISTINCT iso3, admin_level,
                   CASE WHEN admin_level = 0 THEN '(country policy)'
                        ELSE fm_name END AS scope,
                   caveat_kind, caveat_note, note
            FROM storms.{t}
            WHERE (admin_level = 0
                   AND (caveat_kind IS NOT NULL OR caveat_note IS NOT NULL))
               OR (admin_level = 1 AND note IS NOT NULL)
        """), engine)
        df.insert(0, "source", src)
        frames.append(df)
    return pd.concat(frames, ignore_index=True)


def adm1_unit_caveats(engine):
    """Per FM adm1 unit, its STRUCTURAL GDACS/ADAM comparability caveat_kind
    (independent of any storm; surfaces the full taxonomy). Thin wrapper over
    fm_matching.unit_caveats, renamed to the workbook's `unit` column.
    Columns: unit (=fm_pcode), gdacs_caveat, adam_caveat."""
    return fm.unit_caveats(engine).rename(columns={"fm_pcode": "unit"})


def lookup_adm1_coverage(engine):
    """(gdacs_iso3_set, adam_iso3_set): iso3s that each source's FM lookup
    actually covers at admin_level=1. An orphan in a country NOT in the set
    means the whole country is missing from the adm1 lookup (a coverage
    gap); an orphan in a covered country is a name-level mismatch."""
    gd = pd.read_sql(text(
        "SELECT DISTINCT iso3 FROM storms.gdacs_fm_lookup WHERE admin_level=1"),
        engine)
    ad = pd.read_sql(text(
        "SELECT DISTINCT iso3 FROM storms.adam_fm_lookup WHERE admin_level=1"),
        engine)
    return set(gd["iso3"]), set(ad["iso3"])


def storms_with_source_exposure(engine, atcf_ids: list[str]):
    """(chd_set, gdacs_set, adam_set): atcf_ids for which each source actually
    COMPUTED exposure — i.e. has at least one POSITIVE pop_exposed value. This
    is the criterion for the zero-vs-blank fill: a source that computed a storm
    gets 0 for the units it didn't list (not in its footprint); a source that
    never computed the storm is left blank.

    CAVEAT — why `pop_exposed > 0`, not merely "has a row": GDACS lists the
    countries its wind footprint geometrically intersected, but for storms in
    its ~2016–2022 pre-compute era it returns POP_AFFECTED = -1 ("not computed")
    for ALL of them, which lands in the DB as NULL pop_exposed. Counting those
    as "reported" then zero-fills 116 storms' worth of MISSING values into fake
    zeros (see source_diagnostics `values_missing`). A row with NULL pop_exposed
    is a data gap, NOT a true 0, so it must not make a storm "reported". A storm
    GDACS genuinely found no exposure for instead produces NO impact rows
    (`NO_ROWS`/empty getimpact) and is handled via the diagnostic, not here."""
    if not atcf_ids:
        return set(), set(), set()
    p = {"atcf_ids": atcf_ids}
    obsv = pd.read_sql(_expand(
        "SELECT DISTINCT atcf_id FROM storms.nhc_tracks_obsv_exposure "
        "WHERE atcf_id IN :atcf_ids"), engine, params=p)
    fcst = pd.read_sql(_expand(
        "SELECT DISTINCT atcf_id FROM storms.nhc_tracks_fcastonly_exposure "
        "WHERE atcf_id IN :atcf_ids"), engine, params=p)
    gd = pd.read_sql(_expand(
        "SELECT DISTINCT l.atcf_id FROM storms.gdacs_exposure g "
        "JOIN storms.storm_id_lookup l ON l.gdacs_eventid = g.gdacs_eventid "
        "WHERE l.atcf_id IN :atcf_ids AND g.pop_exposed > 0"), engine, params=p)
    ad = pd.read_sql(_expand(
        "SELECT DISTINCT l.atcf_id FROM storms.adam_exposure a "
        "JOIN storms.storm_id_lookup l ON l.adam_eventid = a.adam_eventid "
        "WHERE l.atcf_id IN :atcf_ids AND a.pop_exposed > 0"), engine, params=p)
    return (set(obsv["atcf_id"]) | set(fcst["atcf_id"]),
            set(gd["atcf_id"]), set(ad["atcf_id"]))


TIMELINE_TOTALS_BLOB = ("ds-storm-impact-harmonisation/processed/"
                        "gdacs_timeline_totals.parquet")


def gdacs_computed_thresholds(engine, atcf_ids: list[str]) -> set:
    """{(atcf_id, wind_speed_kt)} where GDACS produced a POSITIVE per-country
    exposure (it COMPUTED that threshold's footprint). For these, a country
    ABSENT from the footprint at that threshold is a genuine 0 (your case 2)."""
    if not atcf_ids:
        return set()
    df = pd.read_sql(_expand(
        "SELECT DISTINCT l.atcf_id, g.wind_speed_kt FROM storms.gdacs_exposure g "
        "JOIN storms.storm_id_lookup l ON l.gdacs_eventid = g.gdacs_eventid "
        "WHERE l.atcf_id IN :atcf_ids AND g.admin_level = 0 AND g.pop_exposed > 0"),
        engine, params={"atcf_ids": atcf_ids})
    return set(zip(df["atcf_id"], df["wind_speed_kt"]))


def gdacs_null_cells(engine, atcf_ids: list[str]) -> set:
    """{(atcf_id, iso3, wind_speed_kt)} where GDACS has a row but pop_exposed IS
    NULL (POP_AFFECTED = -1) — a per-cell DATA GAP (case 4 at the cell level).
    These must stay NaN, never be zero-filled, even inside a computed storm."""
    if not atcf_ids:
        return set()
    df = pd.read_sql(_expand(
        "SELECT DISTINCT l.atcf_id, g.iso3, g.wind_speed_kt "
        "FROM storms.gdacs_exposure g "
        "JOIN storms.storm_id_lookup l ON l.gdacs_eventid = g.gdacs_eventid "
        "WHERE l.atcf_id IN :atcf_ids AND g.admin_level = 0 "
        "AND g.pop_exposed IS NULL"),
        engine, params={"atcf_ids": atcf_ids})
    return set(zip(df["atcf_id"], df["iso3"], df["wind_speed_kt"]))


def load_timeline_totals():
    """atcf_id-indexed GDACS storm-wide timeline totals (max pop39 / pop74).
    The total is the only signal that separates a GENUINE zero (total = 0 →
    fill GDACS 0) from MISSING per-country data (total > 0 → keep NaN). Built by
    scripts/cache_gdacs_timeline_totals.py; reads the local mirror then blob.
    Returns an EMPTY frame (no rows) if neither exists — callers then treat every
    non-computed storm as 'unknown' (NaN), the safe default."""
    from pathlib import Path
    local = Path(__file__).resolve().parents[2] / "artefacts" / \
        "gdacs_timeline_totals.parquet"
    try:
        df = pd.read_parquet(local) if local.exists() else \
            stratus.load_parquet_from_blob(TIMELINE_TOTALS_BLOB)
    except Exception:
        return pd.DataFrame(
            columns=["timeline_pop39_max", "timeline_pop74_max"]).set_index(
            pd.Index([], name="atcf_id"))
    return df.set_index("atcf_id")


def storm_names(engine) -> pd.DataFrame:
    """atcf_id → human-readable storm name + tidy slug. Columns:
    atcf_id, storm_name, storm_slug."""
    sql = text("""
        SELECT atcf_id, name AS storm_name, storm_id AS storm_slug
        FROM storms.nhc_storms
    """)
    return pd.read_sql(sql, engine)


def all_nhc_storms(engine, min_season: int = 2001) -> pd.DataFrame:
    """Master storm list: every NHC storm from `min_season` on (the
    operational/exposure era; exposure data begins ~2001), even those with
    no exposure. Left-joins the crosswalk for GDACS/ADAM ids.

    Columns: atcf_id, storm_name, season, basin, gdacs_eventid, adam_eventid.

    `storm_name` resolution: `ibtracs_storms.name` → `nhc_storms.name` →
    `atcf_id`. IBTrACS is preferred because it is the post-season archive of
    record — `nhc_storms.name` is unreliable: it can be the literal string
    'NaN' (2025, dev) / NULL (2025, prod), OR a stale number-placeholder
    ('NINE' for AL092023, which IBTrACS correctly names HAROLD). NHC is the
    fallback for storms IBTrACS doesn't name (e.g. genuine unnamed
    depressions like AL092003 → 'NINE'); atcf_id is the last resort. The
    real fix still belongs in ds-storms-pipeline's NHC name step.
    `basin` expands the genesis-basin code to a readable label.
    """
    sql = text("""
        SELECT s.atcf_id,
               COALESCE(ib.name, NULLIF(NULLIF(s.name, 'NaN'), ''), s.atcf_id)
                   AS storm_name,
               s.season,
               CASE s.genesis_basin
                   WHEN 'NA' THEN 'North Atlantic'
                   WHEN 'EP' THEN 'Eastern Pacific'
                   WHEN 'CP' THEN 'Central Pacific'
                   ELSE s.genesis_basin END AS basin,
               l.gdacs_eventid, l.adam_eventid
        FROM storms.nhc_storms s
        LEFT JOIN storms.storm_id_lookup l ON l.atcf_id = s.atcf_id
        LEFT JOIN (
            -- dedup: ibtracs_storms has provisional + best-track rows per
            -- atcf_id (62 dups); collapse to one non-empty name each
            SELECT atcf_id, MAX(name) AS name
            FROM storms.ibtracs_storms
            WHERE atcf_id IS NOT NULL AND name IS NOT NULL AND name <> ''
            GROUP BY atcf_id
        ) ib ON ib.atcf_id = s.atcf_id
        WHERE s.season >= :min_season
        ORDER BY s.season, s.atcf_id
    """)
    return pd.read_sql(sql, engine, params={"min_season": min_season})


def fm_names(engine, admin_level: int) -> pd.DataFrame:
    """unit → name labels, unioned across both lookups (at adm0 the unit is the
    iso3 and fm_name the country name). Thin wrapper over fm_matching.fm_names,
    renamed to the workbook's `unit` column. Columns: unit, fm_name."""
    return fm.fm_names(engine, admin_level).rename(columns={"fm_pcode": "unit"})
