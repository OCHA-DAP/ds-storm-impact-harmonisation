"""GDACS + ADAM source-coverage diagnostics.

Purpose
-------
For every storm GDACS / ADAM *could* have reported, say — in one table —
whether we hold its exposure, and if not, *why not*. This turns the
workbook's "GDACS/ADAM blank" cells from "reason unknown" into a named,
defensible status. Read-only: it never writes to the database.

The cheap insight that keeps this affordable
--------------------------------------------
Most gaps are explainable from the DB alone, no API call needed. In the
GDACS pipeline the call order is::

    get_exposure(...)        # on RequestException -> `continue`
    ...                      #   (skips the match below)
    attempt_match(...)       # only runs if the fetch did NOT raise

So a GDACS event that is **linked in storm_id_lookup but has no exposure
rows** must have had a *successful, empty* fetch (the match only runs when
the fetch didn't raise) — i.e. genuinely zero exposure. Only **unlinked**
gap events are ambiguous (error-skipped vs zero-and-unmatched), so only
those are probed. That collapses ~201 GDACS gaps to ~47 probes.

ADAM is different: its event→storm link is inherited from GDACS (the
ADAM-side id enrichment never ran — see queries.py), and ADAM ingests
exposure independently from a per-event ``population_csv_url``. So the
"linked ⟹ succeeded" shortcut does NOT hold for ADAM; a gap there means
the CSV download failed (commonly WFP 403 on historical files) or served
empty. We sample first to detect a uniform 403 pattern before fetching all.

Status taxonomy (column ``status``)
-----------------------------------
have_exposure      in the DB with rows (final footprint held)        [no probe]
reported_zero      GDACS computed exposure, it was empty             [no probe]
recoverable_latest latest episode serves now (transient miss; a
                   normal re-run would ingest it; final available)   [probe]
partial_no_final   earlier episodes serve but the FINAL/cumulative
                   episode 500s/times-out — match is still possible
                   but the final exposure value is NOT available     [probe]
unservable         every probed episode 500s/times-out               [probe]
no_data            no timeline / no episodes                         [probe]
csv_403            ADAM: WFP denies the population CSV (403)          [probe]
served_zero        ADAM: CSV served but empty / zero population       [probe]
csv_error          ADAM: CSV download failed (network/other)         [probe]
"""

from __future__ import annotations

import re
from pathlib import Path

import ocha_stratus as stratus
import pandas as pd
from sqlalchemy import text

# Probe-only dependencies. load_status() (which the workbook calls) needs only
# pandas — it just reads the generated CSV — so these are guarded: the workbook
# runs in an env with openpyxl but NOT ocha_lens, and must still import this
# module. The CSV is the clean hand-off between the two environments.
try:
    import requests
    from ocha_lens.datasources import adam, gdacs
except ImportError:
    requests = gdacs = adam = None

OUT = Path(__file__).parent / "out" / "source_diagnostics.csv"

# Canonical home for the generated diagnostic: Azure blob (dev). The workbook
# reproduces from here when no local copy exists, so it never has to re-run the
# expensive probe just to rebuild.
BLOB_STAGE = "dev"
BLOB_CONTAINER = "projects"
BLOB_NAME = ("ds-storm-impact-harmonisation/processed/"
             "adam_gdacs_per_storm_source_diagnostics.csv")

# Human-readable per-storm status label (for the workbook storms tab). An empty
# string means "exposure held — nothing to caveat". `have_exposure` never
# reaches the label map (the workbook blanks it from the DB flag), but is listed
# for completeness.
GDACS_STATUS_LABEL = {
    "have_exposure": "",
    "reported_zero": "reported zero exposure",
    "values_missing": "exposure not computed by GDACS (POP_AFFECTED=-1)",
    "recoverable_latest": "not yet ingested (GDACS serves it now)",
    "partial_no_final": "final footprint unavailable (GDACS server error, 500)",
    "unservable": "exposure unavailable (GDACS server error, 500)",
    "no_data": "no GDACS episode data",
}
ADAM_STATUS_LABEL = {
    "have_exposure": "",
    "values_missing": "exposure not computed (pop_exposed all NULL)",
    "recoverable_latest": "not yet ingested (CSV available)",
    "served_zero": "reported zero exposure",
    "csv_403": "exposure access denied (403)",
    "csv_error": "download failed",
    "no_data": "no exposure file",
}

# When a storm bridges to more than one event of a source, keep the most
# exposure-bearing status (earlier = better).
_STATUS_PRIORITY = ["have_exposure", "reported_zero", "recoverable_latest",
                    "partial_no_final", "served_zero", "values_missing",
                    "unservable", "csv_403", "csv_error", "no_data"]


def load_status(path: Path = OUT) -> pd.DataFrame:
    """Read the diagnostic → one row per atcf_id with the best GDACS and ADAM
    raw status (NaN where the source has no event for that storm). Columns:
    atcf_id, gdacs_status, adam_status. A non-NaN status means that source has
    the storm ON RECORD (its event exists).

    Reads the local CSV if present, else pulls the canonical copy from blob — so
    the workbook reproduces from blob without re-running the probe."""
    if path is not None and Path(path).exists():
        df = pd.read_csv(path)
        print(f"  diagnostic: local {path}")
    else:
        df = stratus.load_csv_from_blob(
            BLOB_NAME, stage=BLOB_STAGE, container_name=BLOB_CONTAINER)
        print(f"  diagnostic: blob {BLOB_STAGE}:{BLOB_CONTAINER}/{BLOB_NAME}")
    df = df[df["atcf_id"].notna()].copy()
    rank = {s: i for i, s in enumerate(_STATUS_PRIORITY)}
    df["_rank"] = df["status"].map(lambda s: rank.get(s, len(rank)))
    best = (df.sort_values("_rank")
              .drop_duplicates(["atcf_id", "source"])
              .pivot(index="atcf_id", columns="source", values="status"))
    best = best.rename(columns={"GDACS": "gdacs_status",
                                "ADAM": "adam_status"}).reset_index()
    for col in ("gdacs_status", "adam_status"):
        if col not in best.columns:
            best[col] = pd.NA
    return best[["atcf_id", "gdacs_status", "adam_status"]]

# Probe timeout (s) — shorter than the libraries' default 30 so the sweep
# over slow/erroring GDACS endpoints stays bounded.
PROBE_TIMEOUT = 12
# A TIMEOUT (unlike a hard HTTP 500) may just mean the episode is slow, not
# unservable. Events that come back 'unservable' due to a timeout are re-probed
# once at this longer budget before the verdict sticks. 500-class events are
# final and never escalated. This keeps the CSV reproducible from the script
# (the escalation set is derived from results, not a hand-picked event list).
ESCALATE_TIMEOUT = 45
ADAM_SAMPLE = 12   # sample size to detect a uniform ADAM-403 pattern


def _is_timeout(detail: str) -> bool:
    """A probe detail describing a slow/dropped connection (escalatable),
    as opposed to a hard HTTP 500 (final)."""
    return any(k in detail for k in ("Timeout", "Connection", "Chunked"))


def _clean_name(s: str) -> str:
    """GDACS/ADAM names suffix the 2-digit year ('MARTY-15'); strip it so
    they compare to NHC names."""
    if s is None:
        return ""
    return re.sub(r"-\d+$", "", str(s)).strip().upper()


# ──────────────────────────────────────────────────────────────────────
# DB facts (no API)
# ──────────────────────────────────────────────────────────────────────

def _db_facts(engine):
    """Returns a dict of DB-derived facts used to classify without probing."""
    with engine.connect() as c:
        # has_pos distinguishes a real footprint (>=1 positive pop_exposed)
        # from rows that exist but are ALL NULL — GDACS listed the countries
        # its footprint intersected but returned POP_AFFECTED = -1 ("not
        # computed", its ~2016-2022 pre-compute era). Those are a data gap
        # (`values_missing`), NOT exposure and NOT a true zero.
        gd = pd.read_sql(text(
            "SELECT gdacs_eventid AS eid, COUNT(*) AS rows, "
            "COALESCE(BOOL_OR(pop_exposed > 0), FALSE) AS has_pos, "
            "SUM(CASE WHEN admin_level=0 THEN pop_exposed ELSE 0 END) AS pop0 "
            "FROM storms.gdacs_exposure GROUP BY gdacs_eventid"), c)
        ad = pd.read_sql(text(
            "SELECT adam_eventid AS eid, COUNT(*) AS rows, "
            "COALESCE(BOOL_OR(pop_exposed > 0), FALSE) AS has_pos, "
            "SUM(CASE WHEN admin_level=0 THEN pop_exposed ELSE 0 END) AS pop0 "
            "FROM storms.adam_exposure GROUP BY adam_eventid"), c)
        lk = pd.read_sql(text(
            "SELECT gdacs_eventid, atcf_id, adam_eventid "
            "FROM storms.storm_id_lookup"), c)
    return {
        "gd_have": gd.set_index("eid"),
        "ad_have": ad.set_index("eid"),
        "gd_linked": {int(x): a for x, a in
                      zip(lk.gdacs_eventid.dropna(),
                          lk.loc[lk.gdacs_eventid.notna(), "atcf_id"])},
        "ad_linked": {int(x): a for x, a in
                      zip(lk.adam_eventid.dropna(),
                          lk.loc[lk.adam_eventid.notna(), "atcf_id"])},
    }


def _nhc_by_name_year(engine) -> dict:
    """(clean_name, season) -> atcf_id, to bridge unlinked source events
    back to the NHC storm they belong to."""
    from src.source_exposure.queries import all_nhc_storms
    s = all_nhc_storms(engine)
    return {(_clean_name(n), int(y)): a
            for n, y, a in zip(s.storm_name, s.season, s.atcf_id)}


# ──────────────────────────────────────────────────────────────────────
# GDACS exposure-dict helpers
# ──────────────────────────────────────────────────────────────────────

def _exp_nonempty(exp) -> bool:
    """get_exposure_adm0 returns {buffer: DataFrame}; True if any buffer
    carries rows."""
    if not isinstance(exp, dict):
        return bool(exp is not None and len(exp))
    return any(df is not None and len(df) for df in exp.values())


def _probe_gdacs(eid: int):
    """Bounded probe of one unlinked GDACS gap event. Returns
    (status, final_available, detail). Probes the latest episode, then one
    earlier (mid) episode if the latest fails."""
    try:
        detail = gdacs.get_event_detail(eid)
    except requests.exceptions.RequestException as e:
        return "no_data", False, f"event detail failed: {type(e).__name__}"

    # latest episode
    try:
        exp = gdacs.get_exposure_adm0(eid, detail=detail)
        if _exp_nonempty(exp):
            return ("recoverable_latest", True,
                    "latest episode serves with rows (transient miss)")
        return ("reported_zero", True,
                "latest episode serves but empty (genuinely zero)")
    except requests.exceptions.RequestException as e:
        latest_err = type(e).__name__

    # latest failed — does an earlier episode serve?
    try:
        tl = gdacs.get_timeline(eid, detail=detail)
    except (gdacs.NoTimelineError, requests.exceptions.RequestException):
        return ("unservable", False,
                f"latest {latest_err}; no timeline to fall back on")
    act = tl[tl["actual"].astype(str).str.lower() == "true"]
    advs = sorted(int(x) for x in act["advisory_number"].unique())
    if not advs:
        return "no_data", False, f"latest {latest_err}; no actual episodes"
    mid = advs[len(advs) // 2]
    try:
        exp = gdacs.get_exposure_adm0(eid, episodeid=mid)
        if _exp_nonempty(exp):
            # final_exposure_available=False: an earlier episode serves, so a
            # MATCH is possible, but the final/cumulative footprint is NOT
            # retrievable (the latest episode errored).
            return ("partial_no_final", False,
                    f"latest ep {advs[-1]} {latest_err}; "
                    f"earlier ep {mid} serves (final NOT available)")
        return ("reported_zero", True,
                f"latest {latest_err}; earlier ep {mid} serves empty")
    except requests.exceptions.RequestException as e:
        return ("unservable", False,
                f"latest ep {advs[-1]} {latest_err}; "
                f"earlier ep {mid} {type(e).__name__}")


def build_gdacs_diag(engine, facts, bridge, from_date="2010-01-01"):
    ev = gdacs.get_events(from_date=from_date, source="NOAA")
    have = facts["gd_have"]
    linked = facts["gd_linked"]
    rows = []
    to_probe = []
    for _, r in ev.iterrows():
        eid = int(r["eventid"])
        season = pd.to_datetime(r["from_date"]).year
        atcf = linked.get(eid) or bridge.get((_clean_name(r["name"]), season))
        base = dict(source="GDACS", eventid=eid, event_name=r["name"],
                    season=season, atcf_id=atcf,
                    matched=eid in linked,
                    match_method=("lookup" if eid in linked
                                  else ("name_year" if atcf else None)))
        if eid in have.index and bool(have.loc[eid, "has_pos"]):
            rows.append({**base, "status": "have_exposure",
                         "final_exposure_available": True,
                         "exposure_rows": int(have.loc[eid, "rows"]),
                         "total_pop_adm0": int(have.loc[eid, "pop0"]),
                         "detail": "in gdacs_exposure"})
        elif eid in have.index:
            # rows exist but NO positive value ⟹ POP_AFFECTED all -1/NULL:
            # GDACS intersected countries but never computed exposure. A data
            # gap, NOT a true zero (the storm DID hit land — see e.g. FIONA,
            # MATTHEW). Distinct from reported_zero (no rows = genuine empty).
            rows.append({**base, "status": "values_missing",
                         "final_exposure_available": False,
                         "exposure_rows": int(have.loc[eid, "rows"]),
                         "total_pop_adm0": 0,
                         "detail": "rows present but pop_exposed all NULL "
                                   "(GDACS POP_AFFECTED=-1, not computed)"})
        elif eid in linked:
            # linked but no exposure ⟹ fetch succeeded empty ⟹ zero
            rows.append({**base, "status": "reported_zero",
                         "final_exposure_available": True,
                         "exposure_rows": 0, "total_pop_adm0": 0,
                         "detail": "linked (matched) but no rows ⟹ "
                                   "GDACS computed zero exposure"})
        else:
            to_probe.append((eid, base))
    return rows, to_probe


def build_adam_diag(engine, facts, bridge, from_date="2010-01-01"):
    ev = adam.get_events(from_date=from_date, source="NOAA")
    have = facts["ad_have"]
    linked = facts["ad_linked"]
    rows, gaps = [], []
    for _, r in ev.iterrows():
        eid = int(r["event_id"])
        season = pd.to_datetime(r["from_date"]).year
        atcf = linked.get(eid) or bridge.get((_clean_name(r["name"]), season))
        base = dict(source="ADAM", eventid=eid, event_name=r["name"],
                    season=season, atcf_id=atcf,
                    matched=eid in linked,
                    match_method=("lookup" if eid in linked
                                  else ("name_year" if atcf else None)),
                    csv_url=r.get("population_csv_url"))
        if eid in have.index and bool(have.loc[eid, "has_pos"]):
            rows.append({**base, "status": "have_exposure",
                         "final_exposure_available": True,
                         "exposure_rows": int(have.loc[eid, "rows"]),
                         "total_pop_adm0": int(have.loc[eid, "pop0"]),
                         "detail": "in adam_exposure"})
        elif eid in have.index:
            # rows present but no positive value — exposure not computed.
            rows.append({**base, "status": "values_missing",
                         "final_exposure_available": False,
                         "exposure_rows": int(have.loc[eid, "rows"]),
                         "total_pop_adm0": 0,
                         "detail": "rows present but pop_exposed all NULL"})
        else:
            gaps.append((eid, base))
    return rows, gaps


def _probe_adam(eid, csv_url):
    """Fetch one ADAM gap CSV. Returns (status, detail)."""
    if not csv_url or not str(csv_url).strip():
        return "no_data", "no population_csv_url"
    try:
        df = adam.get_exposure(eid, csv_url)
        if df is None or len(df) == 0:
            return "served_zero", "CSV served but empty"
        return "recoverable_latest", f"CSV serves {len(df)} rows"
    except adam.NoExposureCSVError:
        return "served_zero", "NoExposureCSVError (no exposure in CSV)"
    except requests.exceptions.HTTPError as e:
        code = getattr(e.response, "status_code", "?")
        return ("csv_403" if code == 403 else "csv_error",
                f"HTTP {code}")
    except requests.exceptions.RequestException as e:
        return "csv_error", type(e).__name__


def main(gdacs_probe=True, adam_probe=True, upload=True):
    engine = stratus.get_engine(stage="dev")
    facts = _db_facts(engine)
    bridge = _nhc_by_name_year(engine)

    gd_rows, gd_probe = build_gdacs_diag(engine, facts, bridge)
    ad_rows, ad_gaps = build_adam_diag(engine, facts, bridge)
    print(f"GDACS: {len(gd_rows)} from-DB, {len(gd_probe)} to probe")
    print(f"ADAM:  {len(ad_rows)} from-DB, {len(ad_gaps)} gaps")

    if gdacs_probe:
        gdacs._TIMEOUT = PROBE_TIMEOUT
        probed = []   # [eid, base, status, final_avail, detail]
        for i, (eid, base) in enumerate(gd_probe, 1):
            status, final_avail, detail = _probe_gdacs(eid)
            probed.append([eid, base, status, final_avail, detail])
            print(f"  GDACS probe {i}/{len(gd_probe)} {eid} "
                  f"{base['event_name']}: {status}")
        # Reproducible escalation: re-probe timeout-class 'unservable' events
        # (NOT 500s) once at the longer budget. The set is computed from the
        # probe results, so the script alone regenerates the final CSV.
        esc = [r for r in probed
               if r[2] == "unservable" and _is_timeout(r[4])]
        if esc:
            gdacs._TIMEOUT = ESCALATE_TIMEOUT
            print(f"  escalating {len(esc)} timeout-class event(s) "
                  f"to {ESCALATE_TIMEOUT}s budget")
            for r in esc:
                r[2], r[3], r[4] = _probe_gdacs(r[0])
                print(f"    re-probe {r[0]} {r[1]['event_name']}: {r[2]}")
        for eid, base, status, final_avail, detail in probed:
            gd_rows.append({**base, "status": status,
                            "final_exposure_available": final_avail,
                            "exposure_rows": 0, "total_pop_adm0": 0,
                            "detail": detail})

    if adam_probe:
        adam._TIMEOUT = PROBE_TIMEOUT
        for i, (eid, base) in enumerate(ad_gaps, 1):
            status, detail = _probe_adam(eid, base.get("csv_url"))
            ad_rows.append({**base, "status": status,
                            "final_exposure_available": status == "recoverable_latest",
                            "exposure_rows": 0, "total_pop_adm0": 0,
                            "detail": detail})
            if i % 25 == 0 or i == len(ad_gaps):
                print(f"  ADAM probe {i}/{len(ad_gaps)}")

    df = pd.DataFrame(gd_rows + ad_rows)
    df = df.drop(columns=[c for c in ["csv_url"] if c in df.columns])
    OUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT, index=False, encoding="utf-8-sig")   # local (BOM for Excel)
    if upload:                                           # canonical copy → blob
        stratus.upload_csv_to_blob(
            df, BLOB_NAME, stage=BLOB_STAGE, container_name=BLOB_CONTAINER)
        print(f"\nuploaded → blob {BLOB_STAGE}:{BLOB_CONTAINER}/{BLOB_NAME}")
    print(f"wrote {OUT}  ({len(df)} rows)")
    print(df.groupby(["source", "status"]).size())
    return df


if __name__ == "__main__":
    main()
