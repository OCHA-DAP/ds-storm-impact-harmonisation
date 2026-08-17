"""
PDC Hazards API — parsing of captured cyclone records.

Reads the raw responses archived by `scripts/poll_pdc_cyclones.py` and turns
them into frames shaped like the GDACS equivalents in `gdacs.py`, so the two
sources can be compared directly.

Parsing is kept separate from capture: PDC serves no archive and no track
history, so the only PDC data we will ever have is what the poller captured at
the time, and changing the parse must never require re-fetching something
unre-fetchable.

Live reads (:func:`fetch_active_cyclones`, :func:`fetch_detail`) exist for
*consumers that need advisory-fresh data*, such as the daily GDACS monitor
email. PDC publishes at bulletin issuance while the poller runs 3-hourly, so
the blob archive can be up to 3 h stale at send time; an alert should call the
API. The archive's job is the historical record, which is a different job from
alert freshness. See `docs/pdc_api.md` § Update lag.

Reference for the API and its schema traps: `docs/pdc_api.md`.

Layout produced by the poller (container=`projects`)::

    ds-storm-impact-harmonisation/raw/pdc/cyclones/
        polls/<poll_ts>/_list.json
        hazards/<hazard_uuid>/<updatedAt>.json

Schema traps handled here
-------------------------
- **Avro union envelopes.** Many scalars arrive as ``{"string": v}`` /
  ``{"long": v}``; inside ``incident.snapshot.properties.map`` every value is
  wrapped. :func:`unwrap` normalises these recursively.
- **Two different uuids.** The detail object's top-level ``uuid`` is a
  state/version ID that changes on update. The stable key is ``hazard.uuid``.
- **Wind-radius index is not the threshold.** ``rad1`` is 64 kt and ``rad3`` is
  34 kt; the threshold lives in ``rad<N>SpdKt``. :func:`parse_track` keys the
  output on the actual knot value so downstream code never relies on the index.
- **Sentinel end date.** ``endedAt = 32503679999`` (2999-12-31) means "still
  active", not a real timestamp.
"""

from __future__ import annotations

import json
import math
import os
import re
from typing import Any

import ocha_stratus as stratus
import pandas as pd
import requests

BLOB_PREFIX = "ds-storm-impact-harmonisation/raw/pdc/cyclones"
CONTAINER = "projects"
PDC_BASE = "https://hazards-api.pdc.org"
TIMEOUT = 60

#: `endedAt` value PDC uses to mean "still active" (2999-12-31T23:59:59Z).
ACTIVE_SENTINEL = 32503679999

_AVRO_KEYS = {"string", "long", "int", "double", "float", "boolean", "array"}

#: Quadrants, in the order ATCF reports them.
_QUADRANTS = ["Ne", "Se", "Sw", "Nw"]

#: Columns :func:`parse_track` always emits. Radius columns are added
#: per-record, since which thresholds are reported varies by storm.
_TRACK_BASE_COLS = [
    "atcf_id",
    "position_no",
    "valid_time",
    "longitude",
    "latitude",
    "max_winds_kt",
    "gusts_kt",
    "speed_kt",
    "dir_deg",
    "saffir_simpson",
]


def unwrap(obj: Any) -> Any:
    """Recursively strip Avro union envelopes such as ``{"string": v}``.

    A dict with exactly one key drawn from the Avro scalar/array type names is
    treated as an envelope and replaced by its value. Everything else is
    traversed unchanged.
    """
    if isinstance(obj, dict):
        if len(obj) == 1:
            (key,), (val,) = obj.keys(), obj.values()
            if key in _AVRO_KEYS:
                return unwrap(val)
        return {k: unwrap(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [unwrap(v) for v in obj]
    return obj


# --------------------------------------------------------------------------
# Loading captured records
# --------------------------------------------------------------------------


def _headers() -> dict[str, str]:
    key = os.environ.get("PDC_API_KEY")
    if not key:
        raise RuntimeError("PDC_API_KEY is not set")
    return {"x-api-key": key}


def _list_records(feature_collection: dict) -> list[dict]:
    """Flatten a `/hazards` FeatureCollection into per-hazard records.

    Each record is the feature's `properties` dict plus `latitude` /
    `longitude` from its Point geometry — the position is what
    :func:`match_gdacs_storm` matches on, so a feature without one is a
    schema violation, not an ignorable gap.
    """
    records = []
    for f in feature_collection.get("features", []):
        props = dict(f.get("properties", {}))
        geom = f.get("geometry") or {}
        coords = geom.get("coordinates") if geom.get("type") == "Point" else None
        if not coords:
            raise ValueError(
                f"PDC hazard {props.get('name')!r} (uuid={props.get('uuid')!r}) "
                f"has no Point geometry — cannot match by position"
            )
        props["longitude"], props["latitude"] = coords[0], coords[1]
        records.append(props)
    return records


def fetch_active_cyclones() -> list[dict]:
    """Live `GET /hazards?types=CYCLONE` — the list-view records.

    Returns each feature's `properties` dict (uuid, name, type, severity,
    category, startedAt/updatedAt/endedAt) plus `latitude`/`longitude`
    from its geometry. Use :func:`fetch_detail` on a `uuid` for exposure
    and track.
    """
    r = requests.get(
        f"{PDC_BASE}/hazards",
        params={"types": "CYCLONE"},
        headers=_headers(),
        timeout=TIMEOUT,
    )
    r.raise_for_status()
    return _list_records(r.json())


def fetch_detail(hazard_uuid: str) -> dict:
    """Live `GET /hazards/{uuid}`, Avro-unwrapped and ready for the parsers."""
    r = requests.get(
        f"{PDC_BASE}/hazards/{hazard_uuid}", headers=_headers(), timeout=TIMEOUT
    )
    r.raise_for_status()
    return unwrap(r.json())


def _name_key(name: str) -> str:
    """Reduce a storm name to its bare token for cross-source matching.

    GDACS names storms ``DOLPHIN-26``; PDC names the same storm
    ``Typhoon Dolphin`` or ``Post-Tropical Cyclone Genevieve``. Stripping the
    GDACS year suffix and PDC's status words leaves ``DOLPHIN`` on both sides.
    """
    n = re.sub(r"-\d{2}$", "", (name or "").strip().upper())
    for word in (
        "SUPER TYPHOON", "TYPHOON", "POST-TROPICAL CYCLONE", "TROPICAL CYCLONE",
        "TROPICAL STORM", "TROPICAL DEPRESSION", "HURRICANE", "CYCLONE",
        "SUBTROPICAL STORM", "STORM",
    ):
        n = n.replace(word, " ")
    n = re.sub(r"\(.*?\)", " ", n)          # drop "(Response Support)" etc.
    return re.sub(r"[^A-Z]", "", n)


def names_agree(gdacs_name: str, pdc_name: str) -> bool:
    """Whether two storm names reduce to the same bare token.

    A corroboration signal for :func:`match_gdacs_storm`, not a matching
    key: GDACS keeps a storm's pre-naming designation ("ONE-C-26") for the
    event's whole lifetime while PDC adopts the agency-assigned name
    ("Tropical Storm Lala"), so for exactly the storms that get named
    mid-life the names of one physical storm never agree.
    """
    key = _name_key(gdacs_name)
    return bool(key) and key == _name_key(pdc_name)


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    rlat1, rlon1, rlat2, rlon2 = map(math.radians, (lat1, lon1, lat2, lon2))
    a = (
        math.sin((rlat2 - rlat1) / 2) ** 2
        + math.cos(rlat1) * math.cos(rlat2) * math.sin((rlon2 - rlon1) / 2) ** 2
    )
    return 2 * 6371.0 * math.asin(math.sqrt(a))


# Both feeds republish the same agency advisory position, so an in-sync pair
# is metres apart and an out-of-sync pair is off by one 6-hour advisory step
# (~200 km for a fast storm). Simultaneous cyclones in one basin sit many
# hundreds of km apart, so 500 km is generous without being ambiguous.
MATCH_MAX_KM = 500.0


def match_gdacs_storm(
    lat: float, lon: float, pdc_records: list[dict], max_km: float = MATCH_MAX_KM
) -> dict | None:
    """Find the PDC record for a GDACS storm position, or None.

    Returns ``{"record": <pdc record>, "distance_km": <float>}`` for the
    nearest `category == "EVENT"` record within `max_km`, or None if no
    EVENT record is that close. `RESPONSE` records are excluded: they are
    analyst-entered coordination snapshots whose exposure is unfit for
    quantitative use (see `docs/pdc_api.md`).

    Callers should corroborate the match with :func:`names_agree` and
    surface disagreements — expected for designation-stage storms, but
    worth a loud log line.
    """
    best = None
    for rec in pdc_records:
        if rec.get("category") != "EVENT":
            continue
        dist = _haversine_km(lat, lon, rec["latitude"], rec["longitude"])
        if dist <= max_km and (best is None or dist < best["distance_km"]):
            best = {"record": rec, "distance_km": dist}
    return best


def list_captured_versions(stage: str = "dev") -> pd.DataFrame:
    """List every captured hazard version.

    Returns
    -------
    DataFrame with columns ``hazard_uuid``, ``updated_at`` (int epoch
    seconds) and ``blob``, sorted by hazard then time.
    """
    rows = []
    for blob in stratus.list_container_blobs(
        name_starts_with=f"{BLOB_PREFIX}/hazards/", stage=stage
    ):
        if not blob.endswith(".json"):
            continue
        parts = blob.split("/")
        rows.append(
            {
                "hazard_uuid": parts[-2],
                "updated_at": int(parts[-1].removesuffix(".json")),
                "blob": blob,
            }
        )
    df = pd.DataFrame(rows, columns=["hazard_uuid", "updated_at", "blob"])
    return df.sort_values(["hazard_uuid", "updated_at"]).reset_index(drop=True)


def load_version(blob: str, stage: str = "dev") -> dict:
    """Load one captured detail object, Avro-unwrapped."""
    raw = stratus.load_blob_data(blob, stage=stage, container_name=CONTAINER)
    return unwrap(json.loads(raw))


def load_latest(hazard_uuid: str, stage: str = "dev") -> dict:
    """Load the most recently captured version of one hazard."""
    versions = list_captured_versions(stage=stage)
    versions = versions[versions["hazard_uuid"] == hazard_uuid]
    if versions.empty:
        raise KeyError(f"no captured versions for hazard {hazard_uuid}")
    return load_version(versions.iloc[-1]["blob"], stage=stage)


# --------------------------------------------------------------------------
# Parsing
# --------------------------------------------------------------------------


def _incident_map(detail: dict) -> dict:
    """The flat key/value snapshot under ``incident.snapshot.properties.map``."""
    return (
        detail.get("incident", {})
        .get("snapshot", {})
        .get("properties", {})
        .get("map", {})
    ) or {}


def parse_meta(detail: dict) -> dict:
    """Extract identity and provenance from a detail object.

    ``atcf_id`` is the join key to IBTrACS and is present only for
    automatically-ingested cyclones; manual entries
    (``source_name = "PDC Manual Hazard"``) carry a PDC-internal UUID in
    ``incident.sourceRecordId`` instead, and no ATCF ID.
    """
    hazard = detail.get("hazard", {}) or {}
    incident = detail.get("incident", {}) or {}
    m = _incident_map(detail)
    ended = hazard.get("endedAt")
    names = detail.get("name") or []
    return {
        "hazard_uuid": hazard.get("uuid"),
        "name": names[0]["value"] if names else None,
        "category": detail.get("category"),
        "severity": detail.get("severity"),
        "creator": hazard.get("creator"),
        "started_at": hazard.get("startedAt"),
        "updated_at": hazard.get("updatedAt"),
        "ended_at": ended,
        # PDC uses endedAt three ways: the sentinel ("no end set"), a real
        # past time, and a *projected* future end. Typhoon Dolphin was very
        # much active with endedAt set to the next day, so "active" cannot be
        # read off the sentinel alone. Report the fact; let callers decide
        # activity against whatever reference time they care about.
        "end_is_sentinel": ended == ACTIVE_SENTINEL,
        # Provenance: distinguishes auto-ingested from analyst-entered.
        "source": m.get("source"),
        "source_name": m.get("sourceName"),
        "issuer": m.get("issuer"),
        "source_record_id": incident.get("sourceRecordId"),
        "atcf_id": m.get("atcfId"),
        "advisory_num": m.get("advisoryNum"),
        "storm_status": m.get("stormStatus"),
        "saffir_simpson": m.get("saffirSimpson"),
        "max_winds_kph": m.get("maxWindsKph"),
        "pressure": m.get("pressure"),
        "region": m.get("region"),
        # Landfall block: null until the storm is close enough for the model
        # to resolve one. First observed populated on Dolphin adv 35.
        "landfall_admin0": m.get("landfallAdmin0"),
        "landfall_admin1": m.get("landfallAdmin1"),
        "landfall_time": m.get("landfallTime"),
        "hours_landfall": m.get("hoursLandfall"),
        "category_landfall": m.get("categoryLandfall"),
    }


def parse_track(detail: dict) -> pd.DataFrame:
    """Forecast positions with quadrant wind radii.

    One row per ``type="position"`` feature. Note this is **forecast only** —
    PDC carries no past track, so the earliest row is the advisory's own
    synoptic hour. Reconstructing a full track means stacking this across
    successive captures.

    Radii columns are named by their actual knot threshold
    (``r34_ne_nm`` … ``r64_nw_nm``) rather than PDC's ``rad1/2/3`` index,
    because the index order is 64/50/34 kt and inverting it silently would be
    an easy and expensive mistake.
    """
    feats = (detail.get("features", {}) or {}).get("geoJson", {}) or {}
    rows = []
    for feat in feats.get("features", []) or []:
        props = feat.get("properties") or {}
        if props.get("type") != "position":
            continue
        lon, lat = feat["geometry"]["coordinates"][:2]
        row = {
            "atcf_id": props.get("atcfId"),
            "position_no": _as_int(props.get("positionNo")),
            "valid_time": _as_ts(props.get("forecastDateUserPref")),
            "longitude": lon,
            "latitude": lat,
            "max_winds_kt": _as_float(props.get("maxWindsKt")),
            "gusts_kt": _as_float(props.get("gustsKt")),
            "speed_kt": _as_float(props.get("speedKt")),
            "dir_deg": _as_float(props.get("dirDeg")),
            "saffir_simpson": props.get("saffirSimpson"),
        }
        # rad1/rad2/rad3 -> resolve each to its own reported kt threshold.
        for idx in (1, 2, 3):
            kt = _as_int(props.get(f"rad{idx}SpdKt"))
            if kt is None:
                continue
            for quad in _QUADRANTS:
                row[f"r{kt}_{quad.lower()}_nm"] = _as_float(
                    props.get(f"rad{idx}{quad}Nm")
                )
        rows.append(row)
    if not rows:
        # Manual RESPONSE entries carry an incident Point but no forecast
        # positions, so an empty track is an ordinary outcome rather than an
        # error. Return the schema so callers can concat without special-casing.
        return pd.DataFrame(columns=_TRACK_BASE_COLS)
    return pd.DataFrame(rows).sort_values("position_no").reset_index(drop=True)


def parse_exposure(detail: dict) -> pd.DataFrame:
    """Per-country population and capital exposure.

    One row per country in ``exposure.data.totalByCountry``. ``country`` is
    ISO3, so this joins directly to the GDACS and CERF tables. Returns an
    empty frame when PDC computed no exposure (offshore storms, and manual
    entries where the compute never ran).
    """
    data = (detail.get("exposure", {}) or {}).get("data", {}) or {}
    rows = []
    for entry in data.get("totalByCountry") or []:
        pop = entry.get("population", {}) or {}
        cap = entry.get("capital", {}) or {}
        rows.append(
            {
                "iso3": entry.get("country"),
                "admin0": entry.get("admin0"),
                "pop_total": _value(pop.get("total")),
                "pop_0_14": _value(pop.get("total0_14")),
                "pop_15_64": _value(pop.get("total15_64")),
                "pop_65_plus": _value(pop.get("total65_Plus")),
                "pop_vulnerable": _value(pop.get("vulnerable")),
                "households": _value(pop.get("households")),
                "capital_total": _value(cap.get("total")),
                "capital_school": _value(cap.get("school")),
                "capital_hospital": _value(cap.get("hospital")),
            }
        )
    return pd.DataFrame(rows)


def parse_exposure_bands(detail: dict) -> pd.DataFrame:
    """Damage-band exposure from ``exposure.data.exposureLevels``.

    One row per band. Bands appear to be **discrete** (they sum to the
    reported total, like ADAM's 60/90/120 km/h bands) rather than cumulative
    (like GDACS's ``pop_34kt``/``pop_64kt``) — confirmed on Typhoon Dolphin
    only, so treat as provisional.

    PDC labels these by expected damage ("Moderate Damage; 5% of value"), not
    by wind threshold, and the underlying model is not documented. Mapping a
    band to a wind speed is therefore an open question, not something this
    function should guess at.
    """
    data = (detail.get("exposure", {}) or {}).get("data", {}) or {}
    rows = []
    for level in data.get("exposureLevels") or []:
        pop = (level.get("data") or {}).get("population") or {}
        rows.append(
            {
                "level": level.get("level"),
                "description": level.get("exposureDescription"),
                "pop_total": _value(pop.get("total")),
            }
        )
    return pd.DataFrame(rows)


def parse_bands_by_country(detail: dict) -> pd.DataFrame:
    """Damage-class exposure split by country.

    `exposureLevels[].data` carries its own nested `totalByCountry`, so the
    grain available is (country x damage band). Returns columns ``iso3``,
    ``level``, ``description``, ``pop_total``; empty when PDC computed no
    exposure.
    """
    data = (detail.get("exposure", {}) or {}).get("data", {}) or {}
    rows = []
    for level in data.get("exposureLevels") or []:
        ld = level.get("data") or {}
        for entry in ld.get("totalByCountry") or []:
            pop = (entry.get("population") or {}).get("total")
            rows.append(
                {
                    "iso3": entry.get("country"),
                    "level": level.get("level"),
                    "description": level.get("exposureDescription"),
                    "pop_total": _value(pop) or 0.0,
                }
            )
    return pd.DataFrame(
        rows, columns=["iso3", "level", "description", "pop_total"]
    )


# --------------------------------------------------------------------------
# IBTrACS join
# --------------------------------------------------------------------------


def load_ibtracs_atcf_lookup(url: str | None = None) -> pd.DataFrame:
    """IBTrACS lookup keyed on ATCF ID.

    PDC reports the forecast centre's ATCF ID directly (``WP122026``), which
    IBTrACS carries as ``USA_ATCF_ID``. That makes this an exact join —
    unlike the GDACS path, which has to match on name plus season and needs a
    hand-maintained exceptions list (see ``gdacs.match_gdacs_to_ibtracs``).

    Returns one row per storm: ``sid``, ``atcf_id``, ``name``, ``season``,
    ``basin``.
    """
    from src.datasets.gdacs import IBTRACS_LAST3_URL

    df = pd.read_csv(url or IBTRACS_LAST3_URL, skiprows=[1], low_memory=False)
    keep = ["SID", "USA_ATCF_ID", "NAME", "SEASON", "BASIN"]
    lookup = df.groupby("SID").first()[keep[1:]].reset_index()
    lookup.columns = ["sid", "atcf_id", "name", "season", "basin"]
    lookup["atcf_id"] = lookup["atcf_id"].astype(str).str.strip().str.upper()
    lookup["name"] = lookup["name"].str.upper().str.strip()
    return lookup[lookup["atcf_id"].ne("") & lookup["atcf_id"].ne("NAN")]


def match_atcf_to_sid(atcf_id: str, ibtracs: pd.DataFrame | None = None) -> str | None:
    """Resolve a PDC ``atcfId`` to an IBTrACS SID, or None if absent.

    A live storm legitimately has no SID yet: IBTrACS lags real time, so
    None here means "not in IBTrACS yet", not "no match exists".
    """
    if not atcf_id:
        return None
    if ibtracs is None:
        ibtracs = load_ibtracs_atcf_lookup()
    hit = ibtracs[ibtracs["atcf_id"] == str(atcf_id).strip().upper()]
    return None if hit.empty else hit.iloc[0]["sid"]


# --------------------------------------------------------------------------
# Small coercion helpers
#
# PDC returns most numerics as strings inside the GeoJSON feature properties,
# and occasionally omits them, so every conversion has to tolerate both.
# --------------------------------------------------------------------------


def _value(node: Any) -> float | None:
    """Pull ``.value`` out of PDC's {value, valueFormatted, ...} quad."""
    if isinstance(node, dict):
        return node.get("value")
    return node


def _as_float(v: Any) -> float | None:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _as_int(v: Any) -> int | None:
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def _as_ts(v: Any) -> pd.Timestamp | None:
    ts = pd.to_datetime(v, errors="coerce", utc=True)
    return None if pd.isna(ts) else ts
