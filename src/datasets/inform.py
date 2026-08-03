"""INFORM Risk + Severity data access for the 3RM model.

Two audiences:

1. The refresh script (`scripts/refresh_inform_composite.py`) uses
   `fetch_inform_risk()` and `load_inform_severity()` to build the
   processed parquet.
2. The book and the analyst app use `load_inform()` to read that
   parquet, then `calc_inform_composite()` or `build_training_frame()`.
"""

from __future__ import annotations

from functools import lru_cache
from io import BytesIO
from typing import Literal, TypedDict

import numpy as np
import ocha_stratus as stratus
import pandas as pd
import requests

# Blob locations
INFORM_BLOB_PATH = "ds-storm-impact-harmonisation/processed/inform.parquet"
_3RM_BLOB = (
    "ds-storm-impact-harmonisation/raw/"
    "CERF 3RM - RR Regression Model - version 1.8.xlsx"
)
_SEVERITY_BLOB = "output/acaps_inform_severity/raw.parquet"
_SEVERITY_CONTAINER = "hdx-signals"
_SEVERITY_STAGE = "prod"

# DRMKC INFORM Risk API
_API_BASE = "https://drmkc.jrc.ec.europa.eu/inform-index/API/InformAPI"
_RISK_INDICATORS = ["INFORM", "HA", "VU", "CC"]

# 3RM country name -> ISO3 (lifted from artefacts/04_inform_3rm_models.py).
# Kept as an explicit mapping rather than pycountry so edge cases like
# "oPt" -> "PSE" and "Swaziland" -> "SWZ" stay stable.
COUNTRY_TO_ISO3: dict[str, str] = {
    "Afghanistan": "AFG", "Angola": "AGO", "Antigua and Barbuda": "ATG",
    "Armenia": "ARM", "Azerbaijan": "AZE", "Bahamas": "BHS",
    "Bangladesh": "BGD", "Bolivia": "BOL", "Brazil": "BRA",
    "Burkina Faso": "BFA", "Burundi": "BDI", "Cameroon": "CMR",
    "Central African Republic": "CAF", "Chad": "TCD", "Colombia": "COL",
    "Comoros": "COM", "Cote d'Ivoire": "CIV", "Cuba": "CUB",
    "Democratic People's Republic of Korea": "PRK",
    "Democratic Republic of the Congo": "COD",
    "Djibouti": "DJI", "Dominica": "DMA", "Ecuador": "ECU",
    "Egypt": "EGY", "El Salvador": "SLV", "Equatorial Guinea": "GNQ",
    "Eritrea": "ERI", "Ethiopia": "ETH", "Fiji": "FJI",
    "Gambia": "GMB", "Guatemala": "GTM", "Guinea": "GIN",
    "Haiti": "HTI", "Honduras": "HND", "India": "IND",
    "Indonesia": "IDN", "Iraq": "IRQ",
    "Islamic Republic of Iran": "IRN", "Jamaica": "JAM",
    "Jordan": "JOR", "Kenya": "KEN", "Kyrgyzstan": "KGZ",
    "Lao People's Democratic Republic": "LAO",
    "Lebanon": "LBN", "Lesotho": "LSO", "Libya": "LBY",
    "Madagascar": "MDG", "Malawi": "MWI", "Mali": "MLI",
    "Mauritania": "MRT", "Mongolia": "MNG", "Mozambique": "MOZ",
    "Myanmar": "MMR", "Namibia": "NAM", "Nepal": "NPL",
    "Nicaragua": "NIC", "Niger": "NER", "Nigeria": "NGA",
    "Pakistan": "PAK", "Panama": "PAN", "Papua New Guinea": "PNG",
    "Peru": "PER", "Philippines": "PHL",
    "Republic of Congo": "COG", "Republic of the Sudan": "SDN",
    "Rwanda": "RWA", "Saint Vincent and the Grenadines": "VCT",
    "Samoa": "WSM", "Somalia": "SOM", "South Sudan": "SSD",
    "Sri Lanka": "LKA", "Sudan": "SDN", "Swaziland": "SWZ",
    "Syrian Arab Republic": "SYR", "Timor-Leste": "TLS",
    "Tonga": "TON", "Uganda": "UGA", "Ukraine": "UKR",
    "United Republic of Tanzania": "TZA", "Vanuatu": "VUT",
    "Venezuela": "VEN", "Viet Nam": "VNM", "Yemen": "YEM",
    "Zambia": "ZMB", "Zimbabwe": "ZWE", "oPt": "PSE",
}


class CompositeLookup(TypedDict):
    composite: float
    risk: float
    risk_year: int   # the GNAYear the Risk score actually comes from
    severity: float | None
    source: Literal["blended", "risk_only"]
    risk_carried: bool  # True when risk_year != requested year


# ── Composite rule ───────────────────────────────────────────────────

def _calc_composite_value(
    risk: float | None, severity: float | None
) -> float | None:
    """Mean of Risk + Severity when both present, else Risk alone.

    Matches the rule in book/02c-analysis-inform.qmd:314-319. Kept as
    a single helper so the book, the app, and build_training_frame all
    apply identical logic.
    """
    if risk is None or pd.isna(risk):
        return None
    if severity is None or pd.isna(severity):
        return float(risk)
    return (float(risk) + float(severity)) / 2


# ── Upstream fetchers (used by the refresh script) ───────────────────

def fetch_inform_risk(years: list[int] | None = None) -> pd.DataFrame:
    """Fetch INFORM Risk from the DRMKC API for the given years.

    If `years` is None, fetches all available years (the Trends endpoint
    returns every year in a single call). Returns one row per iso3+year
    with columns INFORM, HA, VU, CC.
    """
    wf = requests.get(f"{_API_BASE}/Workflows/Default", timeout=30).json()
    wf_id = wf["WorkflowId"]

    r = requests.get(
        f"{_API_BASE}/Countries/Trends/",
        params={"WorkflowId": wf_id, "IndicatorId": ",".join(_RISK_INDICATORS)},
        timeout=60,
    )
    r.raise_for_status()
    raw = pd.DataFrame(r.json())

    df = (
        raw[["Iso3", "GNAYear", "IndicatorId", "IndicatorScore"]]
        .pivot_table(
            index=["Iso3", "GNAYear"],
            columns="IndicatorId",
            values="IndicatorScore",
            aggfunc="first",
        )
        .reset_index()
        .rename(columns={"Iso3": "iso3", "GNAYear": "year"})
    )
    df.columns.name = None
    df = df.rename(columns={
        "INFORM": "inform_risk", "HA": "inform_ha",
        "VU": "inform_vu", "CC": "inform_cc",
    })
    df["year"] = df["year"].astype(int)
    if years is not None:
        df = df[df["year"].isin(years)].copy()
    return df


def load_inform_severity() -> pd.DataFrame:
    """Load INFORM Severity aggregated to iso3+year_month.

    Takes max severity where a country-month has multiple country-level
    records (rare; 8 countries per book:234-238).
    """
    raw = stratus.load_parquet_from_blob(
        _SEVERITY_BLOB,
        stage=_SEVERITY_STAGE,
        container_name=_SEVERITY_CONTAINER,
    )
    raw["date"] = pd.to_datetime(raw["date"])
    cl = raw[raw["country_level"] == "Yes"].dropna(
        subset=["inform_severity_index"]
    ).copy()
    cl["year_month"] = cl["date"].dt.to_period("M")
    agg = (
        cl.groupby(["iso3", "year_month"])
        .agg(
            inform_severity=("inform_severity_index", "max"),
            inform_impact=("impact_crisis", "max"),
            inform_conditions=("people_condition", "max"),
            inform_complexity=("complexity", "max"),
        )
        .reset_index()
    )
    agg["year"] = agg["year_month"].dt.year.astype(int)
    agg["month"] = agg["year_month"].dt.month.astype(int)
    return agg


def build_inform_frame(
    risk: pd.DataFrame, severity: pd.DataFrame
) -> pd.DataFrame:
    """Expand annual Risk to monthly rows and left-join Severity.

    Result is one row per (iso3, year, month) for every iso3+year in
    `risk`. `inform_severity` and its pillars are NaN where Severity
    is unavailable for that exact month.
    """
    months = pd.DataFrame({"month": range(1, 13)})
    expanded = risk.merge(months, how="cross")
    expanded["year_month"] = pd.to_datetime(
        expanded[["year", "month"]].assign(day=1)
    ).dt.to_period("M")

    merged = expanded.merge(
        severity.drop(columns=["year", "month"]),
        on=["iso3", "year_month"],
        how="left",
    )
    return merged.sort_values(["iso3", "year", "month"]).reset_index(drop=True)


# ── Consumers (book + app) ───────────────────────────────────────────

@lru_cache(maxsize=1)
def load_inform() -> pd.DataFrame:
    """Read the processed INFORM parquet from blob (cached per process)."""
    df = stratus.load_parquet_from_blob(INFORM_BLOB_PATH)
    df["year"] = df["year"].astype(int)
    df["month"] = df["month"].astype(int)
    df["year_month"] = df["year_month"].astype("period[M]")
    if "risk_source_year" in df.columns:
        df["risk_source_year"] = df["risk_source_year"].astype(int)
    else:
        # Older parquet without forward-carry: assume no carry.
        df["risk_source_year"] = df["year"]
    return df


def calc_inform_composite(
    df: pd.DataFrame,
    iso3: str,
    year: int,
    month: int | None = None,
) -> CompositeLookup | None:
    """Look up Risk + Severity for (iso3, year[, month]) and compute composite.

    Returns None if no Risk row exists for iso3+year. When `month` is
    None, Severity is ignored (Risk-only composite). When `month` is
    given, Severity is pulled for that exact year_month; if unavailable,
    falls back to Risk alone.

    The parquet may forward-carry the latest published INFORM Risk
    assessment into the current calendar year (since Risk always lags by
    ~1 year). When that happens, `risk_source_year` reflects the original
    GNAYear and `risk_carried` is True.
    """
    risk_row = df[(df["iso3"] == iso3) & (df["year"] == int(year))]
    if risk_row.empty:
        return None
    risk = risk_row["inform_risk"].iloc[0]
    if pd.isna(risk):
        return None
    risk_source_year = int(risk_row["risk_source_year"].iloc[0])

    severity: float | None = None
    if month is not None:
        sev_row = risk_row[risk_row["month"] == int(month)]
        if not sev_row.empty:
            sv = sev_row["inform_severity"].iloc[0]
            if not pd.isna(sv):
                severity = float(sv)

    composite = _calc_composite_value(float(risk), severity)
    assert composite is not None
    return {
        "composite": composite,
        "risk": float(risk),
        "risk_year": risk_source_year,
        "severity": severity,
        "source": "blended" if severity is not None else "risk_only",
        "risk_carried": risk_source_year != int(year),
    }


# ── Training frame ───────────────────────────────────────────────────

def _load_3rm() -> pd.DataFrame:
    """Load 3RM MergedData. Lifted from artefacts/04_inform_3rm_models.py."""
    raw = stratus.load_blob_data(_3RM_BLOB)
    df = pd.read_excel(BytesIO(raw), sheet_name="MergedData")
    df = df.rename(columns={"Application Code": "ApplicationCode"})
    df["date"] = pd.to_datetime(df["Date of Most Recent Submission"])
    df["iso3"] = df["Country"].map(COUNTRY_TO_ISO3)
    df["year"] = df["Year"].astype(int)
    df["year_month"] = df["date"].dt.to_period("M")
    return df


def build_training_frame(
    inform: pd.DataFrame | None = None,
    cerf: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build the analysis frame the book and the app share.

    Joins 3RM CERF allocations with INFORM Risk (iso3+year) and
    Severity (iso3+year_month), then computes `inform_composite`
    using the same rule as `calc_inform_composite()`. Returned frame
    has LogApproved, LogRequired, LogTargeted and the 8 emergency-type
    dummies ready for `fit_model()`.

    Args are injectable for testing; defaults load from blob.
    """
    if cerf is None:
        cerf = _load_3rm()
    if inform is None:
        inform = load_inform()

    # Training must use real INFORM assessments, not forward-carried ones.
    if "risk_source_year" in inform.columns:
        inform = inform[inform["risk_source_year"] == inform["year"]]

    # One Risk row per (iso3, year); month doesn't matter for the annual join.
    risk_yearly = (
        inform[["iso3", "year", "inform_risk", "inform_ha",
                "inform_vu", "inform_cc"]]
        .drop_duplicates(subset=["iso3", "year"])
    )
    sev_monthly = inform[
        ["iso3", "year_month", "inform_severity", "inform_impact",
         "inform_conditions", "inform_complexity"]
    ]

    df = cerf.merge(risk_yearly, on=["iso3", "year"], how="left")
    df = df.merge(sev_monthly, on=["iso3", "year_month"], how="left")

    df["inform_composite"] = np.where(
        df["inform_severity"].notna(),
        (df["inform_risk"] + df["inform_severity"]) / 2,
        df["inform_risk"],
    )
    return df
