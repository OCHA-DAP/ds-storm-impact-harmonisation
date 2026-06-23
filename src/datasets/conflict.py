"""Conflict-allocation training frame: Finn xlsx + live ACLED + live IDMC.

Mirrors the storm-side `src/datasets/inform.py` pattern. The book and
the app both call `load_conflict_training_frame()`; the refresh script
calls `build_conflict_training_frame()` and writes the result to blob.

Sources:
- Finn xlsx (frozen 2025) → LogApproved, CIRV, LogTargeted, LogRequired
  (= log Total Amount Required), identifiers.
- ACLED `hdx-signals/output/acled_conflict/raw.parquet` → monthly
  fatalities → LogMonthlyFatalities.
- IDMC `global/idmc/displacement_daily.parquet` (Conflict-only) →
  displacement_30d at exact allocation date → LogIDPs30d.
- Xuan review xlsx → `xuan_refugee_excluded` flag for the 6 cross-border
  refugee allocations that don't belong in a conflict-only model.

Match keys: country + year + amount_approved (Xuan ↔ Finn) and
iso3 + alloc_date (Finn ↔ IDMC).
"""

from __future__ import annotations

from datetime import datetime, timezone
from functools import lru_cache
from io import BytesIO

import numpy as np
import ocha_stratus as stratus
import pandas as pd

CONFLICT_TRAINING_BLOB_PATH = (
    "ds-storm-impact-harmonisation/processed/conflict_training_frame.parquet"
)

_FINN_BLOB = (
    "ds-storm-impact-harmonisation/raw/cerf/finn/"
    "CERF - Conflict Model - RR Regression Model - version 1.0.xlsx"
)
_ACLED_BLOB = "output/acled_conflict/raw.parquet"
_IDMC_BLOB = "idmc/displacement_daily.parquet"
_XUAN_BLOB = (
    "ds-cerf-allocation-patterns/processed/"
    "120226_CERF allocations with ACLED data.xlsx"
)

# ISO 3166-1 numeric → alpha-3 for the 34 countries Finn touches.
_ISO_NUM_TO_A3 = {
    4: "AFG", 51: "ARM", 31: "AZE", 108: "BDI", 854: "BFA", 76: "BRA",
    140: "CAF", 120: "CMR", 180: "COD", 170: "COL", 818: "EGY", 231: "ETH",
    332: "HTI", 417: "KGZ", 422: "LBN", 434: "LBY", 466: "MLI", 104: "MMR",
    508: "MOZ", 562: "NER", 566: "NGA", 604: "PER", 275: "PSE", 729: "SDN",
    706: "SOM", 728: "SSD", 760: "SYR", 148: "TCD", 800: "UGA", 804: "UKR",
    862: "VEN", 887: "YEM", 894: "ZMB", 716: "ZWE",
}


# ── Component loaders ────────────────────────────────────────────────

def _load_finn() -> pd.DataFrame:
    """Finn's Regression_Data + LogRequired from Master_Data."""
    raw = stratus.load_blob_data(_FINN_BLOB, stage="dev")
    reg = pd.read_excel(BytesIO(raw), sheet_name="Regression_Data")
    master = pd.read_excel(BytesIO(raw), sheet_name="Master_Data")
    df = reg.merge(master[["Application Code", "LogRequired"]],
                   on="Application Code", how="left")
    df["alloc_date"] = pd.to_datetime(
        dict(year=df["Year"], month=df["Month"], day=df["Day"])
    )
    return df


def _load_acled_monthly(iso3_keep: set[str]) -> pd.DataFrame:
    """ACLED conflict events → fatalities per (iso3, year, month)."""
    df = stratus.load_parquet_from_blob(
        _ACLED_BLOB, stage="prod", container_name="hdx-signals"
    )
    df["iso3"] = df["iso"].map(_ISO_NUM_TO_A3)
    df = df[df["iso3"].isin(iso3_keep)].copy()
    df["event_date"] = pd.to_datetime(df["event_date"])
    df["Year"] = df["event_date"].dt.year
    df["Month"] = df["event_date"].dt.month
    return (df.groupby(["iso3", "Year", "Month"], as_index=False)
              ["fatalities"].sum())


def _load_idmc_30d(iso3_keep: set[str]) -> pd.DataFrame:
    """Conflict-only IDMC daily timeseries with precomputed 30d sum."""
    df = stratus.load_parquet_from_blob(
        _IDMC_BLOB, stage="dev", container_name="global"
    )
    df = df[df["displacement_type"] == "Conflict"].copy()
    df = df[df["iso3"].isin(iso3_keep)].copy()
    df["date"] = pd.to_datetime(df["date"])
    return df[["iso3", "date", "displacement_30d"]]


def _load_xuan_refugees() -> pd.DataFrame:
    """Xuan-verified Refugee allocations as (country, year, amount_usd) keys."""
    raw = stratus.load_blob_data(_XUAN_BLOB, stage="dev")
    df = pd.read_excel(BytesIO(raw))
    return df[df["Refugee Situation"] == "Refugee"][
        ["country_harmonized", "year", "amount_usd"]
    ].copy()


# ── Build ────────────────────────────────────────────────────────────

def build_conflict_training_frame() -> pd.DataFrame:
    """Build the corrected, live-data conflict training frame.

    Schema:
        Application Code, ISO3, Country, Year, Month, Day, alloc_date,
        Emergency Type, Amount Approved, LogApproved,
        CIRV, LogRequired, LogTargeted,
        Monthly Fatalities, LogMonthlyFatalities,
        IDPs 30d, LogIDPs30d,
        xuan_refugee_excluded (bool — True ⇒ drop in conflict-model fits),
        refreshed_at (UTC ISO8601)

    Live ACLED + IDMC values fully replace Finn's frozen columns; Finn's
    originals are accessible via the source xlsx if needed.
    """
    finn = _load_finn()
    iso3_keep = set(finn["ISO3"].unique())

    acled = _load_acled_monthly(iso3_keep)
    idmc = _load_idmc_30d(iso3_keep)
    xuan_refs = _load_xuan_refugees()

    # Drop Finn's frozen versions of the live-replaced columns to avoid
    # merge collisions; live values fully replace them downstream.
    finn = finn.drop(columns=[
        c for c in ["Monthly Fatalities", "IDPs 30d",
                    "Log Monthly Fatalities", "Log IDPs 30d"]
        if c in finn.columns
    ])

    df = (
        finn
        .merge(acled.rename(columns={"fatalities": "Monthly Fatalities"}),
               left_on=["ISO3", "Year", "Month"],
               right_on=["iso3", "Year", "Month"], how="left")
        .drop(columns=["iso3"])
        .merge(idmc.rename(columns={"displacement_30d": "IDPs 30d"}),
               left_on=["ISO3", "alloc_date"],
               right_on=["iso3", "date"], how="left")
        .drop(columns=["iso3", "date"])
    )

    df["Monthly Fatalities"] = df["Monthly Fatalities"].fillna(0)
    df["IDPs 30d"] = df["IDPs 30d"].fillna(0)
    df["LogMonthlyFatalities"] = np.log(df["Monthly Fatalities"] + 1)
    df["LogIDPs30d"] = np.log(df["IDPs 30d"] + 1)

    # inform_composite for live-prediction parity: the production app
    # has Risk + Severity but not CIRV (needs IPC + IASC pillars we
    # don't pull). Per ch. 02d the gap between CIRV and inform_composite
    # is ~1% Adj R² on this sample, so we substitute.
    from src.datasets.inform import _calc_composite_value, load_inform  # noqa: PLC0415
    inform = load_inform()
    inform = inform[inform["risk_source_year"] == inform["year"]]
    df["year_month"] = df["alloc_date"].dt.to_period("M")
    df = df.merge(
        inform[["iso3", "year_month", "inform_risk", "inform_severity"]],
        left_on=["ISO3", "year_month"], right_on=["iso3", "year_month"],
        how="left",
    ).drop(columns=["iso3", "year_month"])
    df["inform_composite"] = [
        _calc_composite_value(r, s)
        for r, s in zip(df["inform_risk"], df["inform_severity"])
    ]

    # Xuan refugee flag: country + year + amount approved.
    flagged = df.merge(
        xuan_refs.assign(_refugee=True),
        left_on=["Country", "Year", "Amount Approved"],
        right_on=["country_harmonized", "year", "amount_usd"], how="left",
    )["_refugee"].fillna(False).astype(bool)
    df["xuan_refugee_excluded"] = flagged.values

    df["refreshed_at"] = datetime.now(timezone.utc).isoformat()

    keep_cols = [
        "Application Code", "ISO3", "Country", "Year", "Month", "Day",
        "alloc_date", "Emergency Type", "Amount Approved", "LogApproved",
        "CIRV", "inform_risk", "inform_severity", "inform_composite",
        "LogRequired", "LogTargeted",
        "Monthly Fatalities", "LogMonthlyFatalities",
        "IDPs 30d", "LogIDPs30d",
        "xuan_refugee_excluded", "refreshed_at",
    ]
    return df[keep_cols].copy()


# ── Load (cached) ────────────────────────────────────────────────────

@lru_cache(maxsize=1)
def load_conflict_training_frame() -> pd.DataFrame:
    """Read the processed conflict training frame from blob (cached)."""
    df = stratus.load_parquet_from_blob(CONFLICT_TRAINING_BLOB_PATH)
    df["alloc_date"] = pd.to_datetime(df["alloc_date"])
    df["xuan_refugee_excluded"] = df["xuan_refugee_excluded"].astype(bool)
    return df
