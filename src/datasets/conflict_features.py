"""Engineered conflict-context features for CERF allocation prediction.

Finn's xlsx exposed two raw counts: `Monthly Fatalities` and `IDPs 30d`.
Both are level-only, single-window, country-naive — a 1,000-fatality
month is treated identically in COD (chronic) and PER (unprecedented),
and a 30d window is an arbitrary slice of a process that has natural
acute / chronic / annual rhythms.

This module builds a richer per-allocation feature panel:

- **Multi-window levels** at 30 / 90 / 180 / 365 days preceding the
  allocation date (separately on the raw count and on the country
  z-score).
- **Momentum** — recent rate vs long-run baseline (acute over chronic).
- **YoY change** — same 90d window now vs a year ago.
- **Decision-lag versions** — the same windows ending 30 days *before*
  the allocation date, since an allocation decision is largely informed
  by what was visible weeks before sign-off.
- **ACLED event-type breakdown** when available (Battles / Violence
  against civilians / Explosions). Distinguishes inter-army conflict
  from atrocity-pattern violence — they tend to attract different
  humanitarian responses.

Why country z-scores? Mexico has high cartel-violence fatalities but
CERF doesn't allocate there; Sudan has high battle fatalities and CERF
does. Raw logged counts treat both as equivalent inputs. A z-score
against the country's own historical baseline normalises out the
"baseline conflict level" of the country before the model sees the
feature. Note: for chronically-violent countries this *deflates*
high-fatality periods (their baseline is already high), which is
directionally what we want for the CERF response question.

Build pattern
-------------
    feats = build_alloc_features(df_alloc, acled_events, idmc_daily)
    df_aug = df_alloc.merge(feats, on=["iso3", "alloc_date"], how="left")

`acled_events` must have columns {iso3, event_date, fatalities}; if
`event_type` is present we also produce per-event-type window features.
`idmc_daily` must have {iso3, displacement_type, date,
displacement_daily} and is filtered to displacement_type='Conflict'
inside this module.
"""

from __future__ import annotations

import numpy as np
import ocha_stratus as stratus
import pandas as pd

from src.datasets.conflict import _ISO_NUM_TO_A3

# Window lengths (days) for level features.
WINDOWS_DAYS = (30, 90, 180, 365)
# Decision-lag offset (days) — how long before alloc_date the decision
# was effectively made. 30d is the conservative default.
DECISION_LAG_DAYS = 30
# ACLED event types that carry the strongest "humanitarian-response"
# signal — battles and atrocities. Riots, Protests and Strategic
# developments are excluded by default since they map weakly onto the
# conflict-allocation decision.
ACLED_RESPONSE_RELEVANT = (
    "Battles",
    "Violence against civilians",
    "Explosions/Remote violence",
)
# Stable, lowercase column-name slugs for the response-relevant types.
ACLED_TYPE_SLUG = {
    "Battles": "battles",
    "Violence against civilians": "vac",
    "Explosions/Remote violence": "explosions",
}


# ── Loaders ──────────────────────────────────────────────────────────

def load_acled_events(iso3_keep: set[str] | None = None) -> pd.DataFrame:
    """Load event-level ACLED with iso3 attached, dates parsed.

    Returns columns: iso3, event_date, event_type, fatalities.
    `iso3_keep` (optional) filters to a country whitelist.
    """
    df = stratus.load_parquet_from_blob(
        "output/acled_conflict/raw.parquet",
        stage="prod", container_name="hdx-signals",
    )
    df["iso3"] = df["iso"].map(_ISO_NUM_TO_A3)
    df = df.dropna(subset=["iso3"])
    if iso3_keep is not None:
        df = df[df["iso3"].isin(iso3_keep)]
    df = df.copy()
    df["event_date"] = pd.to_datetime(df["event_date"])
    return df[["iso3", "event_date", "event_type", "fatalities"]]


def load_idmc_daily() -> pd.DataFrame:
    """Load the conflict-typed IDMC daily timeseries."""
    df = stratus.load_parquet_from_blob(
        "idmc/displacement_daily.parquet",
        stage="dev", container_name="global",
    )
    df = df[df["displacement_type"] == "Conflict"].copy()
    df["date"] = pd.to_datetime(df["date"])
    return df


# ── ACLED features ───────────────────────────────────────────────────

def _acled_country_baseline(acled_events: pd.DataFrame) -> pd.DataFrame:
    """Per-country monthly-fatalities mean & std for z-score normalisation.

    Computed across the full ACLED history available, NOT just the
    training window — we want a stable baseline that doesn't shift
    when a new allocation gets added.
    """
    df = acled_events.copy()
    df["year_month"] = df["event_date"].dt.to_period("M")
    monthly = (
        df.groupby(["iso3", "year_month"], as_index=False)["fatalities"].sum()
    )
    return monthly.groupby("iso3", as_index=False)["fatalities"].agg(
        baseline_mean="mean", baseline_std="std",
    )


def _window_sum(events: pd.DataFrame, end: pd.Timestamp,
                days: int, col: str = "fatalities") -> float:
    """Sum `col` for events with event_date in (end - days, end]."""
    start = end - pd.Timedelta(days=days - 1)
    mask = (events["event_date"] >= start) & (events["event_date"] <= end)
    return float(events.loc[mask, col].sum())


def _window_count(events: pd.DataFrame, end: pd.Timestamp, days: int) -> int:
    """Number of events with event_date in (end - days, end]."""
    start = end - pd.Timedelta(days=days - 1)
    mask = (events["event_date"] >= start) & (events["event_date"] <= end)
    return int(mask.sum())


def _zscore_from_baseline(
    monthly_rate: float, baseline: pd.Series,
) -> float | None:
    """Z-score of an observed monthly fatality rate against country baseline."""
    if pd.isna(baseline.get("baseline_mean")) or pd.isna(
        baseline.get("baseline_std")
    ):
        return None
    sd = baseline["baseline_std"]
    if not sd or sd == 0:
        return None
    return float((monthly_rate - baseline["baseline_mean"]) / sd)


def _build_acled_row(
    iso3: str,
    alloc_date: pd.Timestamp,
    events_country: pd.DataFrame,
    baseline_row: pd.Series | None,
    has_event_type: bool,
) -> dict:
    """Build all ACLED-derived features for one (iso3, alloc_date)."""
    feats: dict = {}
    end_now = alloc_date
    end_lag = alloc_date - pd.Timedelta(days=DECISION_LAG_DAYS)

    # Multi-window levels — fatalities & event count, current and lag.
    for days in WINDOWS_DAYS:
        feats[f"fatalities_{days}d"] = _window_sum(
            events_country, end_now, days
        )
        feats[f"events_{days}d"] = _window_count(
            events_country, end_now, days
        )
        feats[f"fatalities_{days}d_lag30"] = _window_sum(
            events_country, end_lag, days
        )

    # Country-relative z-scores: convert window sum to monthly rate, then z.
    if baseline_row is not None:
        for days in WINDOWS_DAYS:
            monthly_rate = feats[f"fatalities_{days}d"] / (days / 30.0)
            feats[f"fatalities_{days}d_z"] = _zscore_from_baseline(
                monthly_rate, baseline_row
            )
            monthly_rate_lag = feats[f"fatalities_{days}d_lag30"] / (days / 30.0)
            feats[f"fatalities_{days}d_lag30_z"] = _zscore_from_baseline(
                monthly_rate_lag, baseline_row
            )

    # Momentum: recent rate vs annual baseline (log ratio for symmetry).
    annual_rate = feats["fatalities_365d"] / 365.0
    recent_rate = feats["fatalities_30d"] / 30.0
    eps = 1e-3
    feats["fatalities_momentum"] = float(
        np.log((recent_rate + eps) / (annual_rate + eps))
    )

    # Year-on-year change in 90d fatalities (window now vs same window
    # ending 365d earlier).
    yoy_end = alloc_date - pd.Timedelta(days=365)
    feats["fatalities_yoy_change_90d"] = (
        feats["fatalities_90d"] - _window_sum(events_country, yoy_end, 90)
    )

    # Per-event-type windows (Battles, VAC, Explosions).
    if has_event_type:
        for etype in ACLED_RESPONSE_RELEVANT:
            sub = events_country[events_country["event_type"] == etype]
            slug = ACLED_TYPE_SLUG[etype]
            for days in (90, 180):
                feats[f"fatalities_{slug}_{days}d"] = _window_sum(
                    sub, end_now, days
                )
        # Response-relevant total (all three event types combined).
        sub_rr = events_country[
            events_country["event_type"].isin(ACLED_RESPONSE_RELEVANT)
        ]
        for days in (90, 180):
            feats[f"fatalities_response_relevant_{days}d"] = _window_sum(
                sub_rr, end_now, days
            )
    return feats


# ── IDMC features ────────────────────────────────────────────────────

def _idmc_country_baseline(idmc_conflict: pd.DataFrame) -> pd.DataFrame:
    """Per-country monthly-IDPs mean & std for z-score normalisation."""
    df = idmc_conflict.copy()
    df["year_month"] = df["date"].dt.to_period("M")
    monthly = (
        df.groupby(["iso3", "year_month"], as_index=False)["displacement_daily"]
        .sum()
    )
    return monthly.groupby("iso3", as_index=False)["displacement_daily"].agg(
        idmc_baseline_mean="mean", idmc_baseline_std="std",
    )


def _build_idmc_row(
    iso3: str,
    alloc_date: pd.Timestamp,
    daily_country: pd.DataFrame,
    baseline_row: pd.Series | None,
) -> dict:
    """Build all IDMC-derived features for one (iso3, alloc_date)."""
    feats: dict = {}
    end_now = alloc_date
    end_lag = alloc_date - pd.Timedelta(days=DECISION_LAG_DAYS)

    for days in WINDOWS_DAYS:
        start_now = end_now - pd.Timedelta(days=days - 1)
        mask_now = (daily_country["date"] >= start_now) & (
            daily_country["date"] <= end_now
        )
        feats[f"idps_{days}d"] = float(
            daily_country.loc[mask_now, "displacement_daily"].sum()
        )

        start_lag = end_lag - pd.Timedelta(days=days - 1)
        mask_lag = (daily_country["date"] >= start_lag) & (
            daily_country["date"] <= end_lag
        )
        feats[f"idps_{days}d_lag30"] = float(
            daily_country.loc[mask_lag, "displacement_daily"].sum()
        )

    if baseline_row is not None and not pd.isna(
        baseline_row.get("idmc_baseline_std")
    ):
        sd = baseline_row["idmc_baseline_std"]
        mu = baseline_row["idmc_baseline_mean"]
        if sd and sd > 0:
            for days in WINDOWS_DAYS:
                monthly_rate = feats[f"idps_{days}d"] / (days / 30.0)
                feats[f"idps_{days}d_z"] = float((monthly_rate - mu) / sd)

    # Momentum + YoY for IDPs.
    eps = 1e-3
    annual_rate = feats["idps_365d"] / 365.0
    recent_rate = feats["idps_30d"] / 30.0
    feats["idps_momentum"] = float(
        np.log((recent_rate + eps) / (annual_rate + eps))
    )

    yoy_end = alloc_date - pd.Timedelta(days=365)
    yoy_start = yoy_end - pd.Timedelta(days=89)
    mask_yoy = (daily_country["date"] >= yoy_start) & (
        daily_country["date"] <= yoy_end
    )
    feats["idps_yoy_change_90d"] = feats["idps_90d"] - float(
        daily_country.loc[mask_yoy, "displacement_daily"].sum()
    )

    return feats


# ── Public builder ───────────────────────────────────────────────────

def build_alloc_features(
    df_alloc: pd.DataFrame,
    acled_events: pd.DataFrame,
    idmc_daily: pd.DataFrame,
    iso3_col: str = "iso3",
    date_col: str = "alloc_date",
) -> pd.DataFrame:
    """Build the engineered feature panel for a set of allocations.

    Parameters
    ----------
    df_alloc
        Allocation-keyed frame with at minimum {iso3, alloc_date} columns.
    acled_events
        Event-level ACLED with {iso3, event_date, fatalities}, optionally
        {event_type}.
    idmc_daily
        IDMC daily timeseries with {iso3, displacement_type, date,
        displacement_daily}. Filtered to Conflict inside the function.

    Returns
    -------
    A frame keyed on (iso3, alloc_date) with the engineered features.
    Use a left-merge to attach to `df_alloc`.

    Logged versions
    ---------------
    Window-level counts are returned unlogged. The caller decides log
    transforms — typically `np.log(x + 1)`. Z-scores, momentum and YoY
    are already on a meaningful scale.
    """
    acled_events = acled_events.copy()
    acled_events["event_date"] = pd.to_datetime(acled_events["event_date"])
    has_event_type = "event_type" in acled_events.columns

    idmc_conflict = idmc_daily[
        idmc_daily["displacement_type"] == "Conflict"
    ].copy()
    idmc_conflict["date"] = pd.to_datetime(idmc_conflict["date"])

    acled_baseline = _acled_country_baseline(acled_events).set_index("iso3")
    idmc_baseline = _idmc_country_baseline(idmc_conflict).set_index("iso3")

    # Pre-group for speed: many allocations share an iso3.
    acled_by_iso = {
        iso: g for iso, g in acled_events.groupby("iso3", sort=False)
    }
    idmc_by_iso = {
        iso: g for iso, g in idmc_conflict.groupby("iso3", sort=False)
    }

    rows = []
    for _, alloc in df_alloc[[iso3_col, date_col]].drop_duplicates().iterrows():
        iso3 = alloc[iso3_col]
        alloc_date = pd.Timestamp(alloc[date_col])

        events_country = acled_by_iso.get(iso3, acled_events.iloc[0:0])
        daily_country = idmc_by_iso.get(iso3, idmc_conflict.iloc[0:0])

        acled_base_row = (
            acled_baseline.loc[iso3]
            if iso3 in acled_baseline.index else None
        )
        idmc_base_row = (
            idmc_baseline.loc[iso3]
            if iso3 in idmc_baseline.index else None
        )

        row = {iso3_col: iso3, date_col: alloc_date}
        row.update(_build_acled_row(
            iso3, alloc_date, events_country,
            acled_base_row, has_event_type,
        ))
        row.update(_build_idmc_row(
            iso3, alloc_date, daily_country, idmc_base_row,
        ))
        rows.append(row)

    return pd.DataFrame(rows)
