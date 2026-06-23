"""Fetch IDMC IDU and write a daily displacement parquet to blob.

Output: container=`global`, stage=`dev`, blob=`idmc/displacement_daily.parquet`.
Schema matches Finn's CSV (the only known-correct reference for this data):

    iso3, displacement_type, date, displacement_daily,
    displacement_7d, displacement_30d

Why this exists
---------------
hdx-signals (`OCHA-DAP/hdx-signals`) computes this exact timeseries in
memory via the R `idmc::idmc_transform_daily()` reference implementation,
but only persists alert rows to blob. We replicate that logic in Python
and persist the full timeseries so downstream apps (CERF predictor,
chapter 02d) can read it directly.

Role-priority logic (mirrors `idmc/R/idmc_transform_daily.R:79-103`):
- Per `event_id`: keep all `Recommended figure` rows if any exist.
- Otherwise: take the latest `Triangulation` row per (event_id, location)
  by `created_at`, then collapse multi-location triangulation events to
  one row per event_id (sum figures, min start, max end).

Run manually: `uv run python scripts/refresh_idmc_displacement.py`.
Same overwrite semantics as hdx-signals — re-running clobbers the blob.
"""

from __future__ import annotations

import os
from datetime import date

import httpx
import numpy as np
import ocha_stratus as stratus
import pandas as pd
from dotenv import load_dotenv

_ = load_dotenv()

OUT_BLOB = "idmc/displacement_daily.parquet"
OUT_CONTAINER = "global"
OUT_STAGE = "dev"
MIN_DATE = pd.Timestamp("2018-01-01")
EXCLUDE_ISO3 = {"ATA"}  # Antarctica — same exclusion as hdx-signals raw_displacement.R

REQUIRED_COLS = [
    "event_id", "iso3", "displacement_type", "figure", "role",
    "displacement_start_date", "displacement_end_date",
    "locations_coordinates", "created_at",
]


def fetch_idmc_events() -> pd.DataFrame:
    """Pull the IDMC IDU snapshot (event-level)."""
    url = os.environ["IDMC_API"]
    with httpx.Client(timeout=120.0, follow_redirects=True) as client:
        r = client.get(url)
        r.raise_for_status()
        data = r.json()
    df = pd.DataFrame(data)

    for col in ("displacement_start_date", "displacement_end_date", "created_at"):
        df[col] = pd.to_datetime(df[col], errors="coerce")
    df["figure"] = pd.to_numeric(df["figure"], errors="coerce")
    return df


def select_per_event_role(df: pd.DataFrame) -> pd.DataFrame:
    """Per-event_id role priority: Recommended > Triangulation.

    Triangulation events get reduced to one row per (event_id, location)
    via latest `created_at`, then collapsed across locations to a single
    row per event_id (sum figures, min start, max end). Recommended rows
    are kept as-is so multi-location splits flow into the daily expansion
    naturally.
    """
    df = df.dropna(subset=["event_id", "figure"])
    df = df[df["figure"] > 0].copy()

    rec = df[df["role"] == "Recommended figure"]
    rec_event_ids = set(rec["event_id"].unique())

    tri = df[(df["role"] == "Triangulation")
             & (~df["event_id"].isin(rec_event_ids))].copy()
    if not tri.empty:
        tri = (tri.sort_values("created_at")
                  .drop_duplicates(subset=["event_id", "locations_coordinates"],
                                   keep="last"))
        tri = (tri.groupby(["event_id", "iso3", "displacement_type"], as_index=False)
                  .agg(figure=("figure", "sum"),
                       displacement_start_date=("displacement_start_date", "min"),
                       displacement_end_date=("displacement_end_date", "max"),
                       created_at=("created_at", "max")))

    return pd.concat([
        rec[["event_id", "iso3", "displacement_type", "figure",
             "displacement_start_date", "displacement_end_date"]],
        tri[["event_id", "iso3", "displacement_type", "figure",
             "displacement_start_date", "displacement_end_date"]]
        if not tri.empty else pd.DataFrame(columns=rec.columns),
    ], ignore_index=True)


def expand_to_daily(events: pd.DataFrame) -> pd.DataFrame:
    """Expand each event into daily rows, distributing figure uniformly."""
    events = events.dropna(
        subset=["displacement_start_date", "displacement_end_date"]
    ).copy()
    # Same-day events: end can fall before start due to upstream weirdness; clip.
    events["displacement_end_date"] = np.maximum(
        events["displacement_end_date"], events["displacement_start_date"]
    )
    events["n_days"] = (
        (events["displacement_end_date"] - events["displacement_start_date"]).dt.days + 1
    )
    events["displacement_daily"] = events["figure"] / events["n_days"]
    events["date"] = events.apply(
        lambda r: pd.date_range(r["displacement_start_date"],
                                r["displacement_end_date"], freq="D"),
        axis=1,
    )
    daily = events[["iso3", "displacement_type", "displacement_daily", "date"]].explode("date")
    daily["date"] = pd.to_datetime(daily["date"])
    return (daily.groupby(["iso3", "displacement_type", "date"], as_index=False)
                 ["displacement_daily"].sum())


def complete_date_range(
    daily: pd.DataFrame,
    min_date: pd.Timestamp,
    max_date: pd.Timestamp | None = None,
) -> pd.DataFrame:
    """Fill missing dates with 0 over [min_date, max_date] per group.

    Every (iso3, displacement_type) with at least one observed event gets
    full daily coverage in [min_date, max(today, group_max)], 0-filled.
    Mirrors the R reference (idmc::idmc_transform_daily) which uses
    `tidyr::complete(date = seq(min(date, min_date), max(date, max_date)))`.
    """
    if max_date is None:
        max_date = pd.Timestamp.today().normalize()
    pieces: list[pd.DataFrame] = []
    for (iso3, dtype), g in daily.groupby(["iso3", "displacement_type"], sort=False):
        end = max(max_date, g["date"].max())
        full = pd.DataFrame({"date": pd.date_range(min_date, end, freq="D")})
        full["iso3"] = iso3
        full["displacement_type"] = dtype
        merged = full.merge(g, on=["iso3", "displacement_type", "date"], how="left")
        merged["displacement_daily"] = merged["displacement_daily"].fillna(0)
        pieces.append(merged)
    return pd.concat(pieces, ignore_index=True).sort_values(
        ["iso3", "displacement_type", "date"]
    ).reset_index(drop=True)


def add_rolling_sums(df: pd.DataFrame) -> pd.DataFrame:
    """Add right-aligned 7d and 30d rolling sums per (iso3, displacement_type)."""
    df = df.copy()
    g = df.groupby(["iso3", "displacement_type"])["displacement_daily"]
    df["displacement_7d"] = g.transform(lambda s: s.rolling(7, min_periods=7).sum())
    df["displacement_30d"] = g.transform(lambda s: s.rolling(30, min_periods=30).sum())
    return df


def main() -> None:
    print("Fetching IDMC IDU…")
    events = fetch_idmc_events()
    print(f"  events: {len(events):,} rows, "
          f"{events['iso3'].nunique()} iso3 (pre-exclude)")
    missing = [c for c in REQUIRED_COLS if c not in events.columns]
    if missing:
        raise RuntimeError(f"IDMC API response missing columns: {missing}")
    events = events[~events["iso3"].isin(EXCLUDE_ISO3)]

    print("Selecting per-event_id role priority…")
    selected = select_per_event_role(events)
    print(f"  selected: {len(selected):,} event rows")

    print("Expanding to daily…")
    daily = expand_to_daily(selected)
    print(f"  daily aggregated: {len(daily):,} rows")

    print("Filling missing dates per group…")
    full = complete_date_range(daily, MIN_DATE)
    print(f"  completed: {len(full):,} rows, "
          f"{full['iso3'].nunique()} iso3 × {full['displacement_type'].nunique()} types")

    print("Computing 7d / 30d rolling sums…")
    out = add_rolling_sums(full)
    out = out[["iso3", "displacement_type", "date",
               "displacement_daily", "displacement_7d", "displacement_30d"]]

    print(f"Writing → {OUT_CONTAINER}/{OUT_BLOB} ({OUT_STAGE})…")
    stratus.upload_parquet_to_blob(
        out, OUT_BLOB, stage=OUT_STAGE, container_name=OUT_CONTAINER,
    )
    print(f"  done. {len(out):,} rows, "
          f"{out['date'].min().date()} → {out['date'].max().date()}")
    print(f"  refreshed_at: {date.today().isoformat()}")


if __name__ == "__main__":
    main()
