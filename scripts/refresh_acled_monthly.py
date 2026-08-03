"""Aggregate ACLED conflict events to monthly fatalities, write to blob.

Output: container=`global`, stage=`dev`, blob=`acled/monthly_fatalities.parquet`.
Schema: iso3, year, month, fatalities.

Why this exists
---------------
The hdx-signals ACLED parquet (`output/acled_conflict/raw.parquet`) is
3M event-level rows. We don't want the predictor app to load that on
every startup. This script does the country-month aggregation once and
writes a small parquet (~30k rows) the app reads directly.

Same overwrite semantics as the other refresh scripts.
"""

from __future__ import annotations

from datetime import date

import ocha_stratus as stratus
import pandas as pd
import pycountry
from dotenv import load_dotenv

_ = load_dotenv()

ACLED_BLOB = "output/acled_conflict/raw.parquet"
OUT_BLOB = "acled/monthly_fatalities.parquet"
OUT_CONTAINER = "global"
OUT_STAGE = "dev"


def _iso_num_to_a3() -> dict[int, str]:
    """ISO 3166-1 numeric → alpha-3 lookup for all assigned codes."""
    return {int(c.numeric): c.alpha_3 for c in pycountry.countries
            if hasattr(c, "numeric") and c.numeric}


def main() -> None:
    print("Loading ACLED conflict events…")
    df = stratus.load_parquet_from_blob(
        ACLED_BLOB, stage="prod", container_name="hdx-signals"
    )
    print(f"  events: {len(df):,}")

    print("Mapping iso → iso3 and aggregating to (iso3, year, month)…")
    iso_map = _iso_num_to_a3()
    df["iso3"] = df["iso"].map(iso_map)
    n_unmapped = df["iso3"].isna().sum()
    if n_unmapped:
        print(f"  warning: {n_unmapped} events with unmapped iso codes "
              f"(dropping). distinct values: "
              f"{df.loc[df['iso3'].isna(), 'iso'].unique().tolist()[:10]}")
    df = df.dropna(subset=["iso3"])
    df["event_date"] = pd.to_datetime(df["event_date"])
    df["year"] = df["event_date"].dt.year
    df["month"] = df["event_date"].dt.month

    monthly = (
        df.groupby(["iso3", "year", "month"], as_index=False)["fatalities"]
        .sum()
    )
    monthly = monthly.sort_values(["iso3", "year", "month"]).reset_index(drop=True)

    print(f"  rows: {len(monthly):,}, "
          f"{monthly['iso3'].nunique()} iso3, "
          f"{monthly['year'].min()}–{monthly['year'].max()}")

    print(f"\nWriting → {OUT_CONTAINER}/{OUT_BLOB} ({OUT_STAGE})…")
    stratus.upload_parquet_to_blob(
        monthly, OUT_BLOB, stage=OUT_STAGE, container_name=OUT_CONTAINER,
    )
    print(f"  done. refreshed_at: {date.today().isoformat()}")


if __name__ == "__main__":
    main()
