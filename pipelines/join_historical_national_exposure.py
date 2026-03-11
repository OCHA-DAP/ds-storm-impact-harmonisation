"""
Join ADAM + GDACS — Historical National Exposure

Loads national-level population exposure estimates from both the ADAM and GDACS
historical pipelines and joins them into a single dataset where each row is one
storm x one country. Also exports the combined dataset as JSON.

Sources:
    adam_historical_national_exposure.csv  — WFP ADAM wind exposure at 60/90/120 km/h
    gdacs_historical_national_exposure.csv — GDACS wind exposure at 34 kt / 64 kt

Join key: all shared metadata columns (everything except population exposure columns)
"""

import json
import sys
from pathlib import Path

import ocha_stratus as stratus
import pandas as pd
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from constants import PROJECT_PREFIX

load_dotenv()

ADAM_BLOB = f"{PROJECT_PREFIX}/adam_historical_national_exposure.csv"
GDACS_BLOB = f"{PROJECT_PREFIX}/gdacs_historical_national_exposure.csv"
OUTPUT_CSV = f"{PROJECT_PREFIX}/combined_historical_national_exposure.csv"
OUTPUT_JSON = Path(__file__).resolve().parents[1] / "assets" / "exposure_data.json"

JOIN_COLS = [
    "sid",
    "iso3",
    "from_date",
    "season",
    "name",
    "atcf_id",
    "number",
    "genesis_basin",
    "event_id",
    "episode_id",
    "country_name",
    "provisional",
    "storm_id",
]


def main():
    # -----------------------------------------------------------------------
    # 1. Load both datasets from blob storage
    # -----------------------------------------------------------------------
    print("Loading datasets from blob storage...")
    df_adam = stratus.load_csv_from_blob(ADAM_BLOB)
    df_gdacs = stratus.load_csv_from_blob(GDACS_BLOB)
    print(f"ADAM rows:  {len(df_adam)},  columns: {list(df_adam.columns)}")
    print(f"GDACS rows: {len(df_gdacs)}, columns: {list(df_gdacs.columns)}")

    # -----------------------------------------------------------------------
    # 2. Outer join on shared metadata columns
    # -----------------------------------------------------------------------
    df_merged = df_gdacs.merge(
        df_adam,
        on=JOIN_COLS,
        how="outer",
        suffixes=("_gdacs", "_adam"),
    )
    print(f"Merged rows: {len(df_merged)}")

    # -----------------------------------------------------------------------
    # 3. Consolidate duplicate columns (take non-null value from either source)
    # -----------------------------------------------------------------------
    df_merged["alert_level"] = df_merged["alert_level_gdacs"].fillna(
        df_merged["alert_level_adam"]
    )
    df_merged["storm_name"] = df_merged["storm_name_gdacs"].fillna(
        df_merged["storm_name_adam"]
    )
    df_merged["source"] = df_merged["source_gdacs"].fillna(df_merged["source_adam"])

    df_merged = df_merged.drop(
        columns=[
            "alert_level_gdacs",
            "alert_level_adam",
            "storm_name_gdacs",
            "storm_name_adam",
            "source_gdacs",
            "source_adam",
        ]
    )
    print(f"Combined columns, final shape: {df_merged.shape}")

    # -----------------------------------------------------------------------
    # 4. Save combined dataset to Azure blob storage
    # -----------------------------------------------------------------------
    print(f"\nSaving {len(df_merged)} rows to {OUTPUT_CSV}...")
    stratus.upload_csv_to_blob(df_merged, OUTPUT_CSV)

    # -----------------------------------------------------------------------
    # 5. Export to JSON for dashboard
    # -----------------------------------------------------------------------
    df_json = df_merged.replace({pd.NA: None, float("nan"): None})
    df_json = df_json.where(pd.notna(df_json), None)
    data_records = df_json.to_dict(orient="records")

    with open(OUTPUT_JSON, "w") as f:
        json.dump(data_records, f, indent=2)
    print(f"Exported {len(data_records)} records to {OUTPUT_JSON}")

    print("Done.")


if __name__ == "__main__":
    main()
