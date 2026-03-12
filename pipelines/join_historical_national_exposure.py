"""
Join ADAM + GDACS + CHD — Historical National Exposure

Loads national-level population exposure estimates from the ADAM, GDACS, and CHD
historical pipelines and joins them into a single dataset where each row is one
storm x one country. Also exports the combined dataset as JSON.

Sources:
    adam_historical_national_exposure.csv  — WFP ADAM wind exposure at 34/50/64 kt
    gdacs_historical_national_exposure.csv — GDACS wind exposure at 34 kt / 64 kt
    adm0_ibtracs_exp_all.parquet           — CHD wind exposure at 34/50/64 kt

Cleaning applied to ADAM before joining:
    Exposure values must be cumulative (≥ threshold). ADAM stores per-band values,
    so pop_34kt is cumsum'd from the highest threshold down, treating
    missing higher-threshold columns as 0 to preserve partial records:
        pop_34kt = pop_34kt + (pop_50kt or 0) + (pop_64kt or 0)
        pop_50kt = pop_50kt + (pop_64kt or 0)  [only if pop_50kt is non-null]

Join key: all shared metadata columns (everything except population exposure columns)
CHD join key: sid + iso3 only (CHD does not carry the full metadata set)
"""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import ocha_stratus as stratus
import pandas as pd
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from constants import PROJECT_PREFIX

load_dotenv()

ADAM_BLOB = f"{PROJECT_PREFIX}/processed/adam_historical_national_exposure.csv"
GDACS_BLOB = f"{PROJECT_PREFIX}/processed/gdacs_historical_national_exposure.csv"
CHD_BLOB = f"{PROJECT_PREFIX}/processed/adm0_ibtracs_exp_all.parquet"
OUTPUT_CSV = f"{PROJECT_PREFIX}/processed/combined_historical_national_exposure.csv"
OUTPUT_JSON = Path(__file__).resolve().parents[1] / "assets" / "exposure_data.json"


def make_adam_cumulative(df: pd.DataFrame) -> pd.DataFrame:
    """Convert ADAM per-band exposure values to cumulative (≥ threshold).

    ADAM stores population counts per wind-speed band (e.g. pop_34kt is only
    the count *between* 34 kt and 50 kt). Exposure values should be cumulative,
    meaning pop_34kt counts everyone exposed to *at least* 34 kt. This function
    applies a cumsum from the highest threshold downward, treating missing
    higher-threshold values as 0 so that partial records are preserved.
    """
    df = df.copy()
    v34 = df["pop_34kt"]
    v50 = df["pop_50kt"].fillna(0)
    v64 = df["pop_64kt"].fillna(0)
    df["pop_34kt"] = v34.where(v34.isna(), v34 + v50 + v64)
    df["pop_50kt"] = df["pop_50kt"].where(
        df["pop_50kt"].isna(), df["pop_50kt"].fillna(0) + v64
    )
    return df


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


def pivot_chd(df: pd.DataFrame) -> pd.DataFrame:
    """Pivot CHD data from long format to wide format with source-labelled columns.

    Input has one row per storm x country x wind-speed threshold (34/50/64 kt).
    Output has one row per storm x country with columns pop_34kt_chd, pop_50kt_chd,
    pop_64kt_chd.
    """
    df_wide = (
        df.rename(columns={"ADM0_A3": "iso3"})
        .pivot_table(index=["sid", "iso3"], columns="speed", values="pop_exposed")
        .reset_index()
    )
    df_wide.columns.name = None
    df_wide = df_wide.rename(
        columns={34: "pop_34kt_chd", 50: "pop_50kt_chd", 64: "pop_64kt_chd"}
    )
    return df_wide


def main():
    # -----------------------------------------------------------------------
    # 1. Load all datasets from blob storage
    # -----------------------------------------------------------------------
    print("Loading datasets from blob storage...")
    df_adam = stratus.load_csv_from_blob(ADAM_BLOB)
    df_gdacs = stratus.load_csv_from_blob(GDACS_BLOB)
    df_chd = stratus.load_parquet_from_blob(CHD_BLOB)
    print(f"ADAM rows:  {len(df_adam)},  columns: {list(df_adam.columns)}")
    print(f"GDACS rows: {len(df_gdacs)}, columns: {list(df_gdacs.columns)}")
    print(f"CHD rows:   {len(df_chd)},  columns: {list(df_chd.columns)}")

    # -----------------------------------------------------------------------
    # 2. Make ADAM exposure values cumulative
    # -----------------------------------------------------------------------
    df_adam = make_adam_cumulative(df_adam)
    print("Applied cumulative fix to ADAM exposure columns.")

    # -----------------------------------------------------------------------
    # 3. Outer join on shared metadata columns
    # -----------------------------------------------------------------------
    df_merged = df_gdacs.merge(
        df_adam,
        on=JOIN_COLS,
        how="outer",
        suffixes=("_gdacs", "_adam"),
    )
    print(f"Merged rows: {len(df_merged)}")

    # -----------------------------------------------------------------------
    # 4. Consolidate duplicate columns (take non-null value from either source)
    # -----------------------------------------------------------------------
    df_merged["alert_level"] = df_merged["alert_level_gdacs"].fillna(
        df_merged["alert_level_adam"]
    )
    df_merged["storm_name"] = df_merged["storm_name_gdacs"].fillna(
        df_merged["storm_name_adam"]
    )
    df_merged["source"] = df_merged["source_gdacs"].fillna(df_merged["source_adam"])

    drop_cols = [
        "alert_level_gdacs",
        "alert_level_adam",
        "storm_name_gdacs",
        "storm_name_adam",
        "source_gdacs",
        "source_adam",
    ]
    # Drop index columns if present (artefacts from earlier pipeline versions)
    drop_cols += [c for c in ("index_gdacs", "index_adam") if c in df_merged.columns]
    df_merged = df_merged.drop(columns=drop_cols)

    # pop_50kt comes only from ADAM (no GDACS equivalent at that threshold);
    # add _adam suffix explicitly so all pop columns are consistently source-labelled.
    df_merged = df_merged.rename(columns={"pop_50kt": "pop_50kt_adam"})

    print(f"Combined columns after ADAM+GDACS merge: {df_merged.shape}")

    # -----------------------------------------------------------------------
    # 5. Join CHD data
    # -----------------------------------------------------------------------
    df_chd_wide = pivot_chd(df_chd)
    print(f"CHD wide rows: {len(df_chd_wide)}, columns: {list(df_chd_wide.columns)}")

    df_merged = df_merged.merge(df_chd_wide, on=["sid", "iso3"], how="outer")
    print(f"Combined columns after CHD join, final shape: {df_merged.shape}")

    # -----------------------------------------------------------------------
    # 6. Save combined dataset to Azure blob storage
    # -----------------------------------------------------------------------
    print(f"\nSaving {len(df_merged)} rows to {OUTPUT_CSV}...")
    stratus.upload_csv_to_blob(df_merged, OUTPUT_CSV)

    # -----------------------------------------------------------------------
    # 7. Export to JSON for dashboard (only storms with a GDACS event_id)
    # -----------------------------------------------------------------------
    df_json = df_merged[df_merged["event_id"].notna()]
    print(f"Dropped {len(df_merged) - len(df_json)} rows without GDACS event_id")
    df_json = df_json.replace({pd.NA: None, float("nan"): None})
    df_json = df_json.where(pd.notna(df_json), None)
    data_records = df_json.to_dict(orient="records")

    output = {
        "last_updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "data": data_records,
    }
    with open(OUTPUT_JSON, "w") as f:
        json.dump(output, f, indent=2)
    print(f"Exported {len(data_records)} records to {OUTPUT_JSON}")

    print("Done.")


if __name__ == "__main__":
    main()
