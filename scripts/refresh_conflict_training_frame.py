"""Build the conflict training frame from live sources and write to blob.

Joins Finn's xlsx (target + 3RM features) with live ACLED + live IDMC
(refreshed by `scripts/refresh_idmc_displacement.py`) and Xuan's review
(Refugee flagging). Output is one parquet on blob, ready for the book
chapter and the predictor app to fit on.

Run manually: `uv run python scripts/refresh_conflict_training_frame.py`.
Re-running clobbers the blob (same overwrite semantics as
refresh_idmc_displacement.py).
"""

from __future__ import annotations

import sys
from pathlib import Path

import ocha_stratus as stratus
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.datasets.conflict import (  # noqa: E402
    CONFLICT_TRAINING_BLOB_PATH,
    build_conflict_training_frame,
)

_ = load_dotenv()


def main() -> None:
    print("Building conflict training frame…")
    df = build_conflict_training_frame()
    n_total = len(df)
    n_excluded = int(df["xuan_refugee_excluded"].sum())
    print(f"  rows: {n_total} ({n_total - n_excluded} after Xuan correction)")
    print(f"  date range: {df['alloc_date'].min().date()} → "
          f"{df['alloc_date'].max().date()}")
    print(f"  countries: {df['ISO3'].nunique()}")

    print(f"\nWriting → {CONFLICT_TRAINING_BLOB_PATH} (dev)…")
    stratus.upload_parquet_to_blob(df, CONFLICT_TRAINING_BLOB_PATH, stage="dev")
    print("  done.")


if __name__ == "__main__":
    main()
