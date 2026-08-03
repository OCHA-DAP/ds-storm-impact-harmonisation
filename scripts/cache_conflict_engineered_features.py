"""Build engineered conflict features for ch02d and cache locally.

Loads ACLED + IDMC, computes the multi-window / z-score / momentum
feature panel for each of the 97 (post-Xuan-filter) conflict
allocations, and writes the result to:

    book/_cache/02d-engineered-features/conflict_engineered_features.parquet

ACLED loads ~1.4M events for the 34 conflict-allocation countries —
takes ~5 min on the production blob. Run once; the chapter reads from
the local cache. Re-run after refreshing ACLED / IDMC blobs.

Usage:
    uv run python scripts/cache_conflict_engineered_features.py
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.datasets.conflict import load_conflict_training_frame
from src.datasets.conflict_features import (
    build_alloc_features, load_acled_events, load_idmc_daily,
)

CACHE_DIR = Path(__file__).resolve().parent.parent / "book" / "_cache" / "02d-engineered-features"
OUT_PATH = CACHE_DIR / "conflict_engineered_features.parquet"


def main() -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    print("Loading conflict training frame…")
    df = load_conflict_training_frame()
    df = df.rename(columns={"ISO3": "iso3"})
    print(f"  {len(df)} allocations (all)")

    iso3_keep = set(df["iso3"].unique())

    print("Loading ACLED events (this is the slow part)…")
    acled = load_acled_events(iso3_keep)
    print(f"  {len(acled):,} ACLED events for {acled['iso3'].nunique()} iso3")

    print("Loading IDMC daily…")
    idmc = load_idmc_daily()
    print(f"  {len(idmc):,} daily IDMC rows")

    print("Building features for all allocations…")
    feats = build_alloc_features(df[["iso3", "alloc_date"]], acled, idmc)
    print(f"  feature shape: {feats.shape}")

    print(f"\nWriting → {OUT_PATH}")
    feats.to_parquet(OUT_PATH, index=False)
    print(f"  done. {len(feats)} allocation rows × {feats.shape[1]} columns.")


if __name__ == "__main__":
    main()
