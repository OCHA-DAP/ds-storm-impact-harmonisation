"""Cache ADAM API episode latency data for ch03 (GDACS realtime).

Fetches all TC event episodes from the WFP ADAM OGC API in a single paginated
pass, computes the lag between each episode's synoptic time (to_date) and
ADAM's publication timestamp (updated_at), and saves the result to blob storage.

The `source` field on each episode record (e.g. NOAA, JTWC) is preserved so
the chapter can split the latency distribution by basin/provider.

Only includes episodes from 2022 onward with lags between 0 and 24 hours to
filter out retroactively-corrected historical records.

Output blob: ds-storm-impact-harmonisation/processed/adam_episode_latency.parquet

Usage:
    uv run python scripts/cache_adam_latency.py
"""

import base64
import json
import sys
from datetime import datetime
from pathlib import Path

import ocha_stratus as stratus
import pandas as pd
import requests
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

load_dotenv()

ADAM_BASE = "https://api.adam.geospatial.wfp.org/api"
COLLECTION = "adam.adam_ts_events"
PAGE_SIZE = 500
OUTPUT_BLOB = "ds-storm-impact-harmonisation/processed/adam_episode_latency.parquet"
MIN_YEAR = 2022
MAX_LAG_HRS = 24


def fetch_all_episodes() -> list[dict]:
    all_items = []
    offset = 0
    while True:
        resp = requests.get(
            f"{ADAM_BASE}/collections/{COLLECTION}/items",
            params={"limit": PAGE_SIZE, "offset": offset},
            timeout=30,
        )
        resp.raise_for_status()
        data = json.loads(base64.b64decode(resp.content))
        feats = data.get("features", [])
        for f in feats:
            all_items.append(f.get("properties", {}))
        print(f"  offset={offset}: {len(feats)} fetched (total: {len(all_items)})")
        if len(feats) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
    return all_items


def compute_lags(episodes: list[dict]) -> list[dict]:
    rows = []
    for p in episodes:
        to_d = p.get("to_date", "")
        upd = p.get("updated_at", "")
        if not (to_d and upd):
            continue
        try:
            to_dt = datetime.fromisoformat(to_d)
            upd_dt = datetime.fromisoformat(upd)
            lag_hrs = (upd_dt - to_dt).total_seconds() / 3600
            if to_dt.year >= MIN_YEAR and 0 <= lag_hrs <= MAX_LAG_HRS:
                rows.append(
                    {
                        "source": p.get("source", ""),
                        "name": p.get("name", ""),
                        "event_id": p.get("event_id"),
                        "episode_id": p.get("episode_id"),
                        "to_date": to_d,
                        "updated_at": upd,
                        "lag_hrs": lag_hrs,
                    }
                )
        except (ValueError, TypeError):
            continue
    return rows


def main():
    print("Fetching all ADAM episodes...")
    episodes = fetch_all_episodes()
    print(f"Total episodes fetched: {len(episodes)}")

    print("\nComputing lags...")
    rows = compute_lags(episodes)
    print(f"Usable episodes (>={MIN_YEAR}, lag 0-{MAX_LAG_HRS}h): {len(rows)}")

    df = pd.DataFrame(rows)
    print("\nLag summary by source:")
    print(df.groupby("source")["lag_hrs"].describe().round(2))

    print(f"\nSaving to blob: {OUTPUT_BLOB}")
    stratus.upload_parquet_to_blob(df, OUTPUT_BLOB)
    print("Done.")


if __name__ == "__main__":
    main()
