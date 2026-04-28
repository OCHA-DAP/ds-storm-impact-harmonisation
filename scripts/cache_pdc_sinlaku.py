"""Cache PDC + GDACS responses for chapter 08 (PDC evaluation).

Pins the chapter's worked example to a fixed snapshot of the Sinlaku
storm. PDC's rolling-window feed rotates events out within ~30 days of
dissipation, and GDACS metadata can shift over time, so the chapter
loads from cache instead of fetching live.

Cached outputs land in book/_cache/08-pdc-evaluation/:
- pdc_sinlaku.json           full PDC /hazards/{uuid} response
- gdacs_event_detail.json    full GDACS event detail
- gdacs_timeline.csv         per-advisory positions, wind, exposure
- gdacs_impact_buffer39.csv  cumulative country exposure, 39 kt buffer
- gdacs_impact_buffer74.csv  cumulative country exposure, 74 kt buffer

Usage:
    uv run python scripts/cache_pdc_sinlaku.py
"""

import json
import os
import sys
from pathlib import Path

import requests

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from src.datasets import gdacs

CACHE_DIR = REPO_ROOT / "book" / "_cache" / "08-pdc-evaluation"

PDC_BASE = "https://hazards-api.pdc.org"
PDC_SINLAKU_UUID = "e621323a-1d6e-4b3c-9413-e72800dab5d4"
GDACS_SINLAKU_EVENTID = 1001270  # SINLAKU-26


def main():
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Caching to {CACHE_DIR}")

    print("  PDC /hazards/{uuid}...", end=" ", flush=True)
    r = requests.get(
        f"{PDC_BASE}/hazards/{PDC_SINLAKU_UUID}",
        headers={"x-api-key": os.environ["PDC_API_KEY"]},
    )
    r.raise_for_status()
    (CACHE_DIR / "pdc_sinlaku.json").write_text(json.dumps(r.json()))
    print("OK")

    print("  GDACS event detail...", end=" ", flush=True)
    detail = gdacs.get_event_detail(GDACS_SINLAKU_EVENTID)
    (CACHE_DIR / "gdacs_event_detail.json").write_text(json.dumps(detail))
    print("OK")

    print("  GDACS timeline...", end=" ", flush=True)
    tl = gdacs.get_timeline(GDACS_SINLAKU_EVENTID)
    tl.to_csv(CACHE_DIR / "gdacs_timeline.csv", index=False)
    print(f"OK ({len(tl)} rows)")

    print("  GDACS impact (per-country)...", end=" ", flush=True)
    impact = gdacs.get_impact_by_country(GDACS_SINLAKU_EVENTID)
    for buf, df in impact.items():
        df.to_csv(CACHE_DIR / f"gdacs_impact_{buf}.csv", index=False)
    print(f"OK ({list(impact.keys())})")

    print("\nDone.")


if __name__ == "__main__":
    main()
