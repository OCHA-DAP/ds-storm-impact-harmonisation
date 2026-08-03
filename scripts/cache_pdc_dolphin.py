"""Cache the PDC + GDACS Typhoon Dolphin snapshot for chapter 11.

Chapter 11 re-runs the chapter 08 comparison against an *automatically
ingested* cyclone — the case chapter 08 named as its re-evaluation trigger and
could not obtain at the time. Typhoon Dolphin (JTWC `WP122026`, GDACS event
`1001297`) was live when this was captured, so the chapter is pinned to a
snapshot rather than refetched: PDC drops the storm from its rolling feed ~30
days after it ends, and GDACS advisories keep accruing.

Cached outputs land in book/_cache/11-pdc-2026-season/ (tracked in git, small):

- pdc_dolphin.json        PDC /hazards/{uuid} detail, raw
- pdc_genevieve.json      second auto-ingested cyclone (NHC-sourced), raw
- pdc_bavi.json           manual RESPONSE entry, for the contrast
- pdc_list.json           the /hazards?types=CYCLONE list at capture time
- gdacs_timeline.csv      GDACS per-advisory track + radii + pop39/pop74
- gdacs_impact_*.csv      GDACS cumulative country exposure per buffer
- gdacs_event.json        GDACS event detail

Note on `alert_levels`: `gdacs.get_active_cyclones()` omits the `alertlevel`
parameter by default, and GDACS then returns only orange+red — which excludes
Dolphin, a Green event. Passing all three levels explicitly is required to see
it at all. See chapter 11.

Usage:
    uv run python scripts/cache_pdc_dolphin.py
"""

import json
import os
import sys
from pathlib import Path

import requests

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from src.datasets import gdacs  # noqa: E402

CACHE_DIR = REPO_ROOT / "book" / "_cache" / "11-pdc-2026-season"

PDC_BASE = "https://hazards-api.pdc.org"
PDC_HAZARDS = {
    "pdc_dolphin.json": "d0345bd1-3be3-41ab-9f32-bcba63e39f54",
    "pdc_genevieve.json": "05dc1097-0b5f-46fa-b687-f9dfa77d1fe1",
    "pdc_bavi.json": "290c2172-cfa8-4035-a90d-d7bbca300a10",
}
GDACS_DOLPHIN_EVENTID = 1001297  # DOLPHIN-26


def _pdc(path: str, **params) -> dict:
    r = requests.get(
        f"{PDC_BASE}{path}",
        headers={"x-api-key": os.environ["PDC_API_KEY"]},
        params=params or None,
        timeout=60,
    )
    r.raise_for_status()
    return r.json()


def main() -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Caching to {CACHE_DIR}")

    print("  PDC cyclone list...", end=" ", flush=True)
    listing = _pdc("/hazards", types="CYCLONE")
    (CACHE_DIR / "pdc_list.json").write_text(json.dumps(listing))
    print(f"OK ({len(listing.get('features', []))} cyclones)")

    for fname, uuid in PDC_HAZARDS.items():
        print(f"  PDC {fname}...", end=" ", flush=True)
        (CACHE_DIR / fname).write_text(json.dumps(_pdc(f"/hazards/{uuid}")))
        print("OK")

    print("  GDACS event detail...", end=" ", flush=True)
    detail = gdacs.get_event_detail(GDACS_DOLPHIN_EVENTID)
    (CACHE_DIR / "gdacs_event.json").write_text(json.dumps(detail))
    print("OK")

    print("  GDACS timeline...", end=" ", flush=True)
    tl = gdacs.get_timeline(GDACS_DOLPHIN_EVENTID)
    tl.to_csv(CACHE_DIR / "gdacs_timeline.csv", index=False)
    print(f"OK ({len(tl)} rows)")

    print("  GDACS impact (per-country)...", end=" ", flush=True)
    impact = gdacs.get_impact_by_country(GDACS_DOLPHIN_EVENTID)
    for buf, df in impact.items():
        df.to_csv(CACHE_DIR / f"gdacs_impact_{buf}.csv", index=False)
    print(f"OK ({list(impact.keys())})")

    print("\nDone.")


if __name__ == "__main__":
    main()
