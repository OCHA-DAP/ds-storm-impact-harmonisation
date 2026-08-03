"""Cache GDACS API responses for ch06 (episodes) development.

Fetches all episode details, impact data, and selected timelines for
KALMAEGI-25 and saves raw JSON responses to book/_cache/06-gdacs-episodes/.

Usage:
    uv run python scripts/cache_gdacs_episodes.py
"""

import json
import sys
import time
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.datasets.gdacs import get_episode_detail, get_event_detail

EVENTID = 1001233  # KALMAEGI-25
CACHE_DIR = Path(__file__).resolve().parent.parent / "book" / "_cache" / "06-gdacs-episodes"
SELECTED_TIMELINE_EPS = [1, 5, 11, 16, 22]


def main():
    for subdir in ["episodes", "impacts", "timelines"]:
        (CACHE_DIR / subdir).mkdir(parents=True, exist_ok=True)

    # Event detail
    print("Fetching event detail...")
    detail = get_event_detail(EVENTID)
    (CACHE_DIR / "event_detail.json").write_text(json.dumps(detail))

    n_episodes = len(detail["properties"]["episodes"])
    print(f"Found {n_episodes} episodes\n")

    for i in range(1, n_episodes + 1):
        print(f"  Episode {i:2d}/{n_episodes}", end="", flush=True)

        ep = get_episode_detail(EVENTID, i)
        (CACHE_DIR / f"episodes/{i}.json").write_text(json.dumps(ep))

        resource = ep["properties"]["impacts"][0]["resource"]

        # buffer39 impact
        r = requests.get(resource["buffer39"])
        r.raise_for_status()
        (CACHE_DIR / f"impacts/{i}.json").write_text(json.dumps(r.json()))

        # timeline (selected episodes only)
        if i in SELECTED_TIMELINE_EPS:
            r = requests.get(resource["timeline"])
            r.raise_for_status()
            (CACHE_DIR / f"timelines/{i}.json").write_text(json.dumps(r.json()))
            print(" (+timeline)", end="")

        print(" OK")
        time.sleep(0.5)

    print(f"\nCached {n_episodes} episodes to {CACHE_DIR}")


if __name__ == "__main__":
    main()
