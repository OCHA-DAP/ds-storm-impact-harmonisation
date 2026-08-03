"""Poll the PDC Hazards API for cyclones and archive the raw responses.

PDC has no archive endpoint. `/hazards` returns currently-active hazards plus
those that ended within roughly the last ~30 days, and the `?status=`,
`?startedAfter` and `?updatedAfter` filters are silently ignored (see
`docs/pdc_api.md`). The only way to build a historical PDC record is to poll
daily and accumulate. Every day we do not poll is a day of storm history that
cannot be recovered later.

Output (container=`projects`, stage=`dev`):

    ds-storm-impact-harmonisation/raw/pdc/cyclones/date=<YYYY-MM-DD>/
        _list.json            the /hazards?types=CYCLONE FeatureCollection
        <hazard_uuid>.json    one /hazards/{uuid} detail per cyclone

Design notes
------------
Stored **raw and unparsed.** The cyclone schema is still being discovered
(chapter 08 was written from a single manual-entry storm), so parsing at
capture time would bake today's assumptions into an archive we can never
re-fetch. `src/datasets/pdc.py` does the parsing, against these files.

**A poll that finds zero cyclones still writes `_list.json`.** Otherwise a
missing partition is ambiguous between "no cyclones were active" and "the poll
never ran", and that distinction is unrecoverable after the fact.

**Keyed on `hazard.uuid`**, taken from the list view's `properties.uuid`. The
detail object's *top-level* `uuid` is a state/version ID that changes as the
hazard updates, so it is not a stable filename.

**Detail fetches are individually fault-tolerant.** One failing hazard logs and
is skipped rather than losing the whole day's capture.

Re-running on the same day overwrites that day's partition, which is
intentional: the last poll of the day is the one we keep.

Run manually:  uv run python scripts/poll_pdc_cyclones.py
Dry run:       uv run python scripts/poll_pdc_cyclones.py --dry-run
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone

import ocha_stratus as stratus
import requests
from dotenv import load_dotenv

_ = load_dotenv()

PDC_BASE = "https://hazards-api.pdc.org"
BLOB_PREFIX = "ds-storm-impact-harmonisation/raw/pdc/cyclones"
CONTAINER = "projects"
TIMEOUT = 60


def _headers() -> dict[str, str]:
    key = os.environ.get("PDC_API_KEY")
    if not key:
        sys.exit("PDC_API_KEY is not set (expected in the environment or .env)")
    return {"x-api-key": key}


def fetch_cyclone_list() -> dict:
    """Return the /hazards?types=CYCLONE FeatureCollection."""
    r = requests.get(
        f"{PDC_BASE}/hazards",
        params={"types": "CYCLONE"},
        headers=_headers(),
        timeout=TIMEOUT,
    )
    r.raise_for_status()
    return r.json()


def fetch_detail(hazard_uuid: str) -> dict:
    """Return the full /hazards/{uuid} detail object."""
    r = requests.get(
        f"{PDC_BASE}/hazards/{hazard_uuid}",
        headers=_headers(),
        timeout=TIMEOUT,
    )
    r.raise_for_status()
    return r.json()


def _put(blob_name: str, payload: dict, *, stage: str, dry_run: bool) -> int:
    """Upload `payload` as JSON bytes; return the byte count."""
    data = json.dumps(payload).encode()
    if not dry_run:
        stratus.upload_blob_data(data, blob_name, stage=stage, container_name=CONTAINER)
    return len(data)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--stage", default="dev", choices=["dev", "prod"], help="blob stage"
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="fetch and report, but write nothing to blob",
    )
    args = ap.parse_args()

    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    partition = f"{BLOB_PREFIX}/date={day}"
    print(
        f"PDC cyclone poll  {day}  ->  {partition}"
        + (" [DRY RUN]" if args.dry_run else "")
    )

    listing = fetch_cyclone_list()
    features = listing.get("features", []) or []
    print(f"  /hazards?types=CYCLONE -> {len(features)} cyclone(s)")

    # Written even when empty: a zero-cyclone day is a real observation, and
    # without it a missing partition cannot be told apart from a failed run.
    n = _put(f"{partition}/_list.json", listing, stage=args.stage, dry_run=args.dry_run)
    print(f"  _list.json ({n:,} bytes)")

    ok, failed = 0, []
    for feat in features:
        props = feat.get("properties", {}) or {}
        uuid = props.get("uuid")
        name = props.get("name", "?")
        if not uuid:
            failed.append((name, "no uuid in list properties"))
            continue
        try:
            detail = fetch_detail(uuid)
        except requests.RequestException as exc:
            # Keep going: one bad hazard must not cost us the whole day.
            print(f"  FAILED {name}: {exc}")
            failed.append((name, str(exc)))
            continue
        n = _put(
            f"{partition}/{uuid}.json", detail, stage=args.stage, dry_run=args.dry_run
        )
        print(
            f"  {uuid}  {name!r}  "
            f"cat={props.get('category')} sev={props.get('severity')}  "
            f"({n:,} bytes)"
        )
        ok += 1

    print(f"\nCaptured {ok}/{len(features)} detail objects.")
    if failed:
        for name, why in failed:
            print(f"  unresolved: {name} — {why}")
        sys.exit(1)


if __name__ == "__main__":
    main()
