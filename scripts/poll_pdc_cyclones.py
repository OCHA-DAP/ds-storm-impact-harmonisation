"""Poll the PDC Hazards API for cyclones and archive the raw responses.

PDC has no archive endpoint. `/hazards` returns currently-active hazards plus
those that ended within roughly the last ~30 days, and the `?status=`,
`?startedAfter` and `?updatedAfter` filters are silently ignored (see
`docs/pdc_api.md`). The only way to build a historical PDC record is to poll
and accumulate. Every polling cycle we miss is storm history that cannot be
recovered later.

A cyclone detail object is **forecast-only**: it carries the current
advisory's positions from now forward, and no past track. Typhoon Dolphin at
advisory 31 returned nine positions, the earliest being the present synoptic
hour — advisories 1-30 appear nowhere in the payload. GDACS, by contrast,
serves a storm's full advisory history in a single call. So PDC track history
exists *only* in the succession of polls we take, which is why this runs on
the 6-hourly synoptic cycle rather than daily: a daily poll would silently
discard roughly three of every four advisories.

Output (container=`projects`, stage=`dev`), a poll log plus a version store:

    ds-storm-impact-harmonisation/raw/pdc/cyclones/
        polls/<poll_ts>/_list.json          the FeatureCollection, per poll
        hazards/<hazard_uuid>/<updatedAt>.json   one file per hazard version

Design notes
------------
Stored **raw and unparsed.** The cyclone schema is still being discovered
(chapter 08 was written from a single manual-entry storm), so parsing at
capture time would bake today's assumptions into an archive we can never
re-fetch. `src/datasets/pdc.py` does the parsing, against these files.

**Details are keyed on `(hazard.uuid, updatedAt)`** — the dedupe key
`docs/pdc_api.md` identified, given the API offers no incremental-poll
primitive. An unchanged hazard rewrites an identical path, so extra polls cost
nothing and are safe to run at any cadence. `hazard.uuid` comes from the list
view's `properties.uuid`; the detail object's *top-level* `uuid` is a
state/version ID and is not stable.

**Every poll writes `_list.json`, including empty ones.** Otherwise a gap in
the poll log is ambiguous between "no cyclones were active" and "the run
failed", and that distinction is unrecoverable after the fact.

**Detail fetches are individually fault-tolerant.** One failing hazard logs
and is skipped rather than losing the whole poll.

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

    poll_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    print(f"PDC cyclone poll  {poll_ts}" + (" [DRY RUN]" if args.dry_run else ""))

    listing = fetch_cyclone_list()
    features = listing.get("features", []) or []
    print(f"  /hazards?types=CYCLONE -> {len(features)} cyclone(s)")

    # Written even when empty: a zero-cyclone poll is a real observation, and
    # without it a gap in the poll log cannot be told apart from a failed run.
    n = _put(
        f"{BLOB_PREFIX}/polls/{poll_ts}/_list.json",
        listing,
        stage=args.stage,
        dry_run=args.dry_run,
    )
    print(f"  polls/{poll_ts}/_list.json ({n:,} bytes)")

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
            # Keep going: one bad hazard must not cost us the whole poll.
            print(f"  FAILED {name}: {exc}")
            failed.append((name, str(exc)))
            continue
        # updatedAt is the version key. Re-polling an unchanged hazard
        # rewrites this exact path, so repeat polls are idempotent.
        #
        # Taken from the *detail* object, not the list view: the two disagree
        # by 5-22s (the list timestamp appears to be an index-materialisation
        # time). Keying on the detail keeps the filename and the record it
        # contains consistent. Fall back to the list value if absent.
        # Avro wrapping is inconsistent even within `hazard`: updatedAt is a
        # bare int while its sibling endedAt is {"long": ...}. Unwrap
        # defensively so a future change cannot put a dict in a filename.
        updated = (detail.get("hazard") or {}).get("updatedAt")
        if isinstance(updated, dict):
            updated = next(iter(updated.values()), None)
        updated = updated or props.get("updatedAt", "unknown")
        n = _put(
            f"{BLOB_PREFIX}/hazards/{uuid}/{updated}.json",
            detail,
            stage=args.stage,
            dry_run=args.dry_run,
        )
        print(
            f"  {uuid}/{updated}  {name!r}  "
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
