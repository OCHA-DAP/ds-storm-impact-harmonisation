"""Daily GDACS monitor email.

Fetches currently-active tropical cyclones from GDACS (any basin, no
source filter), compares per-country population exposure to the OCHA
historical baseline, renders an HTML email with one strip chart per
affected country, and submits it to Listmonk as a campaign targeting
the configured test list.

Three modes:

    uv run python scripts/daily_gdacs_monitor_email.py
        Full pipeline: render + send. Used by GHA cron.

    uv run python scripts/daily_gdacs_monitor_email.py --dry-run
        Render only. Writes HTML to artefacts/daily_email_previews/.
        No Listmonk call at all. Safe to run anywhere.

    uv run python scripts/daily_gdacs_monitor_email.py --inspect
        Local-only sanity-check before going prod. Creates the draft
        campaign on Listmonk, prints the resolved recipients, opens
        the server-rendered preview (template applied) in your browser,
        and exits without sending. Requires a local browser, so do not
        use this in GHA.
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from ocha_relay.listmonk import ListmonkClient  # noqa: E402

from src.datasets.gdacs import get_active_cyclones, get_impact_by_country  # noqa: E402
from src.gdacs_monitor_email import (  # noqa: E402
    build_email_html,
    build_stub_html,
    load_ocha_historical,
)

load_dotenv()

TEST_LIST_ID = 25
SUBJECT_PREFIX = "[test]"  # triggers the test-variant template on the
                          # OCHA Listmonk instance. Drop when going prod.
DRY_RUN_DIR = Path(__file__).parent.parent / "artefacts" / "daily_email_previews"


# ---------------------------------------------------------------------------
# Fetch live exposure for the active storms
# ---------------------------------------------------------------------------


def fetch_active_exposure() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return (active_storms_df, exposure_long_df).

    exposure_long_df has one row per (eventid, iso3, buffer) tuple.
    """
    events = get_active_cyclones()
    if events.empty:
        return pd.DataFrame(), pd.DataFrame()
    active = events[events["is_current"]].reset_index(drop=True)
    if active.empty:
        return pd.DataFrame(), pd.DataFrame()

    rows = []
    for _, ev in active.iterrows():
        eid = ev["eventid"]
        try:
            impact = get_impact_by_country(eid, aggregate=True)
        except Exception as e:
            print(f"  WARN fetch failed for eventid={eid}: {e}", flush=True)
            continue
        for buf_name, df_buf in impact.items():
            if df_buf is None or df_buf.empty:
                continue
            for _, r in df_buf.iterrows():
                rows.append({
                    "eventid": eid,
                    "name": ev["name"],
                    "alert_level": ev["alert_level"],
                    "from_date": ev["from_date"],
                    "buffer": buf_name,
                    "iso3": r["iso3"],
                    "country": r["country"],
                    "pop_affected": int(r["pop_affected"]),
                })
    return active, pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="Write HTML to disk; no Listmonk call at all.",
    )
    mode.add_argument(
        "--inspect",
        action="store_true",
        help=(
            "Create the draft campaign on Listmonk, print resolved "
            "recipients, open the server-rendered preview in a browser,"
            " and exit without sending."
        ),
    )
    parser.add_argument(
        "--list-id",
        type=int,
        default=TEST_LIST_ID,
        help=f"Listmonk list ID to target (default: {TEST_LIST_ID}).",
    )
    args = parser.parse_args()

    now = datetime.now(tz=timezone.utc)
    print(f"GDACS monitor run at {now.isoformat()}", flush=True)

    print("Step 1: fetch active storms + per-country exposure...", flush=True)
    active, exposure = fetch_active_exposure()
    print(f"  active storms: {len(active)}; exposure rows: {len(exposure)}", flush=True)

    if active.empty:
        print("Step 2: no active storms — stub email.", flush=True)
        html = build_stub_html(now)
        subject = (
            f"{SUBJECT_PREFIX} GDACS Monitor {now:%Y-%m-%d %H%M}Z:"
            " no active storms"
        )
    else:
        print("Step 2: load OCHA historical baseline...", flush=True)
        historical = load_ocha_historical()
        print(
            f"  historical rows: {len(historical):,}"
            f" across {historical['sid'].nunique()} sids,"
            f" {historical['iso3'].nunique()} iso3",
            flush=True,
        )
        print("Step 3: render email HTML with inline strip plots...", flush=True)
        html = build_email_html(active, exposure, historical, now)
        names = ", ".join(active["name"].tolist())
        subject = (
            f"{SUBJECT_PREFIX} GDACS Monitor {now:%Y-%m-%d %H%M}Z: {names}"
        )

    if args.dry_run:
        DRY_RUN_DIR.mkdir(parents=True, exist_ok=True)
        out = DRY_RUN_DIR / f"email_{now:%Y%m%dT%H%M%SZ}.html"
        out.write_text(html)
        print(f"\nDry-run wrote {out} ({len(html):,} bytes)", flush=True)
        print(f"Subject would be: {subject}", flush=True)
        return

    client = ListmonkClient.from_env()
    campaign_name = f"gdacs-monitor-{now:%Y%m%dT%H%MZ}"

    if args.inspect:
        print("Step 4 (inspect): create draft campaign...", flush=True)
        cid = client.create_campaign(
            name=campaign_name,
            subject=subject,
            body=html,
            list_ids=[args.list_id],
        )
        print(f"  draft created: id={cid} name={campaign_name!r}\n", flush=True)

        manifest = client.build_send_manifest(cid)
        print(manifest.format())

        print("\nopening server-rendered preview in default browser...", flush=True)
        preview_path = client.preview_in_browser(cid)
        print(f"  preview written to {preview_path}\n", flush=True)
        print(
            f"draft campaign {cid} left on Listmonk (NOT sent). "
            "Delete via the Listmonk UI if not needed.",
            flush=True,
        )
        return

    print("Step 4: submit and send via Listmonk...", flush=True)
    cid = client.create_campaign(
        name=campaign_name,
        subject=subject,
        body=html,
        list_ids=[args.list_id],
    )
    print(f"  created campaign id={cid} name={campaign_name!r}", flush=True)
    client.send_campaign(cid, skip_confirmation=True)
    print(f"  sent campaign id={cid}", flush=True)


if __name__ == "__main__":
    main()
