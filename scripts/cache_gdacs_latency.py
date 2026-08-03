"""Cache GDACS GTS bulletin latency data for ch03 (GDACS realtime).

For each NOAA TC event, fetches GTS (Global Telecommunication System) bulletin
receipt timestamps from the GDACS API and measures the lag between the
scheduled NHC advisory time (03/09/15/21 UTC) and when GDACS received the
WTNT4x forecast/advisory bulletin (the one carrying track, wind radii, and
intensity data used to compute exposure).

The GTS pubdate is the moment the bulletin arrived at GDACS via the WMO GTS
network — the tightest lower bound available on GDACS episode publication
latency. GDACS processing after receipt is typically sub-minute.

Output blob: ds-storm-impact-harmonisation/processed/gdacs_gts_latency.parquet

Usage:
    uv run python scripts/cache_gdacs_latency.py
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path

import ocha_stratus as stratus
import pandas as pd
import requests
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

load_dotenv()

BASE_URL = "https://www.gdacs.org/gdacsapi/api"
OUTPUT_BLOB = "ds-storm-impact-harmonisation/processed/gdacs_gts_latency.parquet"

# NHC (NOAA) scheduled advisory hours for Atlantic/E.Pac storms
NHC_ADVISORY_HOURS = {3, 9, 15, 21}

# Bulletin type prefixes that carry track + wind radii (forecast/advisory)
FORECAST_ADVISORY_TYPES = {"NT41", "NT42", "NT43", "NT44"}

MIN_YEAR = 2022
MAX_LAG_HRS = 3  # cap at 3h; longer gaps are missed/delayed transmissions


def get_events(from_year: int) -> list[dict]:
    r = requests.get(
        f"{BASE_URL}/events/geteventlist/SEARCH",
        params={"eventlist": "TC", "limit": 100, "fromdate": f"{from_year}-01-01"},
        timeout=30,
    )
    r.raise_for_status()
    return r.json().get("features", [])


def get_gts(eventid: int) -> list[dict]:
    r = requests.get(
        f"{BASE_URL}/Gts/getdatabykey",
        params={"eventtype": "TC", "eventid": eventid},
        timeout=30,
    )
    r.raise_for_status()
    return r.json().get("properties", {}).get("gts", [])


def nearest_advisory_time(issued: datetime, hours: set) -> datetime:
    """Snap an actual issuance time to the nearest nominal advisory hour.

    NHC issues advisories 5-15 minutes before the nominal hour (e.g. 14:45
    for the '15:00 advisory'). We find the nominal hour closest to the actual
    issuance time, searching ±3 hours.
    """
    candidates = []
    for delta_days in (-1, 0, 1):
        base = issued + timedelta(days=delta_days)
        for h in hours:
            candidates.append(base.replace(hour=h, minute=0, second=0, microsecond=0))
    return min(candidates, key=lambda c: abs((c - issued).total_seconds()))


def parse_bulletin_type(name: str) -> str:
    """Extract 4-char type code from WMO abbreviated header, e.g. NT43."""
    clean = name.replace("CCA", "").replace("CCB", "").strip()
    return clean[2:6] if len(clean) >= 6 else ""


def parse_issued_time(name: str, pub: datetime) -> datetime | None:
    """Parse actual issuance time from WMO bulletin identifier.

    Format: TTAAiiCCCC DDHHMM  e.g. WTNT43KNHC 211445 -> day 21, 14:45 UTC
    The last 6 chars of the name are DDHHMM.
    """
    clean = name.replace("CCA", "").replace("CCB", "").strip()
    ddhhmm = clean[-6:]
    try:
        dd, hh, mm = int(ddhhmm[:2]), int(ddhhmm[2:4]), int(ddhhmm[4:6])
        # Use pub year/month as context; handle day rollover
        issued = pub.replace(day=dd, hour=hh, minute=mm, second=0, microsecond=0)
        # If the parsed day is ahead of pub (month boundary edge case), step back
        if issued > pub + timedelta(hours=3):
            issued = issued.replace(month=pub.month - 1 or 12)
        return issued
    except (ValueError, IndexError):
        return None


def compute_lags(eventid: int, event_name: str) -> list[dict]:
    bulletins = get_gts(eventid)
    rows = []
    for g in bulletins:
        btype = parse_bulletin_type(g.get("name", ""))
        if btype not in FORECAST_ADVISORY_TYPES:
            continue
        pub_str = g.get("pubdate", "")
        if not pub_str:
            continue
        try:
            pub = datetime.fromisoformat(pub_str)
            if pub.year < MIN_YEAR:
                continue
            issued = parse_issued_time(g["name"], pub)
            if issued is None:
                continue
            advisory_dt = nearest_advisory_time(issued, NHC_ADVISORY_HOURS)
            # Lag from nominal advisory time to GDACS receipt
            lag_hrs = (pub - advisory_dt).total_seconds() / 3600
            if -1 <= lag_hrs <= MAX_LAG_HRS:
                rows.append({
                    "event_id": eventid,
                    "name": event_name,
                    "bulletin": g["name"],
                    "bulletin_type": btype,
                    "advisory_time": advisory_dt.isoformat(),
                    "issued": issued.isoformat(),
                    "pubdate": pub_str,
                    "lag_hrs": lag_hrs,
                })
        except (ValueError, TypeError):
            continue
    return rows


def main():
    print(f"Fetching NOAA TC events from {MIN_YEAR}+...")
    events = get_events(MIN_YEAR)
    # Filter to NOAA-sourced events only
    noaa_events = [
        f for f in events
        if f.get("properties", {}).get("source", "").upper() in ("NOAA", "NHC", "")
    ]
    print(f"Found {len(noaa_events)} events (filtering to NOAA source)")

    all_rows = []
    for f in noaa_events:
        p = f["properties"]
        eid = p["eventid"]
        name = p.get("eventname") or p.get("name", "")
        print(f"  {name} ({eid})...", end=" ", flush=True)
        try:
            rows = compute_lags(eid, name)
            all_rows.extend(rows)
            print(f"{len(rows)} bulletins")
        except Exception as e:
            print(f"FAIL — {e}")

    df = pd.DataFrame(all_rows)
    print(f"\nTotal rows: {len(df)}")
    if len(df):
        print(df["lag_hrs"].describe().round(3))

    print(f"\nSaving to blob: {OUTPUT_BLOB}")
    stratus.upload_parquet_to_blob(df, OUTPUT_BLOB)
    print("Done.")


if __name__ == "__main__":
    main()
