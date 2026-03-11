"""
GDACS — Historical National Exposure

Retrieves country-level population exposure for all tropical cyclones within a
date range, using the GDACS search endpoint for full historical coverage. Saves
national level exposure estimates to blob storage for all available storms.

API path:
    geteventlist/search  (paginated, filterable by date / source)
      -> getepisodedata  (latest episode per event)
        -> getimpact -> datums[alias='country'] -> ISO_3DIGIT, CNTRY_NAME, POP_AFFECTED
"""

import sys
from pathlib import Path

import ocha_stratus as stratus
import pandas as pd
import requests
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from constants import PROJECT_PREFIX

load_dotenv()

GDACS_BASE = "https://www.gdacs.org/gdacsapi/api"
FROM_DATE = "2010-01-01"
TO_DATE = "2026-12-31"
SOURCE = "NOAA"  # 'NOAA' = Atlantic/E.Pacific | 'JTWC' = W.Pacific/Indian Ocean
OUTPUT_CSV = f"{PROJECT_PREFIX}/processed/gdacs_historical_national_exposure.csv"

# Wind speed (kt) implied by each buffer key
BUFFER_KT = {
    "buffer39": 34,
    "buffer74": 64,
}


# ---------------------------------------------------------------------------
# 1. Fetch all TC events in the date range
# ---------------------------------------------------------------------------


def fetch_all_events():
    all_events = []
    alert_levels = ["Red", "Orange", "Green"]

    for alert_level in alert_levels:
        print(f"\nSearching for {alert_level} alert storms...")
        page = 1

        while True:
            params = {
                "eventlist": "TC",
                "fromDate": FROM_DATE,
                "toDate": TO_DATE,
                "alertlevel": alert_level,
                "pageSize": 100,
                "pageNumber": page,
            }
            resp = requests.get(
                f"{GDACS_BASE}/events/geteventlist/search",
                params=params,
                timeout=30,
            )
            if not resp.text.strip():
                break
            features = resp.json().get("features", [])
            if not features:
                break

            for f in features:
                p = f["properties"]
                if SOURCE and p.get("source") != SOURCE:
                    continue
                event_id = str(p["eventid"])
                if any(e["event_id"] == event_id for e in all_events):
                    continue
                all_events.append(
                    {
                        "event_id": event_id,
                        "storm_name": p.get("name", ""),
                        "source": p.get("source", ""),
                        "from_date": p.get("fromdate", ""),
                        "to_date": p.get("todate", ""),
                        "alert_level": p.get("alertlevel", ""),
                    }
                )

            print(f"  Page {page}: {len(features)} events fetched")
            page += 1

    df_events = pd.DataFrame(all_events)
    print(f"\nTotal TCs found ({SOURCE or 'all basins'}): {len(df_events)}")
    return df_events


# ---------------------------------------------------------------------------
# 2. Fetch exposure data per event
# ---------------------------------------------------------------------------


def fetch_national_exposure(event_id):
    """Return country-row dicts for the latest episode. Empty list if no data."""
    try:
        props = requests.get(
            f"{GDACS_BASE}/events/getepisodedata",
            params={"eventtype": "TC", "eventid": event_id},
            timeout=30,
        ).json()["properties"]
    except Exception as e:
        print(f"  [{event_id}] failed: {e}")
        return []

    last_ep_url = props.get("episodes", [{}])[-1].get("details", "")
    episode_id = (
        last_ep_url.split("episodeid=")[-1].split("&")[0] if last_ep_url else "?"
    )
    buffers = {
        k: v
        for k, v in props.get("impacts", [{}])[0].get("resource", {}).items()
        if k.startswith("buffer")
    }

    country_data = {}
    for buf, url in buffers.items():
        col = f"pop_{BUFFER_KT.get(buf, buf)}kt"
        try:
            datums = requests.get(url, timeout=30).json().get("datums", [])
        except Exception:
            continue
        country_datum = next((d for d in datums if d["alias"] == "country"), None)
        if not country_datum:
            continue
        for row in country_datum.get("datum", []):
            sc = {s["name"]: s["value"] for s in row["scalars"]["scalar"]}
            iso3 = sc.get("ISO_3DIGIT")
            if not iso3:
                continue
            pop_affected = sc.get("POP_AFFECTED")
            if pop_affected is not None and pop_affected != "":
                try:
                    pop_value = int(float(pop_affected))
                except (ValueError, TypeError):
                    pop_value = None
            else:
                pop_value = None

            country_data.setdefault(
                iso3,
                {
                    "event_id": event_id,
                    "episode_id": episode_id,
                    "iso3": iso3,
                    "country_name": sc.get("CNTRY_NAME"),
                },
            )[col] = pop_value

    return list(country_data.values())


def build_exposure_df(df_events):
    all_rows = []
    for _, ev in df_events.iterrows():
        eid = ev["event_id"]
        name = ev["storm_name"]
        print(f"Fetching {name} ({eid}) …", end=" ")
        rows = fetch_national_exposure(eid)
        for r in rows:
            r["storm_name"] = name
            r["source"] = ev["source"]
            r["from_date"] = ev["from_date"]
            r["alert_level"] = ev["alert_level"]
        all_rows.extend(rows)
        print(f"{len(rows)} countries")

    df = (
        pd.DataFrame(all_rows)
        .sort_values(["from_date", "storm_name"], ascending=False)
        .reset_index(drop=True)
    )
    leading = [
        "storm_name",
        "event_id",
        "episode_id",
        "source",
        "from_date",
        "alert_level",
        "iso3",
        "country_name",
    ]
    pop_cols = sorted([c for c in df.columns if c.startswith("pop_")])
    return df[leading + pop_cols]


# ---------------------------------------------------------------------------
# 3. Clean results and join with IBTrACS
# ---------------------------------------------------------------------------


def parse_name(input_name):
    season_str = input_name.split("-")[-1]
    season = int("20" + season_str)
    storm_name = input_name.split("-")[0].split(" ")[-1]
    return season, storm_name


def join_ibtracs(df):
    engine = stratus.get_engine("prod")
    with engine.connect() as conn:
        df_ibtracs = pd.read_sql("SELECT * FROM storms.ibtracs_storms", conn)

    df_cleaned = df.copy()
    df_cleaned[["season", "name"]] = df_cleaned["storm_name"].apply(
        lambda x: pd.Series(parse_name(x))
    )

    df_storms = (
        df_cleaned.drop_duplicates(subset=["storm_name"])
        .drop(columns=["iso3", "country_name", "pop_34kt", "pop_64kt"])
        .reset_index()
    )
    print(f"Dataset has {len(df_storms)} unique storms.")

    df_merged = df_storms.merge(df_ibtracs, on=["season", "name"], how="left")
    df_merged = df_merged.sort_values("provisional").drop_duplicates(
        subset="storm_name", keep="first"
    )
    assert len(df_merged) == len(df_storms)

    df_final = df_cleaned.merge(df_merged)
    assert len(df_final) == len(df_cleaned)

    return df_final


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    print("=== Fetching all TC events ===")
    df_events = fetch_all_events()

    print("\n=== Fetching population exposure per event ===")
    df = build_exposure_df(df_events)

    print("\n=== Joining with IBTrACS ===")
    df_final = join_ibtracs(df)

    print(f"\n=== Saving {len(df_final)} rows to {OUTPUT_CSV} ===")
    stratus.upload_csv_to_blob(df_final, OUTPUT_CSV)
    print("Done.")


if __name__ == "__main__":
    main()
