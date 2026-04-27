"""
Rebuild GDACS historical national-level exposure CSV, with longer timeouts
and retries so major storms (e.g., HELENE, IAN, FIONA buffer39) don't drop
out of the output.

Scope: source="NOAA" (North Atlantic + Eastern Pacific), all alert levels,
from 2022-01-01 to today.

Outputs:
  ds-storm-impact-harmonisation/raw/gdacs/gdacs_historical_adm0_exposure_v2.csv
  ds-storm-impact-harmonisation/raw/gdacs/gdacs_historical_adm1_exposure_v2.csv
  ds-storm-impact-harmonisation/raw/gdacs/gdacs_rebuild_fetch_log.csv

Usage:
    uv run python scripts/rebuild_gdacs_historical_exposure.py [--dry-run] [--from-date YYYY-MM-DD]

--dry-run skips blob upload; CSVs are saved locally to artefacts/rebuild_outputs/.
"""

import argparse
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import ocha_stratus as stratus
import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

GDACS_BASE = "https://www.gdacs.org/gdacsapi/api"
ALERT_LEVELS = ["Red", "Orange", "Green"]
IMPACT_TIMEOUT = 120
IMPACT_RETRIES = 2
WORKERS = 8
OUTPUT_PREFIX = "ds-storm-impact-harmonisation/raw/gdacs"
LOCAL_DRY_DIR = Path(__file__).parent.parent / "artefacts" / "rebuild_outputs"

BUFFER_KT = {"buffer39": 34, "buffer74": 64}


def output_paths(source_tag: str) -> dict:
    """Blob paths, tagged with the source filter so runs do not clobber each other."""
    return {
        "adm0": f"{OUTPUT_PREFIX}/gdacs_historical_adm0_exposure_v2_{source_tag}.csv",
        "adm1": f"{OUTPUT_PREFIX}/gdacs_historical_adm1_exposure_v2_{source_tag}.csv",
        "log": f"{OUTPUT_PREFIX}/gdacs_rebuild_fetch_log_{source_tag}.csv",
    }

print_lock = threading.Lock()


def log(msg: str):
    with print_lock:
        print(msg, flush=True)


# ---------------------------------------------------------------------------
# Step 1: Paginated event list (source=NOAA)
# ---------------------------------------------------------------------------


def fetch_all_events(from_date: str, to_date: str, source: str | None) -> pd.DataFrame:
    """source: None -> all providers; otherwise filter on exact match (e.g. 'NOAA', 'JTWC')."""
    all_events = []
    source_label = source or "ALL"
    for alert_level in ALERT_LEVELS:
        page = 1
        log(f"  [{alert_level}] fetching...")
        while True:
            params = {
                "eventlist": "TC",
                "fromDate": from_date,
                "toDate": to_date,
                "alertlevel": alert_level,
                "pageSize": 100,
                "pageNumber": page,
            }
            resp = requests.get(
                f"{GDACS_BASE}/events/geteventlist/search",
                params=params,
                timeout=60,
            )
            if not resp.text.strip():
                break
            features = resp.json().get("features", [])
            if not features:
                break

            kept = 0
            for f in features:
                p = f["properties"]
                if source and p.get("source") != source:
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
                kept += 1

            log(f"    page {page}: {len(features)} features, {kept} kept ({source_label})")
            page += 1
            time.sleep(0.2)

    df = pd.DataFrame(all_events).drop_duplicates("event_id").reset_index(drop=True)
    log(f"  total: {len(df)} unique {source_label} events in {from_date}..{to_date}")
    return df


# ---------------------------------------------------------------------------
# Step 2: Per-event impact fetch, with retries + threads
# ---------------------------------------------------------------------------


def _get_json(url: str, params: dict | None = None, timeout: int = IMPACT_TIMEOUT) -> dict:
    """GET with retries. Raises the last exception if all attempts fail."""
    last_exc: Exception | None = None
    for attempt in range(IMPACT_RETRIES + 1):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_exc = e
            if attempt < IMPACT_RETRIES:
                time.sleep(2 * (attempt + 1))
                continue
            raise last_exc


def parse_impact_datums(datums: list) -> tuple[dict, dict]:
    """Return (country_rows_by_iso3, alert_rows_by_fips_admin)."""
    country = {}
    alert = {}
    for g in datums:
        alias = g.get("alias")
        if alias == "country":
            for d in g.get("datum", []):
                sc = {s["name"]: s["value"] for s in d.get("scalars", {}).get("scalar", [])}
                iso3 = sc.get("ISO_3DIGIT")
                if not iso3:
                    continue
                pop_raw = sc.get("POP_AFFECTED")
                try:
                    pop = int(float(pop_raw)) if pop_raw not in (None, "") else None
                except Exception:
                    pop = None
                country[iso3] = {
                    "iso3": iso3,
                    "country_name": sc.get("CNTRY_NAME"),
                    "pop": pop,
                }
        elif alias == "alert":
            for d in g.get("datum", []):
                sc = {s["name"]: s["value"] for s in d.get("scalars", {}).get("scalar", [])}
                fips = sc.get("FIPS_ADMIN")
                if not fips:
                    continue
                gmi = sc.get("GMI_ADMIN", "")
                iso3 = gmi.split("-")[0] if gmi else None
                pop_raw = sc.get("POP_AFFECTED")
                try:
                    pop = int(float(pop_raw)) if pop_raw not in (None, "") else None
                except Exception:
                    pop = None
                alert[fips] = {
                    "iso3": iso3,
                    "fips_admin": fips,
                    "gmi_admin": gmi,
                    "adm1_name": sc.get("ADMIN_NAME"),
                    "adm1_type": sc.get("TYPE_ENG"),
                    "pop": pop,
                }
    return country, alert


def fetch_event_impact(event_id: str) -> tuple[list, list, dict]:
    """
    Returns (adm0_rows, adm1_rows, status_dict).

    status_dict has keys: event_detail_ok, buffer39_ok, buffer74_ok, episode_id, errors.
    """
    status = {
        "event_id": event_id,
        "event_detail_ok": False,
        "buffer39_ok": False,
        "buffer74_ok": False,
        "episode_id": None,
        "errors": [],
    }

    try:
        props = _get_json(
            f"{GDACS_BASE}/events/getepisodedata",
            params={"eventtype": "TC", "eventid": event_id},
        ).get("properties", {})
        status["event_detail_ok"] = True
    except Exception as e:
        status["errors"].append(f"detail: {e}")
        return [], [], status

    last_ep_url = props.get("episodes", [{}])[-1].get("details", "")
    if last_ep_url and "episodeid=" in last_ep_url:
        try:
            status["episode_id"] = int(last_ep_url.split("episodeid=")[-1].split("&")[0])
        except Exception:
            pass

    buffers = {
        k: v
        for k, v in props.get("impacts", [{}])[0].get("resource", {}).items()
        if k.startswith("buffer")
    }

    adm0 = {}
    adm1 = {}
    for buf_key, buf_url in buffers.items():
        col_kt = BUFFER_KT.get(buf_key)
        if col_kt is None:
            continue
        col_name = f"pop_{col_kt}kt"
        try:
            datums = _get_json(buf_url).get("datums", [])
            status[f"{buf_key}_ok"] = True
        except Exception as e:
            status["errors"].append(f"{buf_key}: {type(e).__name__}")
            continue

        country_rows, alert_rows = parse_impact_datums(datums)
        for iso3, row in country_rows.items():
            adm0.setdefault(
                iso3,
                {
                    "event_id": event_id,
                    "episode_id": status["episode_id"],
                    "iso3": iso3,
                    "country_name": row["country_name"],
                },
            )[col_name] = row["pop"]
        for fips, row in alert_rows.items():
            adm1.setdefault(
                fips,
                {
                    "event_id": event_id,
                    "episode_id": status["episode_id"],
                    "iso3": row["iso3"],
                    "fips_admin": fips,
                    "gmi_admin": row["gmi_admin"],
                    "adm1_name": row["adm1_name"],
                    "adm1_type": row["adm1_type"],
                },
            )[col_name] = row["pop"]

    adm0_list = list(adm0.values())
    adm1_list = list(adm1.values())

    # If the event had no buffer resources at all (weak storm) or every buffer
    # fetch returned no country rows, still emit one placeholder row at the
    # adm0 level so the event is visible in the output with NaN pop columns.
    if not adm0_list:
        adm0_list = [
            {
                "event_id": event_id,
                "episode_id": status["episode_id"],
                "iso3": None,
                "country_name": None,
                "pop_34kt": None,
                "pop_64kt": None,
            }
        ]
        status["placeholder_row"] = True
    else:
        status["placeholder_row"] = False

    return adm0_list, adm1_list, status


def threaded_impact_fetch(df_events: pd.DataFrame):
    adm0_all: list[dict] = []
    adm1_all: list[dict] = []
    status_all: list[dict] = []

    meta_by_eid = {
        row["event_id"]: row.to_dict() for _, row in df_events.iterrows()
    }

    completed = 0
    total = len(df_events)
    t0 = time.time()

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = {
            ex.submit(fetch_event_impact, eid): eid
            for eid in df_events["event_id"].tolist()
        }
        for fut in as_completed(futures):
            eid = futures[fut]
            meta = meta_by_eid[eid]
            try:
                adm0_rows, adm1_rows, status = fut.result()
            except Exception as e:
                adm0_rows, adm1_rows = [], []
                status = {
                    "event_id": eid,
                    "event_detail_ok": False,
                    "buffer39_ok": False,
                    "buffer74_ok": False,
                    "episode_id": None,
                    "errors": [f"fatal: {e}"],
                }
            for r in adm0_rows:
                r.update(
                    {
                        "storm_name": meta["storm_name"],
                        "source": meta["source"],
                        "from_date": meta["from_date"],
                        "alert_level": meta["alert_level"],
                    }
                )
            for r in adm1_rows:
                r.update(
                    {
                        "storm_name": meta["storm_name"],
                        "source": meta["source"],
                        "from_date": meta["from_date"],
                        "alert_level": meta["alert_level"],
                    }
                )
            status["storm_name"] = meta["storm_name"]
            status["alert_level"] = meta["alert_level"]
            status["errors"] = "; ".join(status["errors"])
            adm0_all.extend(adm0_rows)
            adm1_all.extend(adm1_rows)
            status_all.append(status)
            completed += 1
            if completed % 10 == 0 or completed == total:
                rate = completed / (time.time() - t0 + 0.01)
                log(
                    f"    progress: {completed}/{total}  "
                    f"({rate:.1f} events/s)  adm0_rows={len(adm0_all)}"
                )

    return adm0_all, adm1_all, status_all


# ---------------------------------------------------------------------------
# Step 3: IBTrACS SID join (via Azure DB; skip on failure)
# ---------------------------------------------------------------------------


def parse_storm_name(storm_name: str) -> tuple[int | None, str | None]:
    """GDACS 'Tropical Cyclone FIONA-22' -> (2022, 'FIONA')."""
    try:
        season_str = storm_name.split("-")[-1]
        season = int("20" + season_str)
        name = storm_name.split("-")[0].split(" ")[-1].upper()
        return season, name
    except Exception:
        return None, None


def join_ibtracs(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    parsed = df["storm_name"].apply(parse_storm_name)
    df["season"] = parsed.apply(lambda x: x[0])
    df["name"] = parsed.apply(lambda x: x[1])

    try:
        engine = stratus.get_engine("prod")
        with engine.connect() as conn:
            ibt = pd.read_sql(
                "SELECT sid, season, name, atcf_id, genesis_basin, provisional "
                "FROM storms.ibtracs_storms",
                conn,
            )
        ibt["name"] = ibt["name"].astype(str).str.upper()
    except Exception as e:
        log(f"  IBTrACS join SKIPPED ({type(e).__name__}: {e})")
        df["sid"] = None
        return df

    # Prefer non-provisional SIDs when there are duplicates on (season, name).
    ibt = ibt.sort_values("provisional").drop_duplicates(
        subset=["season", "name"], keep="first"
    )
    return df.merge(ibt, on=["season", "name"], how="left")


# ---------------------------------------------------------------------------
# Step 4: Main
# ---------------------------------------------------------------------------


def write_output(
    df_adm0: pd.DataFrame,
    df_adm1: pd.DataFrame,
    df_log: pd.DataFrame,
    dry_run: bool,
    source_tag: str,
):
    paths = output_paths(source_tag)
    if dry_run:
        LOCAL_DRY_DIR.mkdir(parents=True, exist_ok=True)
        p0 = LOCAL_DRY_DIR / Path(paths["adm0"]).name
        p1 = LOCAL_DRY_DIR / Path(paths["adm1"]).name
        plog = LOCAL_DRY_DIR / Path(paths["log"]).name
        df_adm0.to_csv(p0, index=False)
        df_adm1.to_csv(p1, index=False)
        df_log.to_csv(plog, index=False)
        log(f"  dry-run wrote:\n    {p0}\n    {p1}\n    {plog}")
    else:
        stratus.upload_csv_to_blob(df_adm0, paths["adm0"])
        stratus.upload_csv_to_blob(df_adm1, paths["adm1"])
        stratus.upload_csv_to_blob(df_log, paths["log"])
        log(f"  uploaded:\n    {paths['adm0']}\n    {paths['adm1']}\n    {paths['log']}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--from-date", default="2022-01-01")
    parser.add_argument("--to-date", default=datetime.utcnow().strftime("%Y-%m-%d"))
    parser.add_argument(
        "--source",
        choices=["NOAA", "JTWC", "ALL"],
        default="NOAA",
        help=(
            "GDACS source filter. NOAA = North Atlantic + E.Pacific (Hannah's scope, default). "
            "JTWC = W.Pacific + Indian Ocean + S.Hem. ALL = no source filter."
        ),
    )
    args = parser.parse_args()

    source_filter = None if args.source == "ALL" else args.source
    log(f"Step 1: fetching TC event list (source={args.source})")
    df_events = fetch_all_events(args.from_date, args.to_date, source_filter)
    if df_events.empty:
        log("no events found; aborting.")
        sys.exit(1)

    log(f"\nStep 2: fetching impact per event (workers={WORKERS}, timeout={IMPACT_TIMEOUT}s, retries={IMPACT_RETRIES})")
    adm0_rows, adm1_rows, status_rows = threaded_impact_fetch(df_events)

    df_status = pd.DataFrame(status_rows)
    both_ok = (df_status["buffer39_ok"] & df_status["buffer74_ok"]).sum()
    log(
        f"\nFetch summary:\n"
        f"  events with both buffers ok: {both_ok}/{len(df_status)}\n"
        f"  events with buffer39 fail:   {(~df_status['buffer39_ok']).sum()}\n"
        f"  events with buffer74 fail:   {(~df_status['buffer74_ok']).sum()}\n"
        f"  events with detail fail:     {(~df_status['event_detail_ok']).sum()}"
    )

    log("\nStep 3: IBTrACS SID join")
    df_adm0 = join_ibtracs(pd.DataFrame(adm0_rows))
    df_adm1 = join_ibtracs(pd.DataFrame(adm1_rows))
    log(f"  adm0 rows: {len(df_adm0)}, sid populated: {df_adm0['sid'].notna().sum() if 'sid' in df_adm0 else 0}")
    log(f"  adm1 rows: {len(df_adm1)}, sid populated: {df_adm1['sid'].notna().sum() if 'sid' in df_adm1 else 0}")

    log(f"\nStep 4: {'dry-run write' if args.dry_run else 'upload to blob'}")
    write_output(df_adm0, df_adm1, df_status, dry_run=args.dry_run, source_tag=args.source)

    log("\nDone.")


if __name__ == "__main__":
    main()
