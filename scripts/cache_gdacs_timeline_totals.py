"""Fetch GDACS timeline (gettimeline) totals per storm — max pop39 / pop74 across
all advisories.

WHY: the per-country `getimpact` endpoint returns POP_AFFECTED = -1 ("not
computed") for GDACS's ~2016-2022 era, so the harmonized `gdacs_exposure` table
has no per-country values for those storms. But the *timeline* endpoint DID
compute a storm-wide total (e.g. Matthew 18.3M, Maria 11.6M). That total is the
only thing that distinguishes, when joining with CHD:
  - GENUINE zero  (timeline total = 0  -> GDACS truly exposed nobody  -> fill 0)
  - MISSING data  (timeline total > 0  -> per-country breakdown absent -> keep NaN)

The timeline total is storm-wide (summed over the whole wind polygon, NOT per
country), so it cannot fill a per-country cell — it only classifies the storm.

Output (blob + local mirror):
  ds-storm-impact-harmonisation/processed/gdacs_timeline_totals.parquet
  columns: atcf_id, gdacs_eventid, timeline_pop39_max, timeline_pop74_max,
           n_advisories, status
"""

from __future__ import annotations

import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import ocha_stratus as stratus
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import text

load_dotenv()
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.datasets.gdacs import get_timeline  # noqa: E402

BLOB = "ds-storm-impact-harmonisation/processed/gdacs_timeline_totals.parquet"
LOCAL = Path(__file__).parent.parent / "artefacts" / "gdacs_timeline_totals.parquet"
WORKERS = 8


def gdacs_linked_storms(engine) -> pd.DataFrame:
    """Every NHC storm (season>=2001) linked to a GDACS event."""
    return pd.read_sql(text("""
        SELECT DISTINCT l.atcf_id, l.gdacs_eventid
        FROM storms.storm_id_lookup l
        JOIN storms.nhc_storms s ON s.atcf_id = l.atcf_id
        WHERE l.gdacs_eventid IS NOT NULL AND s.season >= 2001
    """), engine)


def fetch_one(atcf_id: str, eid: int) -> dict:
    base = dict(atcf_id=atcf_id, gdacs_eventid=eid)
    try:
        tl = get_timeline(int(eid))
        p39 = pd.to_numeric(tl.get("pop39"), errors="coerce")
        p74 = pd.to_numeric(tl.get("pop74"), errors="coerce")
        return {**base,
                "timeline_pop39_max": float(p39.max()) if p39.notna().any() else 0.0,
                "timeline_pop74_max": float(p74.max()) if p74.notna().any() else 0.0,
                "n_advisories": int(len(tl)), "status": "ok"}
    except Exception as e:  # noqa: BLE001 - record the failure, don't abort the sweep
        return {**base, "timeline_pop39_max": None, "timeline_pop74_max": None,
                "n_advisories": 0, "status": f"err:{type(e).__name__}"}


def main(upload: bool = True) -> None:
    engine = stratus.get_engine("dev")
    storms = gdacs_linked_storms(engine)
    print(f"GDACS-linked storms to fetch: {len(storms)}", flush=True)
    rows, done = [], 0
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(fetch_one, r.atcf_id, r.gdacs_eventid): r.atcf_id
                for r in storms.itertuples()}
        for fut in as_completed(futs):
            rows.append(fut.result())
            done += 1
            if done % 25 == 0 or done == len(storms):
                print(f"  {done}/{len(storms)}", flush=True)
    out = pd.DataFrame(rows)
    LOCAL.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(LOCAL, index=False)
    ok = (out.status == "ok").sum()
    nonzero = (pd.to_numeric(out.timeline_pop39_max, errors="coerce") > 0).sum()
    print(f"\nfetched ok: {ok}/{len(out)} | pop39>0: {nonzero} | "
          f"errors: {(out.status != 'ok').sum()}")
    if upload:
        stratus.upload_parquet_to_blob(out, BLOB)
        print(f"uploaded -> blob {BLOB}")
    print(f"wrote {LOCAL}")


if __name__ == "__main__":
    main(upload="--no-upload" not in sys.argv)
