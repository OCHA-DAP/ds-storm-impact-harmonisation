"""Export static data files for the JS storm exposure comparison app (app/).

One-off (rerun when the DB updates):
    uv run python export_app_data.py

Writes to app/data/:
    core.json            countries, storms, forecast/obsv/external series, finals
    tracks/{iso3}.json   IBTrACS track polylines for that country's storms
    buffers/{atcf}.json  simplified fcastonly/obsv track buffers per time & wind level
"""

import json
from pathlib import Path

import geopandas as gpd
import ocha_stratus as stratus
import pandas as pd
from sqlalchemy import bindparam, text

STAGE = "dev"
ADMIN_LEVEL = 0
KEYS = ["atcf_id", "iso3", "wind_speed_kt"]
OUT = Path(__file__).parent / "app" / "data"
SIMPLIFY_TOL = 0.02  # degrees, ~2 km
NE_URL = "https://naturalearth.s3.amazonaws.com/10m_cultural/ne_10m_admin_0_countries.zip"


def iso(t) -> str:
    return pd.Timestamp(t).strftime("%Y-%m-%dT%H:%M")


def export_core(engine) -> pd.DataFrame:
    chd_sql = text("""
        SELECT atcf_id, iso3, wind_speed_kt, issued_time AS time,
               'fcastonly' AS source, pop_exposed
        FROM storms.nhc_tracks_fcastonly_exposure WHERE admin_level = :al
        UNION ALL
        SELECT atcf_id, iso3, wind_speed_kt, valid_time AS time,
               'obsv' AS source, pop_exposed
        FROM storms.nhc_tracks_obsv_exposure WHERE admin_level = :al
    """)
    ext_sql = text("""
        SELECT DISTINCT lk.atcf_id, e.iso3, e.wind_speed_kt, e.valid_time AS time,
               'gdacs' AS source, e.pop_exposed
        FROM storms.gdacs_exposure e
        JOIN storms.storm_id_lookup lk ON lk.gdacs_eventid = e.gdacs_eventid
        WHERE e.admin_level = :al AND lk.atcf_id IS NOT NULL
        UNION ALL
        SELECT DISTINCT lk.atcf_id, e.iso3, e.wind_speed_kt, e.valid_time AS time,
               'adam' AS source, e.pop_exposed
        FROM storms.adam_exposure e
        JOIN storms.storm_id_lookup lk ON lk.adam_eventid = e.adam_eventid
        WHERE e.admin_level = :al AND lk.atcf_id IS NOT NULL
    """)
    storms_sql = text("""
        SELECT COALESCE(nhc.atcf_id, ib.atcf_id) AS atcf_id,
               COALESCE(NULLIF(nhc.name, 'NaN'), ib.name) AS name,
               COALESCE(nhc.season, ib.season) AS season,
               ib.sid
        FROM storms.nhc_storms nhc
        FULL JOIN storms.ibtracs_storms ib ON ib.atcf_id = nhc.atcf_id
        WHERE COALESCE(nhc.atcf_id, ib.atcf_id) IS NOT NULL
    """)
    with engine.connect() as conn:
        chd = pd.read_sql(chd_sql, conn, params={"al": ADMIN_LEVEL})
        ext = pd.read_sql(ext_sql, conn, params={"al": ADMIN_LEVEL})
        storms = pd.read_sql(storms_sql, conn).drop_duplicates("atcf_id")
        countries = pd.read_sql(
            text("SELECT iso3, name FROM public.polygon WHERE adm_level = 0"), conn
        )
        pop = pd.read_sql(
            text("SELECT iso3, total_pop FROM storms.admin_population WHERE admin_level = 0"),
            conn,
        )

    chd = chd.dropna(subset=["atcf_id", "iso3"])
    ext = ext.dropna(subset=["atcf_id", "iso3"])
    for df in (chd, ext):
        df["time"] = pd.to_datetime(df["time"])
        df["wind_speed_kt"] = df["wind_speed_kt"].astype(int)
        df["pop_exposed"] = df["pop_exposed"].astype(float)

    # Forecast value per issued_time = obsv accrued up to that time (backward
    # match) + fcastonly from that time onward (same semantics as the apps).
    fo = chd[chd["source"] == "fcastonly"].drop(columns="source")
    ob = (
        chd[chd["source"] == "obsv"]
        .drop(columns="source")
        .rename(columns={"pop_exposed": "pop_obsv", "time": "obsv_time"})
    )
    comb = pd.merge_asof(
        fo.sort_values("time"),
        ob.sort_values("obsv_time"),
        left_on="time",
        right_on="obsv_time",
        by=KEYS,
        direction="backward",
    )
    comb["total"] = comb["pop_exposed"].fillna(0) + comb["pop_obsv"].fillna(0)

    final_obsv = (
        ob.sort_values("obsv_time").groupby(KEYS, as_index=False).last()
    )[KEYS + ["pop_obsv"]]
    max_fcst = (
        comb.groupby(KEYS, as_index=False)["total"]
        .max()
        .rename(columns={"total": "fcast_max"})
    )

    def final_ext(source: str) -> pd.DataFrame:
        d = ext[ext["source"] == source].sort_values("time")
        return (
            d.groupby(KEYS, as_index=False)
            .last()[KEYS + ["pop_exposed"]]
            .rename(columns={"pop_exposed": source})
        )

    finals = (
        final_obsv.merge(max_fcst, on=KEYS, how="outer")
        .merge(final_ext("gdacs"), on=KEYS, how="outer")
        .merge(final_ext("adam"), on=KEYS, how="outer")
    )

    print("Loading Natural Earth bboxes…")
    ne = gpd.read_file(NE_URL)
    nb = ne.bounds
    nb["iso3"] = ne["ADM0_A3"].values
    bboxes = nb.groupby("iso3").agg(
        {"minx": "min", "miny": "min", "maxx": "max", "maxy": "max"}
    )

    name_map = dict(zip(countries["iso3"], countries["name"]))
    pop_map = dict(zip(pop["iso3"], pop["total_pop"].astype(int)))
    iso3s = sorted(finals["iso3"].dropna().unique())
    country_rows = []
    for i in iso3s:
        bbox = (
            [round(v, 2) for v in bboxes.loc[i]] if i in bboxes.index else None
        )
        name = name_map.get(i)
        if not isinstance(name, str) or not name:
            name = i
        p = pop_map.get(i)
        country_rows.append(
            {"iso3": i, "name": name, "pop": int(p) if pd.notna(p) else None, "bbox": bbox}
        )

    def num(v):
        return None if pd.isna(v) else int(v)

    core = {
        "record_years": int(comb["time"].dt.year.max() - comb["time"].dt.year.min())
        + 1,
        "countries": country_rows,
        "storms": {
            r.atcf_id: {
                "name": (
                    str(r.name).title()
                    if pd.notna(r.name) and str(r.name) not in ("", "NaN")
                    else r.atcf_id
                ),
                "season": num(r.season),
                "sid": r.sid if pd.notna(r.sid) else None,
            }
            for r in storms.itertuples()
        },
        # forecast value series per issued time
        "series": [
            [r.atcf_id, r.iso3, int(r.wind_speed_kt), iso(r.time),
             num(r.pop_exposed), num(r.pop_obsv), num(r.total)]
            for r in comb.sort_values("time").itertuples()
        ],
        # observed series per valid time
        "obsv": [
            [r.atcf_id, r.iso3, int(r.wind_speed_kt), iso(r.obsv_time), num(r.pop_obsv)]
            for r in ob.sort_values("obsv_time").itertuples()
        ],
        # external estimates per valid time
        "ext": [
            [r.source, r.atcf_id, r.iso3, int(r.wind_speed_kt), iso(r.time),
             num(r.pop_exposed)]
            for r in ext.sort_values("time").itertuples()
        ],
        "finals": [
            [r.atcf_id, r.iso3, int(r.wind_speed_kt), num(r.pop_obsv),
             num(r.fcast_max), num(r.gdacs), num(r.adam)]
            for r in finals.itertuples()
        ],
    }
    OUT.mkdir(parents=True, exist_ok=True)
    with open(OUT / "core.json", "w") as f:
        json.dump(core, f, separators=(",", ":"))
    print(f"core.json: {(OUT / 'core.json').stat().st_size / 1e6:.1f} MB")

    # tracks per country
    finals_storms = storms.set_index("atcf_id")
    country_storms = (
        finals[["atcf_id", "iso3"]].drop_duplicates().groupby("iso3")["atcf_id"].agg(list)
    )
    sid_of = {
        a: finals_storms.loc[a, "sid"]
        for a in finals["atcf_id"].unique()
        if a in finals_storms.index and pd.notna(finals_storms.loc[a, "sid"])
    }
    all_sids = sorted(set(sid_of.values()))
    tracks_sql = text("""
        SELECT sid, valid_time, ST_Y(geometry) AS lat, ST_X(geometry) AS lon
        FROM storms.ibtracs_tracks_geo
        WHERE sid IN :sids
        ORDER BY sid, valid_time
    """).bindparams(bindparam("sids", expanding=True))
    with engine.connect() as conn:
        tr = pd.read_sql(tracks_sql, conn, params={"sids": all_sids})

    def polyline(g: pd.DataFrame) -> list:
        lons = g["lon"].to_numpy().copy()
        # unwrap antimeridian jumps so leaflet doesn't streak across the map
        for j in range(1, len(lons)):
            if lons[j] - lons[j - 1] > 180:
                lons[j:] -= 360
            elif lons[j] - lons[j - 1] < -180:
                lons[j:] += 360
        return [
            [round(la, 2), round(lo, 2)] for la, lo in zip(g["lat"], lons)
        ]

    lines = {sid: polyline(g) for sid, g in tr.groupby("sid")}
    (OUT / "tracks").mkdir(exist_ok=True)
    for iso3_, atcfs in country_storms.items():
        payload = {
            a: lines[sid_of[a]] for a in atcfs if a in sid_of and sid_of[a] in lines
        }
        with open(OUT / "tracks" / f"{iso3_}.json", "w") as f:
            json.dump(payload, f, separators=(",", ":"))
    print(f"tracks: {len(country_storms)} country files")

    return finals


def export_buffers(engine) -> None:
    (OUT / "buffers").mkdir(parents=True, exist_ok=True)
    sql = {
        "fcastonly": text(f"""
            SELECT atcf_id, issued_time AS time, wind_speed_kt,
                   ST_AsGeoJSON(ST_SimplifyPreserveTopology(geometry, {SIMPLIFY_TOL}), 2) AS geom
            FROM storms.nhc_tracks_fcastonly_buffers
            ORDER BY atcf_id, issued_time
        """),
        "obsv": text(f"""
            SELECT atcf_id, valid_time AS time, wind_speed_kt,
                   ST_AsGeoJSON(ST_SimplifyPreserveTopology(geometry, {SIMPLIFY_TOL}), 2) AS geom
            FROM storms.nhc_tracks_obsv_buffers
            ORDER BY atcf_id, valid_time
        """),
    }
    data: dict[str, dict] = {}
    for kind, q in sql.items():
        with engine.connect().execution_options(stream_results=True) as conn:
            for chunk in pd.read_sql(q, conn, chunksize=2000):
                for r in chunk.itertuples():
                    if not isinstance(r.geom, str):
                        continue  # NULL/degenerate geometry after simplification
                    s = data.setdefault(r.atcf_id, {"fcastonly": {}, "obsv": {}})
                    s[kind].setdefault(iso(r.time), {})[str(r.wind_speed_kt)] = (
                        json.loads(r.geom)
                    )
        print(f"buffers: {kind} loaded")
    total = 0
    for atcf_id, payload in data.items():
        p = OUT / "buffers" / f"{atcf_id}.json"
        with open(p, "w") as f:
            json.dump(payload, f, separators=(",", ":"))
        total += p.stat().st_size
    print(f"buffers: {len(data)} storm files, {total / 1e6:.0f} MB total")


if __name__ == "__main__":
    engine = stratus.get_engine(stage=STAGE)
    export_core(engine)
    export_buffers(engine)
    print("done")
