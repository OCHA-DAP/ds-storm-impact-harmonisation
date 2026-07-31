import marimo

__generated_with = "0.23.5"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell
def _(mo):
    mo.center(mo.md("# Exposure comparison: CHD vs GDACS vs ADAM"))
    return


@app.cell
def _():
    import pandas as pd
    from sqlalchemy import text
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    import plotly.express as px
    import ocha_stratus as stratus

    STAGE = "dev"
    ADMIN_LEVEL = 0
    return ADMIN_LEVEL, STAGE, go, pd, px, stratus, text


@app.cell
def _(STAGE, stratus):
    engine = stratus.get_engine(stage=STAGE)
    return (engine,)


@app.cell
def _(ADMIN_LEVEL, engine, pd, text):
    # Final (latest) estimate per (atcf_id, iso3, wind_speed_kt) across all five sources
    _sql = text("""
        WITH fcast AS (
            SELECT DISTINCT ON (atcf_id, iso3, wind_speed_kt)
                atcf_id, iso3, wind_speed_kt, pop_exposed AS pop_fcast
            FROM storms.nhc_tracks_fcast_exposure
            WHERE admin_level = :al
            ORDER BY atcf_id, iso3, wind_speed_kt, issued_time DESC
        ),
        obsv AS (
            SELECT DISTINCT ON (atcf_id, iso3, wind_speed_kt)
                atcf_id, iso3, wind_speed_kt, pop_exposed AS pop_obsv
            FROM storms.nhc_tracks_obsv_exposure
            WHERE admin_level = :al
            ORDER BY atcf_id, iso3, wind_speed_kt, valid_time DESC
        ),
        fcastonly AS (
            SELECT DISTINCT ON (atcf_id, iso3, wind_speed_kt)
                atcf_id, iso3, wind_speed_kt, pop_exposed AS pop_fcastonly
            FROM storms.nhc_tracks_fcastonly_exposure
            WHERE admin_level = :al
            ORDER BY atcf_id, iso3, wind_speed_kt, issued_time DESC
        ),
        gdacs AS (
            SELECT DISTINCT ON (lk.atcf_id, g.iso3, g.wind_speed_kt)
                lk.atcf_id, g.iso3, g.wind_speed_kt, g.pop_exposed AS pop_gdacs
            FROM storms.gdacs_exposure g
            JOIN storms.storm_id_lookup lk ON lk.gdacs_eventid = g.gdacs_eventid
            WHERE g.admin_level = :al
            ORDER BY lk.atcf_id, g.iso3, g.wind_speed_kt, g.valid_time DESC
        ),
        adam AS (
            SELECT DISTINCT ON (lk.atcf_id, a.iso3, a.wind_speed_kt)
                lk.atcf_id, a.iso3, a.wind_speed_kt, a.pop_exposed AS pop_adam
            FROM storms.adam_exposure a
            JOIN storms.storm_id_lookup lk ON lk.adam_eventid = a.adam_eventid
            WHERE a.admin_level = :al
            ORDER BY lk.atcf_id, a.iso3, a.wind_speed_kt, a.valid_time DESC
        ),
        all_keys AS (
            SELECT atcf_id, iso3, wind_speed_kt FROM fcast
            UNION SELECT atcf_id, iso3, wind_speed_kt FROM obsv
            UNION SELECT atcf_id, iso3, wind_speed_kt FROM fcastonly
            UNION SELECT atcf_id, iso3, wind_speed_kt FROM gdacs
            UNION SELECT atcf_id, iso3, wind_speed_kt FROM adam
        )
        SELECT
            k.atcf_id, k.iso3, k.wind_speed_kt,
            COALESCE(NULLIF(nhc.name, 'NaN'), ib.name) AS name,
            COALESCE(nhc.season, ib.season) AS season,
            f.pop_fcast, o.pop_obsv, fc.pop_fcastonly,
            g.pop_gdacs, a.pop_adam
        FROM all_keys k
        LEFT JOIN storms.nhc_storms nhc ON nhc.atcf_id = k.atcf_id
        LEFT JOIN storms.ibtracs_storms ib ON ib.atcf_id = k.atcf_id
        LEFT JOIN fcast f ON f.atcf_id = k.atcf_id AND f.iso3 = k.iso3 AND f.wind_speed_kt = k.wind_speed_kt
        LEFT JOIN obsv o ON o.atcf_id = k.atcf_id AND o.iso3 = k.iso3 AND o.wind_speed_kt = k.wind_speed_kt
        LEFT JOIN fcastonly fc ON fc.atcf_id = k.atcf_id AND fc.iso3 = k.iso3 AND fc.wind_speed_kt = k.wind_speed_kt
        LEFT JOIN gdacs g ON g.atcf_id = k.atcf_id AND g.iso3 = k.iso3 AND g.wind_speed_kt = k.wind_speed_kt
        LEFT JOIN adam a ON a.atcf_id = k.atcf_id AND a.iso3 = k.iso3 AND a.wind_speed_kt = k.wind_speed_kt
        ORDER BY COALESCE(nhc.season, ib.season) DESC NULLS LAST, k.atcf_id, k.iso3, k.wind_speed_kt
    """)
    with engine.connect() as _conn:
        df_summary = pd.read_sql(_sql, _conn, params={"al": ADMIN_LEVEL})
    return (df_summary,)


@app.cell
def _(STAGE, df_summary, mo):
    _n_storms = df_summary["atcf_id"].nunique()
    _n_pairs = df_summary[["atcf_id", "iso3"]].drop_duplicates().shape[0]
    _seasons = df_summary["season"].dropna()
    _season_range = (
        f"{int(_seasons.min())}–{int(_seasons.max())}" if len(_seasons) else "—"
    )
    _n_gdacs = df_summary["pop_gdacs"].notna().sum()
    _n_adam = df_summary["pop_adam"].notna().sum()
    mo.hstack(
        [
            mo.stat(value=STAGE, label="Database"),
            mo.stat(value=str(_n_storms), label="Storms"),
            mo.stat(value=str(_n_pairs), label="Storm–country pairs"),
            mo.stat(value=_season_range, label="Season range"),
            mo.stat(value=str(_n_gdacs), label="GDACS rows"),
            mo.stat(value=str(_n_adam), label="ADAM rows"),
        ],
        justify="center",
    )
    return


@app.cell
def _(ADMIN_LEVEL, engine, pd, text):
    # Each CHD row paired with the nearest-in-time GDACS/ADAM value via lateral join.
    # For storms with a single GDACS valid_time this just picks that one value;
    # for 2025 storms where GDACS publishes rolling updates it picks the temporally
    # closest estimate.
    _sql = text("""
        SELECT f.atcf_id, f.iso3, f.wind_speed_kt, f.issued_time AS time,
               'fcast' AS source, f.pop_exposed AS pop_chd,
               g_near.pop_gdacs, g_near.gdacs_time,
               a_near.pop_adam, a_near.adam_time,
               COALESCE(NULLIF(nhc.name, 'NaN'), ib.name) AS name,
               COALESCE(nhc.season, ib.season) AS season
        FROM storms.nhc_tracks_fcast_exposure f
        LEFT JOIN storms.nhc_storms nhc ON nhc.atcf_id = f.atcf_id
        LEFT JOIN storms.ibtracs_storms ib ON ib.atcf_id = f.atcf_id
        LEFT JOIN LATERAL (
            SELECT g.pop_exposed AS pop_gdacs, g.valid_time AS gdacs_time
            FROM storms.gdacs_exposure g
            JOIN storms.storm_id_lookup lk ON lk.gdacs_eventid = g.gdacs_eventid
            WHERE lk.atcf_id = f.atcf_id AND g.iso3 = f.iso3
              AND g.wind_speed_kt = f.wind_speed_kt AND g.admin_level = :al
            ORDER BY ABS(EXTRACT(EPOCH FROM (g.valid_time - f.issued_time)))
            LIMIT 1
        ) g_near ON true
        LEFT JOIN LATERAL (
            SELECT a.pop_exposed AS pop_adam, a.valid_time AS adam_time
            FROM storms.adam_exposure a
            JOIN storms.storm_id_lookup lk ON lk.adam_eventid = a.adam_eventid
            WHERE lk.atcf_id = f.atcf_id AND a.iso3 = f.iso3
              AND a.wind_speed_kt = f.wind_speed_kt AND a.admin_level = :al
            ORDER BY ABS(EXTRACT(EPOCH FROM (a.valid_time - f.issued_time)))
            LIMIT 1
        ) a_near ON true
        WHERE f.admin_level = :al

        UNION ALL

        SELECT o.atcf_id, o.iso3, o.wind_speed_kt, o.valid_time AS time,
               'obsv' AS source, o.pop_exposed AS pop_chd,
               g_near.pop_gdacs, g_near.gdacs_time,
               a_near.pop_adam, a_near.adam_time,
               COALESCE(NULLIF(nhc.name, 'NaN'), ib.name) AS name,
               COALESCE(nhc.season, ib.season) AS season
        FROM storms.nhc_tracks_obsv_exposure o
        LEFT JOIN storms.nhc_storms nhc ON nhc.atcf_id = o.atcf_id
        LEFT JOIN storms.ibtracs_storms ib ON ib.atcf_id = o.atcf_id
        LEFT JOIN LATERAL (
            SELECT g.pop_exposed AS pop_gdacs, g.valid_time AS gdacs_time
            FROM storms.gdacs_exposure g
            JOIN storms.storm_id_lookup lk ON lk.gdacs_eventid = g.gdacs_eventid
            WHERE lk.atcf_id = o.atcf_id AND g.iso3 = o.iso3
              AND g.wind_speed_kt = o.wind_speed_kt AND g.admin_level = :al
            ORDER BY ABS(EXTRACT(EPOCH FROM (g.valid_time - o.valid_time)))
            LIMIT 1
        ) g_near ON true
        LEFT JOIN LATERAL (
            SELECT a.pop_exposed AS pop_adam, a.valid_time AS adam_time
            FROM storms.adam_exposure a
            JOIN storms.storm_id_lookup lk ON lk.adam_eventid = a.adam_eventid
            WHERE lk.atcf_id = o.atcf_id AND a.iso3 = o.iso3
              AND a.wind_speed_kt = o.wind_speed_kt AND a.admin_level = :al
            ORDER BY ABS(EXTRACT(EPOCH FROM (a.valid_time - o.valid_time)))
            LIMIT 1
        ) a_near ON true
        WHERE o.admin_level = :al

        UNION ALL

        SELECT fc.atcf_id, fc.iso3, fc.wind_speed_kt, fc.issued_time AS time,
               'fcastonly' AS source, fc.pop_exposed AS pop_chd,
               g_near.pop_gdacs, g_near.gdacs_time,
               a_near.pop_adam, a_near.adam_time,
               COALESCE(NULLIF(nhc.name, 'NaN'), ib.name) AS name,
               COALESCE(nhc.season, ib.season) AS season
        FROM storms.nhc_tracks_fcastonly_exposure fc
        LEFT JOIN storms.nhc_storms nhc ON nhc.atcf_id = fc.atcf_id
        LEFT JOIN storms.ibtracs_storms ib ON ib.atcf_id = fc.atcf_id
        LEFT JOIN LATERAL (
            SELECT g.pop_exposed AS pop_gdacs, g.valid_time AS gdacs_time
            FROM storms.gdacs_exposure g
            JOIN storms.storm_id_lookup lk ON lk.gdacs_eventid = g.gdacs_eventid
            WHERE lk.atcf_id = fc.atcf_id AND g.iso3 = fc.iso3
              AND g.wind_speed_kt = fc.wind_speed_kt AND g.admin_level = :al
            ORDER BY ABS(EXTRACT(EPOCH FROM (g.valid_time - fc.issued_time)))
            LIMIT 1
        ) g_near ON true
        LEFT JOIN LATERAL (
            SELECT a.pop_exposed AS pop_adam, a.valid_time AS adam_time
            FROM storms.adam_exposure a
            JOIN storms.storm_id_lookup lk ON lk.adam_eventid = a.adam_eventid
            WHERE lk.atcf_id = fc.atcf_id AND a.iso3 = fc.iso3
              AND a.wind_speed_kt = fc.wind_speed_kt AND a.admin_level = :al
            ORDER BY ABS(EXTRACT(EPOCH FROM (a.valid_time - fc.issued_time)))
            LIMIT 1
        ) a_near ON true
        WHERE fc.admin_level = :al

        UNION ALL

        -- fcastonly + nearest obsv, anchored on fcastonly issued_time
        SELECT fc.atcf_id, fc.iso3, fc.wind_speed_kt, fc.issued_time AS time,
               'combined' AS source,
               fc.pop_exposed + o_near.pop_obsv AS pop_chd,
               g_near.pop_gdacs, g_near.gdacs_time,
               a_near.pop_adam, a_near.adam_time,
               COALESCE(NULLIF(nhc.name, 'NaN'), ib.name) AS name,
               COALESCE(nhc.season, ib.season) AS season
        FROM storms.nhc_tracks_fcastonly_exposure fc
        LEFT JOIN storms.nhc_storms nhc ON nhc.atcf_id = fc.atcf_id
        LEFT JOIN storms.ibtracs_storms ib ON ib.atcf_id = fc.atcf_id
        LEFT JOIN LATERAL (
            SELECT o.pop_exposed AS pop_obsv
            FROM storms.nhc_tracks_obsv_exposure o
            WHERE o.atcf_id = fc.atcf_id AND o.iso3 = fc.iso3
              AND o.wind_speed_kt = fc.wind_speed_kt AND o.admin_level = :al
            ORDER BY ABS(EXTRACT(EPOCH FROM (o.valid_time - fc.issued_time)))
            LIMIT 1
        ) o_near ON true
        LEFT JOIN LATERAL (
            SELECT g.pop_exposed AS pop_gdacs, g.valid_time AS gdacs_time
            FROM storms.gdacs_exposure g
            JOIN storms.storm_id_lookup lk ON lk.gdacs_eventid = g.gdacs_eventid
            WHERE lk.atcf_id = fc.atcf_id AND g.iso3 = fc.iso3
              AND g.wind_speed_kt = fc.wind_speed_kt AND g.admin_level = :al
            ORDER BY ABS(EXTRACT(EPOCH FROM (g.valid_time - fc.issued_time)))
            LIMIT 1
        ) g_near ON true
        LEFT JOIN LATERAL (
            SELECT a.pop_exposed AS pop_adam, a.valid_time AS adam_time
            FROM storms.adam_exposure a
            JOIN storms.storm_id_lookup lk ON lk.adam_eventid = a.adam_eventid
            WHERE lk.atcf_id = fc.atcf_id AND a.iso3 = fc.iso3
              AND a.wind_speed_kt = fc.wind_speed_kt AND a.admin_level = :al
            ORDER BY ABS(EXTRACT(EPOCH FROM (a.valid_time - fc.issued_time)))
            LIMIT 1
        ) a_near ON true
        WHERE fc.admin_level = :al
    """)
    with engine.connect() as _conn:
        df_all_times = pd.read_sql(_sql, _conn, params={"al": ADMIN_LEVEL})
    return (df_all_times,)


@app.cell
def _(mo):
    _SOURCES = ["CHD fcast", "CHD obsv", "CHD fcastonly", "CHD fcastonly+obsv", "GDACS", "ADAM"]
    scatter_x = mo.ui.dropdown(options=_SOURCES, value="CHD fcastonly", label="X axis")
    scatter_y = mo.ui.dropdown(options=_SOURCES, value="GDACS", label="Y axis")
    scatter_ws = mo.ui.dropdown(options=[34, 50, 64], value=34, label="Wind speed (kt)")
    mo.hstack([scatter_x, scatter_y, scatter_ws], gap="2rem")
    return scatter_ws, scatter_x, scatter_y


@app.cell
def _(df_all_times, df_summary, go, mo, pd, scatter_ws, scatter_x, scatter_y):
    # Sources backed by df_all_times (per issued_time, nearest-matched external)
    _CHD_TO_SRC = {
        "CHD fcast": "fcast", "CHD obsv": "obsv",
        "CHD fcastonly": "fcastonly", "CHD fcastonly+obsv": "combined",
    }
    _EXT_TO_COL = {"GDACS": ("pop_gdacs", "gdacs_time"), "ADAM": ("pop_adam", "adam_time")}
    _SUM_COL = {
        "CHD fcast": "pop_fcast", "CHD obsv": "pop_obsv",
        "CHD fcastonly": "pop_fcastonly", "CHD fcastonly+obsv": "pop_combined",
        "GDACS": "pop_gdacs", "ADAM": "pop_adam",
    }

    _src_x, _src_y, _ws = scatter_x.value, scatter_y.value, scatter_ws.value

    # Precompute number of distinct external valid_times per (atcf_id, iso3, wind_speed_kt)
    # so we can flag single-estimate sources in the hover
    def _ext_single_flags(time_col: str):
        _n = (
            df_all_times[df_all_times[time_col].notna()]
            .groupby(["atcf_id", "iso3", "wind_speed_kt"])[time_col]
            .nunique()
            .reset_index(name="_n")
        )
        return set(
            zip(_n[_n["_n"] == 1]["atcf_id"],
                _n[_n["_n"] == 1]["iso3"],
                _n[_n["_n"] == 1]["wind_speed_kt"])
        )

    _gdacs_singles = _ext_single_flags("gdacs_time")
    _adam_singles  = _ext_single_flags("adam_time")

    # Decide data source
    _chd_src = _CHD_TO_SRC.get(_src_x) or _CHD_TO_SRC.get(_src_y)
    _ext_src  = _EXT_TO_COL.get(_src_y) or _EXT_TO_COL.get(_src_x)
    _chd_is_x = _src_x in _CHD_TO_SRC

    _use_per_time = bool(_chd_src and _ext_src)

    if _use_per_time:
        _ext_pop_col, _ext_time_col = _ext_src
        _ext_singles = _gdacs_singles if "gdacs" in _ext_pop_col else _adam_singles
        _d = df_all_times[
            (df_all_times["source"] == _chd_src) &
            (df_all_times["wind_speed_kt"] == _ws)
        ][["atcf_id", "iso3", "pop_chd", _ext_pop_col, _ext_time_col,
           "time", "name", "season"]].copy()
        _d = _d.dropna(subset=["pop_chd", _ext_pop_col]).query("pop_chd > 0 and " + _ext_pop_col + " > 0")
        _x_vals = _d["pop_chd"] if _chd_is_x else _d[_ext_pop_col]
        _y_vals = _d[_ext_pop_col] if _chd_is_x else _d["pop_chd"]

        def _fmt_time(row):
            _chd_t = row["time"].strftime("%Y-%m-%d %H:%M") if pd.notna(row["time"]) else "—"
            _ext_t_raw = row[_ext_time_col]
            _is_single = (row["atcf_id"], row["iso3"], _ws) in _ext_singles
            if pd.isna(_ext_t_raw):
                _ext_t = "—"
            elif _is_single:
                _ext_t = _ext_t_raw.strftime("%Y-%m-%d %H:%M") + " (single est.)"
            else:
                _ext_t = _ext_t_raw.strftime("%Y-%m-%d %H:%M")
            return f"CHD: {_chd_t} | {_src_y if _chd_is_x else _src_x}: {_ext_t}"

        _time_labels = _d.apply(_fmt_time, axis=1).values
        _customdata = list(zip(_d["name"], _d["season"], _d["iso3"], _time_labels))
    else:
        # Final estimates from df_summary
        _dfs = df_summary[df_summary["wind_speed_kt"] == _ws].copy()
        _dfs["pop_combined"] = _dfs["pop_fcastonly"].fillna(0) + _dfs["pop_obsv"].fillna(0)
        _dfs.loc[_dfs["pop_fcastonly"].isna() & _dfs["pop_obsv"].isna(), "pop_combined"] = float("nan")
        _xcol, _ycol = _SUM_COL[_src_x], _SUM_COL[_src_y]
        _d = _dfs[["atcf_id", "iso3", "name", "season", _xcol, _ycol]].dropna(subset=[_xcol, _ycol])
        _d = _d.query(f"{_xcol} > 0 and {_ycol} > 0")
        _x_vals, _y_vals = _d[_xcol], _d[_ycol]
        _customdata = list(zip(_d["name"], _d["season"], _d["iso3"],
                               ["final estimate"] * len(_d)))

    _fig = go.Figure()
    if len(_x_vals):
        _fig.add_trace(go.Scatter(
            x=_x_vals,
            y=_y_vals,
            mode="markers",
            marker=dict(color="#1976D2", size=5, opacity=0.55),
            customdata=_customdata,
            hovertemplate=(
                "<b>%{customdata[0]}</b> (%{customdata[1]})<br>"
                "ISO3: %{customdata[2]}<br>"
                "%{customdata[3]}<br>"
                f"{_src_x}: %{{x:,.0f}}<br>"
                f"{_src_y}: %{{y:,.0f}}"
                "<extra></extra>"
            ),
            showlegend=False,
        ))
        _all_vals = list(_x_vals[_x_vals > 0]) + list(_y_vals[_y_vals > 0])
        _vmin, _vmax = min(_all_vals), max(_all_vals)
        _fig.add_trace(go.Scatter(
            x=[_vmin, _vmax], y=[_vmin, _vmax],
            mode="lines",
            line=dict(color="grey", dash="dot", width=1),
            showlegend=False,
            hoverinfo="skip",
        ))

    _fig.update_layout(
        template="simple_white",
        height=600,
        margin=dict(l=60, r=20, t=40, b=60),
        xaxis=dict(type="log", title=_src_x),
        yaxis=dict(type="log", title=_src_y),
        title=f"{_src_x} vs {_src_y} — {_ws} kt",
    )
    mo.ui.plotly(_fig)
    return


@app.cell
def _(df_summary, pd, stratus):
    from io import BytesIO
    _iso3s = df_summary["iso3"].dropna().unique()
    _names: dict[str, str] = {}
    for _iso3 in _iso3s:
        try:
            _raw = stratus.load_blob_data(
                f"fieldmaps/edge-matched/humanitarian/intl/adm1/{_iso3}.parquet",
                container_name="global",
            )
            _row = pd.read_parquet(BytesIO(_raw), columns=["iso_3", "adm0_name"])
            _name = _row["adm0_name"].iloc[0]
            _names[_iso3] = str(_name) if (pd.notna(_name) and _name) else _iso3
        except Exception:
            _names[_iso3] = _iso3
    iso3_to_name = _names
    return (iso3_to_name,)


@app.cell
def _(mo):
    mo.md("""
    ## Detail view
    """)
    return


@app.cell
def _(df_summary, iso3_to_name, mo):
    _iso3s = df_summary["iso3"].dropna().unique()
    _opts = dict(sorted({iso3_to_name.get(iso3, iso3): iso3 for iso3 in _iso3s}.items()))
    country_selector = mo.ui.dropdown(
        options=_opts,
        value=next(iter(_opts)) if _opts else None,
        label="Country:",
    )
    country_selector
    return (country_selector,)


@app.cell
def _(country_selector, df_all_times, df_summary, go, iso3_to_name, mo, pd):
    _iso3 = country_selector.value

    # Final obsv per (atcf_id, wind_speed_kt) for this country
    _obsv = (
        df_summary[df_summary["iso3"] == _iso3][
            ["atcf_id", "name", "season", "wind_speed_kt", "pop_obsv"]
        ]
        .dropna(subset=["pop_obsv"])
    )
    mo.stop(_obsv.empty)

    # Max fcastonly+obsv across issued_times per (atcf_id, wind_speed_kt)
    _comb = (
        df_all_times[
            (df_all_times["iso3"] == _iso3)
            & (df_all_times["source"] == "combined")
        ]
        .groupby(["atcf_id", "wind_speed_kt"])["pop_chd"]
        .max()
        .reset_index(name="pop_max_combined")
    )

    # Storm label lookup
    _meta = (
        df_summary[df_summary["iso3"] == _iso3][["atcf_id", "name", "season"]]
        .drop_duplicates("atcf_id")
        .set_index("atcf_id")
    )
    def _label(atcf_id: str) -> str:
        row = _meta.loc[atcf_id] if atcf_id in _meta.index else None
        if row is None:
            return atcf_id
        n = row["name"]
        s = row["season"]
        n = str(n).title() if (pd.notna(n) and n) else atcf_id
        s = str(int(s)) if pd.notna(s) else ""
        return f"{n} {s}".strip()

    # Sort storms by 34kt final obsv exposure descending
    _order34 = (
        _obsv[_obsv["wind_speed_kt"] == 34]
        .set_index("atcf_id")["pop_obsv"]
        .sort_values(ascending=False)
    )
    _all_ids = _obsv["atcf_id"].unique()
    _storm_ids = list(_order34.index) + [a for a in _all_ids if a not in _order34.index]
    _xlabels = [_label(a) for a in _storm_ids]

    # 34=yellow, 50=orange, 64=red; obsv=dark, fcast+obsv=light
    _WS_COLORS = {
        34: ("#F9A825", "#FFE082"),   # amber-700, amber-200
        50: ("#E65100", "#FFCC80"),   # deep-orange-900, orange-200
        64: ("#C62828", "#EF9A9A"),   # red-900, red-200
    }

    _fig = go.Figure()
    for _ws in [34, 50, 64]:
        _dark, _light = _WS_COLORS[_ws]
        _sub = _obsv[_obsv["wind_speed_kt"] == _ws].set_index("atcf_id")
        _y = [_sub["pop_obsv"].get(a) for a in _storm_ids]
        _fig.add_trace(go.Bar(
            name=f"Obsv {_ws} kt",
            x=_xlabels, y=_y,
            marker_color=_dark,
            legendgroup=f"obsv{_ws}",
        ))
        _sub2 = _comb[_comb["wind_speed_kt"] == _ws].set_index("atcf_id")
        _y2 = [_sub2["pop_max_combined"].get(a) for a in _storm_ids]
        _fig.add_trace(go.Bar(
            name=f"Max fcast+obsv {_ws} kt",
            x=_xlabels, y=_y2,
            marker_color=_light,
            legendgroup=f"comb{_ws}",
        ))

    _cname = iso3_to_name.get(_iso3, _iso3)
    _fig.update_layout(
        barmode="group",
        template="simple_white",
        title=f"All storms — {_cname}",
        xaxis_title="Storm",
        yaxis_title="Population exposed",
        height=450,
        margin=dict(l=60, r=20, t=40, b=80),
        legend=dict(orientation="h", y=-0.25),
    )
    mo.ui.plotly(_fig)
    return


@app.cell
def _(country_selector, df_summary, mo, pd):
    _df_c = df_summary[df_summary["iso3"] == country_selector.value]
    _storms = (
        _df_c[["atcf_id", "name", "season"]]
        .drop_duplicates()
        .sort_values(["season", "name"], ascending=[False, True])
    )
    _options = {}
    for _, _sr in _storms.iterrows():
        _sname = str(_sr["name"]).title() if (pd.notna(_sr["name"]) and _sr["name"]) else _sr["atcf_id"]
        _sseas = str(int(_sr["season"])) if pd.notna(_sr["season"]) else ""
        _key = f"{_sname} {_sseas} ({_sr['atcf_id']})".strip()
        _options[_key] = _sr["atcf_id"]
    storm_selector = mo.ui.dropdown(
        options=_options,
        value=next(iter(_options.keys())) if _options else None,
        label="Storm:",
    )
    return (storm_selector,)


@app.cell
def _(storm_selector):
    storm_selector
    return


@app.cell
def _(country_selector, df_summary, mo, pd, storm_selector):
    mo.stop(storm_selector.value is None)
    _atcf_id = storm_selector.value
    _iso3 = country_selector.value
    _row = df_summary[
        (df_summary["atcf_id"] == _atcf_id) & (df_summary["iso3"] == _iso3)
    ].iloc[0]
    _has_gdacs = df_summary[
        (df_summary["atcf_id"] == _atcf_id) & df_summary["pop_gdacs"].notna()
    ].shape[0] > 0
    _has_adam = df_summary[
        (df_summary["atcf_id"] == _atcf_id) & df_summary["pop_adam"].notna()
    ].shape[0] > 0
    mo.hstack(
        [
            mo.stat(value=str(_row["name"] or _atcf_id).title(), label="Storm"),
            mo.stat(
                value=str(int(_row["season"])) if pd.notna(_row["season"]) else "—",
                label="Season",
            ),
            mo.stat(value=_atcf_id, label="ATCF ID"),
            mo.stat(value="Yes" if _has_gdacs else "No", label="In GDACS"),
            mo.stat(value="Yes" if _has_adam else "No", label="In ADAM"),
        ],
        justify="center",
    )
    return


@app.cell
def _(ADMIN_LEVEL, country_selector, engine, mo, pd, storm_selector, text):
    mo.stop(storm_selector.value is None, mo.md("Select a country and storm above."))
    _atcf_id = storm_selector.value
    _iso3 = country_selector.value
    _p = {"atcf_id": _atcf_id, "iso3": _iso3, "al": ADMIN_LEVEL}
    _queries = {
        "fcast": text("""
            SELECT issued_time AS time, wind_speed_kt, pop_exposed
            FROM storms.nhc_tracks_fcast_exposure
            WHERE atcf_id = :atcf_id AND iso3 = :iso3 AND admin_level = :al
            ORDER BY wind_speed_kt, issued_time
        """),
        "obsv": text("""
            SELECT valid_time AS time, wind_speed_kt, pop_exposed
            FROM storms.nhc_tracks_obsv_exposure
            WHERE atcf_id = :atcf_id AND iso3 = :iso3 AND admin_level = :al
            ORDER BY wind_speed_kt, valid_time
        """),
        "fcastonly": text("""
            SELECT issued_time AS time, wind_speed_kt, pop_exposed
            FROM storms.nhc_tracks_fcastonly_exposure
            WHERE atcf_id = :atcf_id AND iso3 = :iso3 AND admin_level = :al
            ORDER BY wind_speed_kt, issued_time
        """),
        "fcastonly+obsv": text("""
            SELECT fc.issued_time AS time, fc.wind_speed_kt,
                   fc.pop_exposed + o_near.pop_obsv AS pop_exposed
            FROM storms.nhc_tracks_fcastonly_exposure fc
            LEFT JOIN LATERAL (
                SELECT o.pop_exposed AS pop_obsv
                FROM storms.nhc_tracks_obsv_exposure o
                WHERE o.atcf_id = :atcf_id AND o.iso3 = :iso3
                  AND o.wind_speed_kt = fc.wind_speed_kt AND o.admin_level = :al
                ORDER BY ABS(EXTRACT(EPOCH FROM (o.valid_time - fc.issued_time)))
                LIMIT 1
            ) o_near ON true
            WHERE fc.atcf_id = :atcf_id AND fc.iso3 = :iso3 AND fc.admin_level = :al
              AND o_near.pop_obsv IS NOT NULL
            ORDER BY fc.wind_speed_kt, fc.issued_time
        """),
        "gdacs": text("""
            SELECT g.valid_time AS time, g.wind_speed_kt, g.pop_exposed
            FROM storms.gdacs_exposure g
            JOIN storms.storm_id_lookup lk ON lk.gdacs_eventid = g.gdacs_eventid
            WHERE lk.atcf_id = :atcf_id AND g.iso3 = :iso3 AND g.admin_level = :al
            ORDER BY g.wind_speed_kt, g.valid_time
        """),
        "adam": text("""
            SELECT a.valid_time AS time, a.wind_speed_kt, a.pop_exposed
            FROM storms.adam_exposure a
            JOIN storms.storm_id_lookup lk ON lk.adam_eventid = a.adam_eventid
            WHERE lk.atcf_id = :atcf_id AND a.iso3 = :iso3 AND a.admin_level = :al
            ORDER BY a.wind_speed_kt, a.valid_time
        """),
    }
    _dfs = []
    with engine.connect() as _conn:
        for _src, _sql in _queries.items():
            _df = pd.read_sql(_sql, _conn, params=_p)
            if not _df.empty:
                _df["source"] = _src
                _dfs.append(_df)
    df_ts = (
        pd.concat(_dfs, ignore_index=True)
        if _dfs
        else pd.DataFrame(columns=["time", "wind_speed_kt", "pop_exposed", "source"])
    )
    return (df_ts,)


@app.cell
def _(country_selector, df_summary, df_ts, mo, px, storm_selector):
    mo.stop(df_ts.empty, mo.md("No time series data for this selection."))
    _atcf_id = storm_selector.value
    _iso3 = country_selector.value
    _name_rows = df_summary[(df_summary["atcf_id"] == _atcf_id) & (df_summary["iso3"] == _iso3)]
    _storm_name = _name_rows["name"].iloc[0] if not _name_rows.empty else None
    _title = f"{_storm_name or _atcf_id} — {_iso3}"
    _SOURCE_COLORS = {
        "fcast": "#1565C0",
        "obsv": "#2E7D32",
        "fcastonly": "#6A1B9A",
        "fcastonly+obsv": "#00838F",
        "gdacs": "#BF360C",
        "adam": "#E65100",
    }
    _df = df_ts.copy()
    _df["wind_speed_kt"] = _df["wind_speed_kt"].astype(str) + " kt"
    _fig = px.line(
        _df.sort_values("time"),
        x="time",
        y="pop_exposed",
        color="source",
        facet_row="wind_speed_kt",
        markers=True,
        color_discrete_map=_SOURCE_COLORS,
        template="simple_white",
        title=_title,
        labels={"pop_exposed": "Population exposed", "time": "", "source": "Source"},
        height=550,
    )
    _fig.update_layout(margin=dict(l=0, r=0, t=40, b=0))
    _fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))
    mo.ui.plotly(_fig)
    return


@app.cell
def _(country_selector, df_summary, engine, go, mo, pd, storm_selector, text):
    mo.stop(storm_selector.value is None)
    _atcf_id = storm_selector.value
    _iso3 = country_selector.value
    _sql = text("""
        WITH chd_obsv AS (
            SELECT DISTINCT ON (pcode, wind_speed_kt)
                pcode, wind_speed_kt, pop_exposed AS pop_obsv
            FROM storms.nhc_tracks_obsv_exposure
            WHERE atcf_id = :atcf_id AND iso3 = :iso3 AND admin_level = 1
            ORDER BY pcode, wind_speed_kt, valid_time DESC
        ),
        gdacs_latest AS (
            SELECT DISTINCT ON (g.gdacs_admin_code, g.wind_speed_kt)
                g.gdacs_admin_code, g.wind_speed_kt, g.pop_exposed AS pop_gdacs
            FROM storms.gdacs_exposure g
            JOIN storms.storm_id_lookup lk ON lk.gdacs_eventid = g.gdacs_eventid
            WHERE lk.atcf_id = :atcf_id AND g.admin_level = 1
            ORDER BY g.gdacs_admin_code, g.wind_speed_kt, g.valid_time DESC
        )
        SELECT
            o.pcode,
            lk.fm_name AS adm1_name,
            o.wind_speed_kt,
            o.pop_obsv,
            gdl.pop_gdacs
        FROM chd_obsv o
        JOIN storms.gdacs_fm_lookup lk
            ON lk.fm_pcode = o.pcode AND lk.admin_level = 1
        LEFT JOIN gdacs_latest gdl
            ON gdl.gdacs_admin_code = lk.gmi_admin
            AND gdl.wind_speed_kt = o.wind_speed_kt
        WHERE gdl.pop_gdacs IS NOT NULL
        ORDER BY o.wind_speed_kt, lk.fm_name
    """)
    try:
        with engine.connect() as _conn:
            df_adm1 = pd.read_sql(_sql, _conn, params={"atcf_id": _atcf_id, "iso3": _iso3})
    except Exception:
        mo.stop(True, mo.md("Admin 1 lookup table not yet available."))
    mo.stop(df_adm1.empty)

    _name_rows = df_summary[(df_summary["atcf_id"] == _atcf_id) & (df_summary["iso3"] == _iso3)]
    _storm_name = str(_name_rows["name"].iloc[0]).title() if not _name_rows.empty else _atcf_id

    _WS_COLORS = {34: "#F9A825", 50: "#E65100", 64: "#C62828"}
    _fig = go.Figure()
    for _ws in [34, 50, 64]:
        _sub = df_adm1[df_adm1["wind_speed_kt"] == _ws]
        if _sub.empty:
            continue
        _fig.add_trace(go.Scatter(
            x=_sub["pop_obsv"],
            y=_sub["pop_gdacs"],
            mode="markers",
            marker=dict(color=_WS_COLORS[_ws], size=8),
            text=_sub["adm1_name"],
            hovertemplate="%{text}<br>CHD: %{x:,}<br>GDACS: %{y:,}<extra></extra>",
            name=f"{_ws} kt",
        ))
    _max_val = max(df_adm1["pop_obsv"].max(), df_adm1["pop_gdacs"].max())
    _fig.add_trace(go.Scatter(
        x=[0, _max_val], y=[0, _max_val],
        mode="lines",
        line=dict(dash="dash", color="gray", width=1),
        showlegend=False,
    ))
    _fig.update_layout(
        title=f"Admin 1: CHD vs GDACS — {_storm_name}",
        xaxis_title="CHD observed",
        yaxis_title="GDACS",
        template="simple_white",
        height=450,
        legend=dict(orientation="h", y=-0.15),
    )
    mo.ui.plotly(_fig)
    return


if __name__ == "__main__":
    app.run()
