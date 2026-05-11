import marimo

__generated_with = "0.23.5"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import ocha_stratus as stratus
    import geopandas as gpd
    import pandas as pd
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
    from dotenv import load_dotenv
    from sqlalchemy import text

    load_dotenv()
    return gpd, mo, mpatches, pd, plt, stratus, text


@app.cell
def _(gpd):
    _world = gpd.read_file(
        "https://naturalearth.s3.amazonaws.com/110m_cultural/ne_110m_admin_0_countries.zip"
    )
    world_outlines = _world[["ADM0_ISO", "geometry"]].rename(columns={"ADM0_ISO": "iso3"})
    return (world_outlines,)


@app.cell
def _(mo, pd, stratus, text):
    _engine = stratus.get_engine(stage="dev")
    with _engine.connect() as _conn:
        _df = pd.read_sql(
            text("SELECT iso3, name FROM public.polygon WHERE adm_level = 0 ORDER BY name"),
            _conn,
        )
    _engine.dispose()
    _opts = {f"{r['name']} ({r['iso3']})": r["iso3"] for _, r in _df.iterrows()}
    country_sel = mo.ui.dropdown(_opts, label="Country", searchable=True)
    country_sel
    return (country_sel,)


@app.cell
def _(country_sel, mo, pd, stratus, text):
    mo.stop(not country_sel.value, mo.md("*Select a country above.*"))
    _iso3 = country_sel.value
    _engine = stratus.get_engine(stage="dev")
    with _engine.connect() as _conn:
        _df = pd.read_sql(
            text(
                "SELECT DISTINCT e.atcf_id, COALESCE(s.name, e.atcf_id) AS label"
                " FROM storms.nhc_tracks_fcast_exposure e"
                " LEFT JOIN storms.nhc_storms s ON e.atcf_id = s.atcf_id"
                " WHERE e.pcode = :iso3 AND e.admin_level = 0"
                " ORDER BY e.atcf_id"
            ),
            _conn,
            params={"iso3": _iso3},
        )
    _engine.dispose()
    mo.stop(_df.empty, mo.md(f"*No storm exposure data for {_iso3}.*"))
    _opts = {f"{r.atcf_id} — {r.label}": r.atcf_id for _, r in _df.iterrows()}
    storm_sel = mo.ui.dropdown(_opts, label="Storm", searchable=True)
    storm_sel
    return (storm_sel,)


@app.cell
def _(country_sel, mo, pd, storm_sel, stratus, text):
    mo.stop(not storm_sel.value)
    _iso3 = country_sel.value
    _atcf_id = storm_sel.value
    _engine = stratus.get_engine(stage="dev")
    with _engine.connect() as _conn:
        _times = pd.read_sql(
            text(
                "SELECT DISTINCT issued_time FROM storms.nhc_tracks_fcast_exposure"
                " WHERE atcf_id = :atcf_id AND pcode = :iso3 AND admin_level = 0"
                " ORDER BY issued_time"
            ),
            _conn,
            params={"atcf_id": _atcf_id, "iso3": _iso3},
        )["issued_time"].tolist()
    _engine.dispose()

    _time_opts = {str(t): t for t in _times}
    issued_time_sel = mo.ui.dropdown(
        _time_opts, label="Issued time", value=list(_time_opts.keys())[-1]
    )
    obsv_toggle = mo.ui.checkbox(value=True, label="Observed")
    track_toggle = mo.ui.checkbox(value=True, label="Track buffer")
    track_type = mo.ui.radio({"Full forecast": "fcast", "Fcast-only": "fcastonly"}, value="Full forecast")
    wsp_toggle = mo.ui.checkbox(value=False, label="WSP")
    wsp_kt = mo.ui.radio({"34 kt": 34, "50 kt": 50, "64 kt": 64}, value="34 kt")
    wsp_type = mo.ui.radio({"Full WSP": "full", "Fcast-only WSP": "fcastonly"}, value="Full WSP")

    mo.vstack([
        issued_time_sel,
        mo.hstack([obsv_toggle, mo.md("&nbsp;"), track_toggle, track_type]),
        mo.hstack([wsp_toggle, wsp_kt, wsp_type]),
    ])
    return issued_time_sel, obsv_toggle, track_toggle, track_type, wsp_toggle, wsp_kt, wsp_type


@app.cell
def _(issued_time_sel, mo, gpd, storm_sel, stratus, text):
    """Compute stable map bounds from fcast buffers only — locked per issued_time."""
    mo.stop(not issued_time_sel.value)
    _engine = stratus.get_engine(stage="dev")
    with _engine.connect() as _conn:
        _gdf_bounds = gpd.read_postgis(
            text("SELECT wind_speed_kt, geometry FROM storms.nhc_tracks_fcast_buffers WHERE atcf_id = :a AND issued_time = :it"),
            _conn, geom_col="geometry", params={"a": storm_sel.value, "it": issued_time_sel.value},
        )
    _engine.dispose()
    mo.stop(_gdf_bounds.empty, mo.md("No fcast buffer data for this issued time."))
    _b = _gdf_bounds.total_bounds
    _pad = 3
    map_xlim = (_b[0] - _pad, _b[2] + _pad)
    map_ylim = (_b[1] - _pad, _b[3] + _pad)
    return map_xlim, map_ylim


@app.cell
def _(
    country_sel, gpd, issued_time_sel, map_xlim, map_ylim, mo, mpatches,
    obsv_toggle, pd, plt, storm_sel, stratus, text, track_toggle, track_type,
    world_outlines, wsp_kt, wsp_toggle, wsp_type,
):
    mo.stop(not issued_time_sel.value)
    _iso3 = country_sel.value
    _atcf_id = storm_sel.value
    _it = issued_time_sel.value

    # NHC WSP categorical colour scale — matches standard product
    _WSP_COLORS = {
        0:  "#ffffff",  # white (needs grey outline)
        5:  "#00a000",  # dark green
        10: "#64c832",  # medium green
        20: "#b4e600",  # lime
        30: "#e8dc00",  # yellow
        40: "#c8a832",  # tan
        50: "#a07828",  # brown
        60: "#e06400",  # orange
        70: "#c82800",  # red
        80: "#901828",  # dark red
        90: "#641464",  # purple
    }
    _kt_colors = {34: "#f5c842", 50: "#f5a623", 64: "#e8320a"}

    # Load track buffers (type selected by radio)
    _track_table = "nhc_tracks_fcast_buffers" if track_type.value == "fcast" else "nhc_tracks_fcastonly_buffers"
    _engine = stratus.get_engine(stage="dev")
    with _engine.connect() as _conn:
        _gdf_track = (
            gpd.read_postgis(
                text(f"SELECT wind_speed_kt, geometry FROM storms.{_track_table} WHERE atcf_id = :a AND issued_time = :it"),
                _conn, geom_col="geometry", params={"a": _atcf_id, "it": _it},
            ) if track_toggle.value else gpd.GeoDataFrame()
        )
        _gdf_obsv = (
            gpd.read_postgis(
                text("SELECT wind_speed_kt, geometry FROM storms.nhc_tracks_obsv_buffers WHERE atcf_id = :a AND valid_time = :it"),
                _conn, geom_col="geometry", params={"a": _atcf_id, "it": _it},
            ) if obsv_toggle.value else gpd.GeoDataFrame()
        )
        _wsp_table = "nhc_wsp_polygon" if wsp_type.value == "full" else "nhc_wsp_fcastonly_polygon"
        _gdf_wsp = (
            gpd.read_postgis(
                text(f"SELECT wind_threshold_kt, percentage, geometry FROM storms.{_wsp_table} WHERE issued_time = :it AND wind_threshold_kt = :kt ORDER BY percentage DESC"),
                _conn, geom_col="geometry", params={"it": _it, "kt": wsp_kt.value},
            ) if wsp_toggle.value else gpd.GeoDataFrame()
        )
    _engine.dispose()

    def _valid(gdf):
        return gdf[gdf.geometry.apply(lambda g: g is not None and not g.is_empty)]

    _fig, _ax = plt.subplots(figsize=(9, 7), dpi=130)
    _ax.set_facecolor("white")
    _ax.set_aspect("auto")

    # World land fill (light grey) then country black outline on top
    _world_fill = world_outlines.copy()
    _world_fill.plot(ax=_ax, color="#f0f0f0", edgecolor="none", linewidth=0, aspect=None, zorder=1)
    _country_geom = world_outlines[world_outlines["iso3"] == _iso3]
    if not _country_geom.empty:
        _country_geom.boundary.plot(ax=_ax, color="black", linewidth=1.0, aspect=None, zorder=5)

    # WSP: one wind speed, categorical colour per probability band, low→high prob plotted first
    if wsp_toggle.value and not _gdf_wsp.empty:
        for _, _wr in _valid(_gdf_wsp).sort_values("percentage").iterrows():
            _pct = int(_wr["percentage"])
            _c = _WSP_COLORS.get(_pct, "#ffffff")
            _ec = "#888888" if _pct == 0 else "none"
            gpd.GeoSeries([_wr.geometry], crs=4326).plot(
                ax=_ax, color=_c, alpha=0.7, linewidth=0.6 if _pct == 0 else 0,
                edgecolor=_ec, aspect=None, zorder=2
            )

    # Track buffers (one type at a time)
    for _kt in [34, 50, 64]:
        _color = _kt_colors[_kt]
        if track_toggle.value and not _gdf_track.empty:
            _row = _valid(_gdf_track[_gdf_track["wind_speed_kt"] == _kt])
            if not _row.empty:
                _row.plot(ax=_ax, color=_color, alpha=0.4, linewidth=0, aspect=None, zorder=3)
        if obsv_toggle.value and not _gdf_obsv.empty:
            _row = _valid(_gdf_obsv[_gdf_obsv["wind_speed_kt"] == _kt])
            if not _row.empty:
                _row.plot(ax=_ax, color=_color, alpha=0.3, linewidth=0, aspect=None, zorder=3)

    _ax.set_xlim(*map_xlim)
    _ax.set_ylim(*map_ylim)
    _ax.set_xlabel("Longitude")
    _ax.set_ylabel("Latitude")
    _ax.set_title(
        f"{_atcf_id}  |  issued {_it}"
        + (f"  |  {track_type.value} track" if track_toggle.value else "")
        + (f"  |  WSP {wsp_kt.value}kt ({wsp_type.value})" if wsp_toggle.value else ""),
        fontsize=10,
    )
    _ax.grid(True, linewidth=0.3, alpha=0.3, zorder=0)

    _legend_handles = [mpatches.Patch(color=_kt_colors[_kt], label=f"{_kt} kt") for _kt in [34, 50, 64]]
    if track_toggle.value:
        _label = "Fcast-only buffer" if track_type.value == "fcastonly" else "Forecast buffer"
        _legend_handles.append(mpatches.Patch(color="grey", alpha=0.4, label=_label))
    if obsv_toggle.value:
        _legend_handles.append(mpatches.Patch(color="grey", alpha=0.3, label="Observed buffer"))
    _ax.legend(handles=_legend_handles, loc="lower left", fontsize=7, framealpha=0.8)

    # WSP probability colour strip along the bottom
    if wsp_toggle.value:
        _bands = sorted(_WSP_COLORS.keys())
        _n = len(_bands)
        _strip_ax = _fig.add_axes([0.12, 0.01, 0.78, 0.03])
        _strip_ax.set_xlim(0, _n)
        _strip_ax.set_ylim(0, 1)
        _strip_ax.axis("off")
        for _i, _pct in enumerate(_bands):
            _c = _WSP_COLORS[_pct]
            _ec = "#888888" if _pct == 0 else "none"
            _strip_ax.add_patch(mpatches.FancyBboxPatch(
                (_i, 0.2), 1, 0.6, boxstyle="square,pad=0",
                facecolor=_c, edgecolor=_ec, linewidth=0.8
            ))
            _strip_ax.text(_i + 0.5, -0.1, f"{_pct}%" if _pct > 0 else "0",
                           ha="center", va="top", fontsize=6.5, color="black")
        _strip_ax.text(_n + 0.1, 0.5, "%", ha="left", va="center", fontsize=7)
        _wsp_label = "Full WSP" if wsp_type.value == "full" else "Fcast-only WSP"
        _strip_ax.set_title(f"{_wsp_label}  {wsp_kt.value} kt  —  probability band", fontsize=7, pad=2)

    plt.tight_layout()
    _fig


@app.cell
def _(country_sel, issued_time_sel, mo, pd, plt, storm_sel, stratus, text):
    mo.stop(not storm_sel.value)
    _iso3 = country_sel.value
    _atcf_id = storm_sel.value
    _it = issued_time_sel.value
    _engine = stratus.get_engine(stage="dev")
    with _engine.connect() as _conn:
        _fcast = pd.read_sql(
            text("SELECT issued_time, wind_speed_kt, pop_exposed FROM storms.nhc_tracks_fcast_exposure WHERE atcf_id = :a AND pcode = :p AND admin_level = 0 ORDER BY issued_time"),
            _conn, params={"a": _atcf_id, "p": _iso3},
        )
        _obsv = pd.read_sql(
            text("SELECT valid_time AS issued_time, wind_speed_kt, pop_exposed FROM storms.nhc_tracks_obsv_exposure WHERE atcf_id = :a AND pcode = :p AND admin_level = 0 ORDER BY valid_time"),
            _conn, params={"a": _atcf_id, "p": _iso3},
        )
        _fcastonly = pd.read_sql(
            text("SELECT issued_time, wind_speed_kt, pop_exposed FROM storms.nhc_tracks_fcastonly_exposure WHERE atcf_id = :a AND pcode = :p AND admin_level = 0 ORDER BY issued_time"),
            _conn, params={"a": _atcf_id, "p": _iso3},
        )
    _engine.dispose()

    _kt_colors = {34: "#f5c842", 50: "#f5a623", 64: "#e8320a"}
    _fig, _axes = plt.subplots(3, 1, figsize=(10, 7), sharex=True, dpi=130)
    _fig.suptitle(f"Track exposure — {_atcf_id}  ({_iso3})", fontsize=11)
    for _i, (_kt, _ax) in enumerate(zip([34, 50, 64], _axes)):
        _color = _kt_colors[_kt]
        _f = _fcast[_fcast["wind_speed_kt"] == _kt]
        _o = _obsv[_obsv["wind_speed_kt"] == _kt]
        _fo = _fcastonly[_fcastonly["wind_speed_kt"] == _kt]
        if not _f.empty:
            _ax.plot(_f["issued_time"], _f["pop_exposed"], "-o", color=_color, markersize=3, linewidth=1.2, label="Forecast")
        if not _o.empty:
            _ax.plot(_o["issued_time"], _o["pop_exposed"], "--s", color=_color, markersize=3, linewidth=1.2, alpha=0.8, label="Observed")
        if not _fo.empty:
            _ax.plot(_fo["issued_time"], _fo["pop_exposed"], ":^", color=_color, markersize=3, linewidth=1.2, alpha=0.8, label="Fcast-only")
        if _it:
            _ax.axvline(x=_it, color="black", linestyle="--", linewidth=0.8, alpha=0.6)
        _ax.set_ylabel("Pop. exposed", fontsize=8)
        _ax.set_title(f"{_kt} kt", fontsize=9, loc="left")
        _ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x/1e6:.1f}M" if x >= 1e6 else f"{x/1e3:.0f}k"))
        _ax.grid(True, linewidth=0.4, alpha=0.5)
        if _i == 0:
            _ax.legend(fontsize=8, loc="upper right")
    _axes[-1].tick_params(axis="x", rotation=25)
    plt.tight_layout()
    _fig


@app.cell
def _(country_sel, issued_time_sel, mo, pd, plt, storm_sel, stratus, text):
    mo.stop(not issued_time_sel.value)
    _iso3 = country_sel.value
    _atcf_id = storm_sel.value
    _it = issued_time_sel.value
    _engine = stratus.get_engine(stage="dev")
    with _engine.connect() as _conn:
        # Full WSP exposure at this issued_time
        _wsp_it = pd.read_sql(
            text("SELECT wind_threshold_kt, percentage, pop_exposed FROM storms.nhc_wsp_exposure WHERE atcf_id = :a AND pcode = :p AND admin_level = 0 AND issued_time = :it ORDER BY wind_threshold_kt, percentage"),
            _conn, params={"a": _atcf_id, "p": _iso3, "it": _it},
        )
        # Fcast-only WSP at this issued_time
        _wsp_fo_it = pd.read_sql(
            text("SELECT wind_threshold_kt, percentage, pop_exposed FROM storms.nhc_wsp_fcastonly_exposure WHERE atcf_id = :a AND pcode = :p AND admin_level = 0 AND issued_time = :it ORDER BY wind_threshold_kt, percentage"),
            _conn, params={"a": _atcf_id, "p": _iso3, "it": _it},
        )
        # Observed track exposure at this issued_time (valid_time)
        _obsv_it = pd.read_sql(
            text("SELECT wind_speed_kt, pop_exposed FROM storms.nhc_tracks_obsv_exposure WHERE atcf_id = :a AND pcode = :p AND admin_level = 0 AND valid_time = :it"),
            _conn, params={"a": _atcf_id, "p": _iso3, "it": _it},
        )
    _engine.dispose()

    mo.stop(
        _wsp_it.empty and _wsp_fo_it.empty,
        mo.md(f"*No WSP data for {_atcf_id} / {_iso3} at issued time {_it}.*"),
    )

    # Build obsv lookup: wind_speed_kt → pop_exposed (0 if not present)
    _obsv_lookup = {int(r.wind_speed_kt): int(r.pop_exposed) for _, r in _obsv_it.iterrows()}

    # Band width in probability (as fraction summing to 1)
    _BAND_WIDTH_FRAC = {0: 0.05, 5: 0.05, 10: 0.10, 20: 0.10, 30: 0.10,
                        40: 0.10, 50: 0.10, 60: 0.10, 70: 0.10, 80: 0.10, 90: 0.10}

    def _pdf_bars(ax, df, color, x_offset=0):
        """Proper PDF: x = cumulative pop (high→low prob), y = probability density.
        Bars sorted highest probability first so most-certain exposure is leftmost.
        Area of each bar = band_width_frac → total area ≈ 1.
        """
        _cum = x_offset
        for _, _r in df.sort_values("percentage", ascending=False).iterrows():
            _pct = int(_r["percentage"])
            _pop = _r["pop_exposed"]
            _bw = _BAND_WIDTH_FRAC.get(_pct, 0.05)
            if _pop > 0:
                _density = _bw / _pop
                ax.bar(_cum, _density, width=_pop, align="edge",
                       color=color, alpha=0.75, edgecolor="white", linewidth=0.5)
                ax.text(_cum + _pop / 2, _density, f"{_pct}%",
                        ha="center", va="bottom", fontsize=6, color="black", alpha=0.7)
            _cum += _pop

    _fmt_pop = plt.FuncFormatter(lambda x, _: f"{x/1e6:.1f}M" if x >= 1e6 else f"{x/1e3:.0f}k")
    _kt_colors = {34: "#f5c842", 50: "#f5a623", 64: "#e8320a"}
    _fig, _axes = plt.subplots(2, 3, figsize=(13, 7), dpi=130)
    _fig.suptitle(
        f"Exposure PDF  |  {_atcf_id}  ({_iso3})  |  {_it}\n"
        "x = cumulative population (high→low certainty)  ·  y = probability density  ·  area ≈ 1",
        fontsize=9,
    )

    for _col, _kt in enumerate([34, 50, 64]):
        _color = _kt_colors[_kt]
        _w = _wsp_it[_wsp_it["wind_threshold_kt"] == _kt]
        _wfo = _wsp_fo_it[_wsp_fo_it["wind_threshold_kt"] == _kt]
        _obsv_val = _obsv_lookup.get(_kt, 0)

        # Row 0: full WSP PDF
        _ax0 = _axes[0, _col]
        if not _w.empty:
            _pdf_bars(_ax0, _w, _color)
        _ax0.set_title(f"{_kt} kt — WSP full", fontsize=9)
        _ax0.set_xlabel("Population exposed", fontsize=8)
        _ax0.set_ylabel("Probability density", fontsize=8)
        _ax0.set_xlim(left=0)
        _ax0.set_ylim(bottom=0)
        _ax0.xaxis.set_major_formatter(_fmt_pop)
        _ax0.grid(True, axis="y", linewidth=0.4, alpha=0.5)

        # Row 1: obsv (certain) + fcast-only WSP PDF, x offset by obsv_val
        _ax1 = _axes[1, _col]
        if _obsv_val > 0:
            _ax1.axvline(_obsv_val, color=_color, linestyle="--", linewidth=1.2,
                         alpha=0.8, label=f"Observed floor\n({_fmt_pop(_obsv_val, None)})")
            _ax1.axvspan(0, _obsv_val, color=_color, alpha=0.08)
        if not _wfo.empty:
            _pdf_bars(_ax1, _wfo, _color, x_offset=_obsv_val)
        _ax1.set_title(f"{_kt} kt — obsv + fcast-only WSP", fontsize=9)
        _ax1.set_xlabel("Population exposed", fontsize=8)
        _ax1.set_ylabel("Probability density", fontsize=8)
        _ax1.set_xlim(left=0)
        _ax1.set_ylim(bottom=0)
        _ax1.xaxis.set_major_formatter(_fmt_pop)
        _ax1.grid(True, axis="y", linewidth=0.4, alpha=0.5)
        if _obsv_val > 0:
            _ax1.legend(fontsize=7)

    plt.tight_layout()
    _fig


if __name__ == "__main__":
    app.run()
