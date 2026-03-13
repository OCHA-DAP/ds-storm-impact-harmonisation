import marimo

__generated_with = "0.10.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import ocha_stratus as stratus
    import geopandas as gpd
    import pandas as pd
    import matplotlib.pyplot as plt
    from sqlalchemy import text

    return gpd, mo, pd, plt, stratus, text


@app.cell
def _(mo, stratus):
    df_exp_all = stratus.load_parquet_from_blob(
        "ds-storm-impact-harmonisation/processed/adm0_ibtracs_exp_all.parquet"
    )
    mo.output.replace(mo.md(f"✓ Exposure data loaded ({len(df_exp_all):,} rows)"))
    return (df_exp_all,)


@app.cell
def _(gpd, mo):
    gdf_buffers = gpd.read_parquet("data/ibtracs_usa_buffers.parquet")
    mo.output.replace(mo.md(f"✓ Storm buffers loaded ({len(gdf_buffers):,} rows)"))
    return (gdf_buffers,)


@app.cell
def _(gpd, mo):
    adm0 = gpd.read_file(
        "https://naturalearth.s3.amazonaws.com/10m_cultural/ne_10m_admin_0_countries.zip"
    )
    mo.output.replace(mo.md(f"✓ ADM0 boundaries loaded ({len(adm0):,} countries)"))
    return (adm0,)


@app.cell
def _(mo, pd, stratus):
    engine = stratus.get_engine(stage="prod")
    with engine.connect() as _con:
        df_storms = pd.read_sql("SELECT sid, name FROM storms.ibtracs_storms", _con)
    mo.output.replace(mo.md(f"✓ Storm names loaded ({len(df_storms):,} storms)"))
    return df_storms, engine


@app.cell
def _(df_exp_all, df_storms):
    df_exp_named = df_exp_all.merge(df_storms, on="sid", how="left")
    return (df_exp_named,)


@app.cell
def _(adm0, df_exp_all, mo):
    _adm0_in_data = set(df_exp_all["ADM0_A3"].unique())
    _adm0_filtered = adm0[adm0["ADM0_A3"].isin(_adm0_in_data)]
    _country_options = (
        _adm0_filtered[["ADM0_A3", "NAME"]]
        .drop_duplicates("ADM0_A3")
        .sort_values("NAME")
    )
    _country_map = dict(zip(_country_options["NAME"], _country_options["ADM0_A3"]))

    country_selector = mo.ui.dropdown(options=_country_map, label="Country")
    country_selector

    return (country_selector,)


@app.cell
def _(country_selector, df_exp_named, mo, pd):
    selected_adm0 = country_selector.value

    if selected_adm0 is None:
        storm_selector = mo.ui.dropdown(options={}, label="Storm")
    else:
        _country_storms = df_exp_named[df_exp_named["ADM0_A3"] == selected_adm0][
            ["sid", "name"]
        ].drop_duplicates("sid").copy()

        _country_storms["season"] = _country_storms["sid"].str[:4]

        def _label(row):
            season = row["season"]
            sid = row["sid"]
            name = str(row["name"]).strip() if pd.notna(row["name"]) else ""
            if name:
                return f"{name.title()} ({season}) — {sid}"
            return f"({season}) — {sid}"

        _country_storms["label"] = _country_storms.apply(_label, axis=1)
        _country_storms = _country_storms.sort_values(["season", "label"])
        _storm_map = dict(zip(_country_storms["label"], _country_storms["sid"]))
        storm_selector = mo.ui.dropdown(options=_storm_map, label="Storm")

    storm_selector

    return selected_adm0, storm_selector


@app.cell
def _(adm0, engine, gdf_buffers, gpd, mo, plt, selected_adm0, storm_selector, text):
    _GEO_CRS_ANTIMERIDIAN = "+proj=longlat +datum=WGS84 +lon_wrap=180"

    _sid = storm_selector.value

    if _sid is None:
        mo.stop(True, mo.md("Select a country and storm above to view the map."))

    with engine.connect() as _con:
        _tracks = gpd.read_postgis(
            text("SELECT * FROM storms.ibtracs_tracks_geo WHERE sid = :sid"),
            _con,
            geom_col="geometry",
            params={"sid": _sid},
        )

    _buffers = gdf_buffers[gdf_buffers["sid"] == _sid]
    _adm = adm0[adm0["ADM0_A3"] == selected_adm0]

    _minx, _miny, _maxx, _maxy = _adm.total_bounds
    _wrap = _maxx > 160 or _minx < -160

    if _wrap:
        _adm = _adm.to_crs(_GEO_CRS_ANTIMERIDIAN)
        _buffers = _buffers.to_crs(_GEO_CRS_ANTIMERIDIAN).dissolve(by="speed").reset_index()
        _tracks = _tracks.to_crs(_GEO_CRS_ANTIMERIDIAN)

    _country_name = _adm.iloc[0]["NAME"] if not _adm.empty else selected_adm0
    _storm_label = next((k for k, v in storm_selector.options.items() if v == _sid), _sid)

    fig, ax = plt.subplots(dpi=150)

    _adm.boundary.plot(ax=ax, linewidth=0.5, color="k")

    if not _buffers.empty:
        _buffers.plot(column="speed", ax=ax, alpha=0.3, legend=False)
        _minx, _miny, _maxx, _maxy = _buffers.total_bounds
    else:
        _minx, _miny, _maxx, _maxy = _adm.total_bounds

    if not _tracks.empty:
        _tracks.plot(ax=ax, markersize=2, color="red")

    _pad = 5
    ax.set_xlim(_minx - _pad, _maxx + _pad)
    ax.set_ylim(_miny - _pad, _maxy + _pad)
    ax.set_title(f"{_country_name} — {_storm_label}")
    ax.set_axis_off()

    fig


@app.cell
def _(df_exp_all, mo, selected_adm0, storm_selector):
    _sid = storm_selector.value

    if _sid is None:
        mo.stop(True)

    _df = (
        df_exp_all[(df_exp_all["sid"] == _sid) & (df_exp_all["ADM0_A3"] == selected_adm0)]
        [["speed", "pop_exposed"]]
        .sort_values("speed")
        .rename(columns={"speed": "Wind speed (kt)", "pop_exposed": "Population exposed"})
        .reset_index(drop=True)
    )
    _df["Population exposed"] = _df["Population exposed"].map("{:,.0f}".format)

    mo.ui.table(_df, selection=None)
