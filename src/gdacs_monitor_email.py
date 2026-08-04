"""Rendering helpers for the daily GDACS monitor email.

Produces per-country strip-chart PNGs (plotnine) and assembles an HTML
email body that embeds them as base64 data URIs so the email is
self-contained.

Historical baseline: OCHA in-house IBTrACS-based exposure parquet
(global, 2001+). This does not match GDACS's live methodology exactly
(see book chapter 07); good enough for wireframe monitoring.
"""

from __future__ import annotations

import base64
import io
from datetime import datetime, timezone

import matplotlib
import ocha_stratus as stratus
import pandas as pd

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from plotnine import (
    aes,
    element_blank,
    element_text,
    geom_point,
    ggplot,
    labs,
    scale_color_manual,
    scale_size_manual,
    scale_x_log10,
    theme,
    theme_minimal,
)

OCHA_HISTORICAL_BLOB = (
    "ds-storm-impact-harmonisation/processed/adm0_ibtracs_exp_all.parquet"
)
BUFFER_TO_SPEED = {"buffer39": 34, "buffer74": 64}

# Sequential blue ramp, light -> dark = minor -> widespread. Severity is an
# ORDERED scale, so it gets a one-hue ramp rather than categorical hues.
PDC_SEVERITY_COLORS = {"1": "#b7d3f6", "2": "#3987e5", "3": "#164a8c"}
PDC_SEVERITY_ORDER = ["3", "2", "1"]
BUFFER_TO_LABEL = {"buffer39": "34 kt (TS)", "buffer74": "64 kt (hurricane)"}


# ----------------------------------------------------------------------------
# Historical baseline
# ----------------------------------------------------------------------------


def load_ocha_historical() -> pd.DataFrame:
    """Load OCHA's global storm-country exposure parquet.

    Returns long-form DataFrame: speed, sid, iso3, pop_exposed, year.
    """
    df = stratus.load_parquet_from_blob(OCHA_HISTORICAL_BLOB)
    df = df.rename(columns={"ADM0_A3": "iso3"})
    df["year"] = df["sid"].str[:4].astype(int)
    return df[["speed", "sid", "iso3", "pop_exposed", "year"]]


# ----------------------------------------------------------------------------
# Strip plot (plotnine -> PNG bytes)
# ----------------------------------------------------------------------------


def build_strip_png(
    iso3: str,
    current_pop: int,
    current_label: str,
    historical: pd.DataFrame,
    speed_kt: int,
) -> bytes:
    """Single strip chart: current storm marker vs historical points.

    Parameters
    ----------
    iso3 : ISO3 country code, used to filter historical data.
    current_pop : population affected by the current storm (x-axis).
    current_label : short label for the current storm (e.g. 'MELISSA-25').
    historical : long-form DataFrame with columns speed, sid, iso3, pop_exposed.
    speed_kt : 34 or 64; filters historical rows.
    """
    hist = historical[
        (historical["iso3"] == iso3)
        & (historical["speed"] == speed_kt)
        & (historical["pop_exposed"] > 0)
    ].copy()
    hist["kind"] = "historical"
    hist["label"] = hist["sid"].str[:4]

    current_row = pd.DataFrame(
        [{
            "pop_exposed": max(1, int(current_pop)),
            "kind": "current",
            "label": current_label,
        }]
    )
    data = pd.concat([hist[["pop_exposed", "kind", "label"]], current_row],
                     ignore_index=True)
    data["y"] = 0

    plot = (
        ggplot(data, aes(x="pop_exposed", y="y",
                         color="kind", size="kind"))
        + geom_point(alpha=0.7)
        + scale_x_log10(
            labels=lambda lst: [f"{int(v):,}" for v in lst],
        )
        + scale_color_manual(
            values={"historical": "#7f8c8d", "current": "#d9534f"}
        )
        + scale_size_manual(values={"historical": 2.5, "current": 6})
        + theme_minimal()
        + theme(
            axis_text_y=element_blank(),
            axis_title_y=element_blank(),
            legend_position="none",
            plot_title=element_text(size=10),
            figure_size=(6, 1.3),
        )
        + labs(
            title=f"{iso3} at {speed_kt} kt  (n historical = {len(hist)})",
            x="pop exposed (log)",
        )
    )

    buf = io.BytesIO()
    plot.save(buf, format="png", width=6, height=1.3, dpi=110, verbose=False)
    return buf.getvalue()


def png_to_data_uri(png: bytes) -> str:
    b64 = base64.b64encode(png).decode("ascii")
    return f"data:image/png;base64,{b64}"


# ----------------------------------------------------------------------------
# HTML body assembly
# ----------------------------------------------------------------------------


# ----------------------------------------------------------------------------
# PDC panels (optional — omitted entirely when PDC has no match for a storm)
# ----------------------------------------------------------------------------


def build_severity_png(bands: pd.DataFrame) -> bytes:
    """Horizontal stacked bar of PDC damage-class share for one country.

    `bands` is `pdc.parse_exposure_bands()` output: level, description,
    pop_total. Bands are a modelled damage ratio, NOT wind thresholds — they
    cannot be mapped onto 34/50/64 kt (see docs/pdc_api.md).
    """
    total = float(bands["pop_total"].sum())
    if total <= 0:
        raise ValueError("no PDC severity mass to plot")

    fig, ax = plt.subplots(figsize=(6, 0.62), dpi=110)
    left = 0.0
    for lvl in PDC_SEVERITY_ORDER:
        row = bands[bands["level"] == lvl]
        if row.empty:
            continue
        val = float(row["pop_total"].iloc[0])
        if val <= 0:
            continue
        frac = val / total
        ax.barh(0, frac, left=left, height=0.55,
                color=PDC_SEVERITY_COLORS[lvl], edgecolor="white", linewidth=1.4)
        if frac > 0.16:  # only label when it fits
            ax.text(left + frac / 2, 0, f"{frac * 100:.0f}%", ha="center",
                    va="center", color="white", fontsize=9, fontweight="bold")
        left += frac

    ax.set_xlim(0, 1)
    ax.set_ylim(-0.5, 0.5)
    ax.axis("off")
    fig.subplots_adjust(left=0.01, right=0.99, top=0.98, bottom=0.02)
    buf = io.BytesIO()
    fig.savefig(buf, format="png", transparent=True)
    plt.close(fig)
    return buf.getvalue()


def _pdc_country_html(
    bands: pd.DataFrame, exposure_row: pd.Series, country: str = ""
) -> str:
    """Severity bar + the facts GDACS carries no equivalent for."""
    total = float(bands["pop_total"].sum())
    if total <= 0:
        return ""
    top = bands[bands["level"] == "3"]["pop_total"].sum()
    share = top / total * 100

    try:
        bar = png_to_data_uri(build_severity_png(bands))
    except ValueError:
        return ""

    facts = []
    for label, key, fmt in (
        ("hospitals", "capital_hospital", "{:,.0f}"),
        ("schools", "capital_school", "{:,.0f}"),
        ("households", "households", "{:,.0f}"),
        ("vulnerable", "pop_vulnerable", "{:,.0f}"),
    ):
        v = exposure_row.get(key)
        if v is not None and not pd.isna(v) and v > 0:
            facts.append(f"{fmt.format(v)} {label}")

    return (
        "<div style='margin: 6px 0 4px 0; padding: 8px 10px;"
        " background: #f6f8fa; border-radius: 4px;'>"
        "<div style='font-size: 12px; color: #555; margin-bottom: 3px;'>"
        "<b style='color:#164a8c;'>PDC severity</b>"
        + (f" &middot; {country}" if country else "")
        + f" &middot; <b>{share:.0f}%</b> of PDC's {total:,.0f} exposed are in"
        " the widespread-damage class"
        "</div>"
        f"<img src='{bar}' alt='PDC damage-class share'"
        " style='max-width: 100%; display: block;' />"
        + "<div style='font-size: 10.5px; color: #8a8a8a; margin-top: 2px;'>"
        "<span style='color:#164a8c;'>&#9632;</span> widespread damage "
        "&nbsp; <span style='color:#3987e5;'>&#9632;</span> moderate (5% of value) "
        "&nbsp; <span style='color:#b7d3f6;'>&#9632;</span> minor (power out)"
        "</div>"
        + (
            "<div style='font-size: 11px; color: #777; margin-top: 3px;'>"
            + " &middot; ".join(facts) + " in footprint</div>"
            if facts else ""
        )
        + "</div>"
    )


def _landfall_html(meta: dict) -> str:
    """Landfall place / time / category. GDACS has no equivalent field."""
    admin0 = meta.get("landfall_admin0")
    lf_time = meta.get("landfall_time")
    hours = meta.get("hours_landfall")
    cat = meta.get("category_landfall")
    if not admin0 or not lf_time:
        return ""
    try:
        when = datetime.fromtimestamp(float(lf_time), timezone.utc)
        when_s = when.strftime("%d %b %H:%M UTC")
    except (TypeError, ValueError):
        return ""
    bits = [f"<b>{admin0}</b>", when_s]
    if hours:
        bits.append(f"{int(float(hours))} h out")
    if cat:
        bits.append(f"Cat {int(float(cat))} at landfall")
    return (
        "<div style='margin: 8px 0 10px 0; padding: 9px 12px;"
        " border-left: 3px solid #164a8c; background: #eef4fb;"
        " font-size: 13px; color: #22303f;'>"
        "<span style='font-size:11px; letter-spacing:.06em; color:#5a6b7d;'>"
        "PDC LANDFALL FORECAST</span><br/>"
        + " &nbsp;&middot;&nbsp; ".join(bits)
        + "</div>"
    )


def _storm_header_html(storm: pd.Series) -> str:
    alert = str(storm.get("alert_level", "")).capitalize()
    alert_color = {
        "Red": "#d9534f", "Orange": "#f0ad4e", "Green": "#5cb85c",
    }.get(alert, "#888")
    return (
        f"<h2 style='margin-bottom: 4px;'>{storm['name']}</h2>"
        f"<p style='margin: 0 0 12px 0; color: #555;'>"
        f"eventid <code>{storm['eventid']}</code> &middot; "
        f"alert <span style='color: {alert_color}; font-weight: bold;'>"
        f"{alert or 'unknown'}</span> &middot; "
        f"from {storm.get('from_date', '')}"
        f"</p>"
    )


def _country_block_html(
    country: str,
    iso3: str,
    buffer: str,
    current_pop: int,
    png_data_uri: str,
) -> str:
    speed_kt = BUFFER_TO_SPEED[buffer]
    return (
        f"<div style='margin: 10px 0 16px 0;'>"
        f"<div style='font-size: 14px;'>"
        f"<b>{country}</b> <span style='color:#888;'>({iso3})</span>"
        f" &middot; <b>{int(current_pop):,}</b> exposed at {speed_kt} kt"
        f"</div>"
        f"<img src='{png_data_uri}' alt='{iso3} {speed_kt}kt strip plot'"
        f" style='max-width: 100%; display: block; margin-top: 4px;' />"
        f"</div>"
    )


def _storm_section_html(
    storm: pd.Series,
    storm_exposure: pd.DataFrame,
    historical: pd.DataFrame,
    buffer: str = "buffer39",
    pdc: dict | None = None,
) -> str:
    """Render one active-storm block with a country strip chart per affected iso3.

    `pdc`, when present, is {"meta": dict, "exposure": DataFrame,
    "bands": DataFrame} for the matched PDC record. It is strictly additive:
    when PDC has no match for this storm, or no exposure for a given country,
    the block renders exactly as it did before PDC existed.
    """
    speed_kt = BUFFER_TO_SPEED[buffer]
    cur = storm_exposure[storm_exposure["buffer"] == buffer].copy()
    cur = cur[cur["pop_affected"] > 0].sort_values(
        "pop_affected", ascending=False
    )
    if cur.empty:
        body = (
            f"<p style='color:#888;'>No population exposed at {speed_kt} kt in "
            "this episode's GDACS footprint.</p>"
        )
    else:
        country_blocks = []
        for _, row in cur.iterrows():
            png = build_strip_png(
                iso3=row["iso3"],
                current_pop=row["pop_affected"],
                current_label=storm["name"],
                historical=historical,
                speed_kt=speed_kt,
            )
            block = _country_block_html(
                country=row["country"],
                iso3=row["iso3"],
                buffer=buffer,
                current_pop=row["pop_affected"],
                png_data_uri=png_to_data_uri(png),
            )
            if pdc is not None:
                block += _pdc_for_iso3(pdc, row["iso3"])
            country_blocks.append(block)
        body = "\n".join(country_blocks)

    landfall = _landfall_html(pdc["meta"]) if pdc else ""
    return (
        "<div style='border-left: 3px solid #d9534f; padding-left: 14px;"
        " margin: 20px 0;'>"
        + _storm_header_html(storm)
        + landfall
        + body
        + "</div>"
    )


def _pdc_for_iso3(pdc: dict, iso3: str) -> str:
    """PDC severity panel for one country, or '' when PDC has nothing for it."""
    bands = pdc["bands"]
    exp = pdc["exposure"]
    b = bands[bands["iso3"] == iso3]
    e = exp[exp["iso3"] == iso3]
    if b.empty or e.empty or float(b["pop_total"].sum()) <= 0:
        return ""
    return _pdc_country_html(b, e.iloc[0], country=e.iloc[0].get("admin0") or iso3)


def _header_html(ts: datetime, n_storms: int) -> str:
    return (
        f"<h1 style='color: #2c3e50; border-bottom: 2px solid #eee;"
        f" padding-bottom: 8px;'>GDACS Monitor &middot; {ts:%Y-%m-%d %H:%M} UTC</h1>"
        f"<p style='color: #555;'>"
        f"{n_storms} active tropical cyclone{'s' if n_storms != 1 else ''} in GDACS."
        f"</p>"
    )


def _footer_html() -> str:
    return (
        "<hr style='border: none; border-top: 1px solid #eee; margin-top: 30px;' />"
        "<p style='color: #888; font-size: 12px;'>"
        "<b>Methodology note.</b> Current storm values come from GDACS's "
        "live impact endpoint (buffer39 / buffer74, symmetric max-radius "
        "corridor). Historical baseline dots are the OCHA in-house "
        "IBTrACS-based exposure product (asymmetric wind-radii polygons, "
        "2001 onward, global). The two use slightly different wind-field "
        "reconstructions and track-phase filters, so the comparison is "
        "indicative rather than strict. See chapter 07 of the book "
        "for details."
        "</p>"
        "<p style='color: #888; font-size: 12px;'>"
        "<b>PDC panels.</b> Severity classes and landfall come from PDC's "
        "Hazards API, computed by the KineticCast model (Kinetic Analysis "
        "Corporation) on a 60 arc-second grid from the same JTWC/NHC bulletin "
        "GDACS uses \u2014 position and wind radii are identical between the two "
        "sources. Damage classes are a modelled damage <i>ratio</i>, NOT wind "
        "thresholds, and cannot be mapped onto 34/50/64 kt. PDC is admin-0 "
        "only and has no historical archive, so no return period is shown for "
        "it. Facility counts come from a merged inventory (OpenStreetMap/HOT, "
        "HIFLD, NDPBA, national sources) whose completeness varies by country."
        "</p>"
    )


def build_email_html(
    active_storms: pd.DataFrame,
    exposure: pd.DataFrame,
    historical: pd.DataFrame,
    timestamp: datetime,
    buffer: str = "buffer39",
    pdc_by_event: dict | None = None,
) -> str:
    parts = [
        "<html><body style='font-family: -apple-system, BlinkMacSystemFont,"
        " \"Segoe UI\", sans-serif; max-width: 720px; margin: 0 auto;"
        " padding: 24px; color: #222;'>",
        _header_html(timestamp, len(active_storms)),
    ]
    for _, storm in active_storms.iterrows():
        sub = exposure[exposure["eventid"] == storm["eventid"]]
        pdc = (pdc_by_event or {}).get(storm["eventid"])
        parts.append(
            _storm_section_html(storm, sub, historical, buffer=buffer, pdc=pdc)
        )
    parts.append(_footer_html())
    parts.append("</body></html>")
    return "\n".join(parts)


def build_stub_html(timestamp: datetime) -> str:
    """Placeholder email for days with no active storms."""
    return "\n".join([
        "<html><body style='font-family: -apple-system, BlinkMacSystemFont,"
        " \"Segoe UI\", sans-serif; max-width: 720px; margin: 0 auto;"
        " padding: 24px; color: #222;'>",
        _header_html(timestamp, 0),
        "<p style='color: #555;'>Nothing to report. The monitor will send"
        " again at the next scheduled run.</p>",
        _footer_html(),
        "</body></html>",
    ])
