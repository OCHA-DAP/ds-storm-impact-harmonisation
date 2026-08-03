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
from datetime import datetime

import ocha_stratus as stratus
import pandas as pd
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
) -> str:
    """Render one active-storm block with a country strip chart per affected iso3."""
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
            country_blocks.append(
                _country_block_html(
                    country=row["country"],
                    iso3=row["iso3"],
                    buffer=buffer,
                    current_pop=row["pop_affected"],
                    png_data_uri=png_to_data_uri(png),
                )
            )
        body = "\n".join(country_blocks)

    return (
        "<div style='border-left: 3px solid #d9534f; padding-left: 14px;"
        " margin: 20px 0;'>"
        + _storm_header_html(storm)
        + body
        + "</div>"
    )


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
    )


def build_email_html(
    active_storms: pd.DataFrame,
    exposure: pd.DataFrame,
    historical: pd.DataFrame,
    timestamp: datetime,
    buffer: str = "buffer39",
) -> str:
    parts = [
        "<html><body style='font-family: -apple-system, BlinkMacSystemFont,"
        " \"Segoe UI\", sans-serif; max-width: 720px; margin: 0 auto;"
        " padding: 24px; color: #222;'>",
        _header_html(timestamp, len(active_storms)),
    ]
    for _, storm in active_storms.iterrows():
        sub = exposure[exposure["eventid"] == storm["eventid"]]
        parts.append(_storm_section_html(storm, sub, historical, buffer=buffer))
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
