"""
GDACS Storm Monitor.

Fetches current storm exposure and compares against historical storms
that hit the same countries. Produces a context plot showing where the
current storm ranks.

Usage:
    uv run python src/monitor.py
    uv run python src/monitor.py --build-history   # cache historical data
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import time

import ocha_stratus as stratus
import pandas as pd
import plotly.graph_objects as go
from dotenv import load_dotenv

from src.datasets.gdacs import get_active_cyclones, get_impact_by_country

load_dotenv()

GDACS_HIST_BLOB = (
    "ds-cyclone-exposure/gdacs_historical_national_exposure.csv"
)
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds


# -- Helpers ------------------------------------------------------------------


def _fetch_exposure_for_events(
    events: pd.DataFrame,
) -> pd.DataFrame:
    """Fetch per-country pop_affected for a set of events.

    Returns long-form DataFrame: one row per event-country-buffer.
    """
    rows = []
    for _, ev in events.iterrows():
        eid = ev["eventid"]
        print(f"  {ev['name']} (id={eid})...", end=" ", flush=True)
        impact = None
        for attempt in range(MAX_RETRIES):
            try:
                impact = get_impact_by_country(eid, aggregate=True)
                break
            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
                else:
                    print(f"FAILED after {MAX_RETRIES} attempts: {e}")
        if impact is None:
            continue

        has_data = False
        for buf_name, df_buf in impact.items():
            if df_buf.empty:
                continue
            has_data = True
            for _, row in df_buf.iterrows():
                rows.append(
                    {
                        "eventid": eid,
                        "name": ev["name"],
                        "alert_level": ev["alert_level"],
                        "is_current": ev.get("is_current", False),
                        "from_date": ev["from_date"],
                        "iso3": row["iso3"],
                        "country": row["country"],
                        "buffer": buf_name,
                        "pop_affected": row["pop_affected"],
                    }
                )
        print("OK" if has_data else "no data")

    return pd.DataFrame(rows)


# -- Step 1: Current exposure ------------------------------------------------


def get_current_exposure(
    target_date: str | None = None,
) -> pd.DataFrame:
    """Fetch per-country pop affected for active storms.

    Parameters
    ----------
    target_date : str "YYYY-MM-DD", optional
        If provided, fetch storms active around that date instead
        of today. Useful for backtesting against historical data.
    """
    if target_date:
        events = get_active_cyclones(
            from_date=target_date, to_date=target_date
        )
        if events.empty:
            print(f"No cyclones found for {target_date}.")
            return pd.DataFrame()
        print(
            f"Storms near {target_date}:"
            f" {', '.join(events['name'])}"
        )
        return _fetch_exposure_for_events(events)

    events = get_active_cyclones()
    active = events[events["is_current"]]
    if active.empty:
        print("No currently active cyclones.")
        return pd.DataFrame()

    print(f"Active storms: {', '.join(active['name'])}")
    return _fetch_exposure_for_events(active)


# -- Step 2: Historical baseline ---------------------------------------------


def load_historical() -> pd.DataFrame:
    """Load pre-compiled GDACS historical exposure from blob storage.

    Source: ds-cyclone-exposure/gdacs_historical_national_exposure.csv
    Columns: sid, iso3, season, from_date, pop_34kt, pop_64kt

    Reshapes to long form matching the current-exposure format:
        name, iso3, country, buffer, pop_affected
    """
    df = stratus.load_csv_from_blob(GDACS_HIST_BLOB)
    print(f"Loaded {len(df)} historical rows from blob")

    # Reshape wide (pop_34kt, pop_64kt) to long (buffer, pop_affected)
    rows = []
    for _, row in df.iterrows():
        storm_name = row.get("name", "")
        season = row.get("season", "")
        base = {
            "name": storm_name,
            "label": f"{storm_name}-{str(season)[-2:]}"
            if season
            else storm_name,
            "eventid": row.get("event_id"),
            "iso3": row.get("iso3", ""),
            "country": row.get("country_name", ""),
            "from_date": row.get("from_date", ""),
        }
        for col, buf in [
            ("pop_34kt", "buffer39"),
            ("pop_64kt", "buffer74"),
        ]:
            val = row.get(col)
            if pd.notna(val) and val > 0:
                rows.append(
                    {**base, "buffer": buf, "pop_affected": int(val)}
                )

    result = pd.DataFrame(rows)
    print(
        f"  {result['name'].nunique()} storms,"
        f" {result['iso3'].nunique()} countries"
    )
    return result


# -- Step 3: Context plot ----------------------------------------------------


def _country_strip(
    country: str,
    current_pop: int,
    storm_name: str,
    hist_country: pd.DataFrame,
    kt_label: str,
) -> go.Figure:
    """Single-country strip chart: current storm vs historical."""
    fig = go.Figure()
    annotations = []

    h = hist_country.sort_values("pop_affected").reset_index(
        drop=True
    )
    n_hist = len(h)

    # Historical dots
    if not h.empty:
        fig.add_trace(
            go.Scatter(
                x=h["pop_affected"],
                y=[0] * n_hist,
                mode="markers",
                marker=dict(
                    size=8, color="#7f8c8d", opacity=0.7
                ),
                text=h.get("label", h["name"]),
                hovertemplate=(
                    "<b>%{text}</b><br>"
                    "%{x:,.0f}<extra></extra>"
                ),
                showlegend=False,
            )
        )
        for i, (_, row) in enumerate(h.iterrows()):
            yshift = 18 if i % 2 == 0 else -18
            label = row.get("label", row["name"])
            annotations.append(
                dict(
                    x=row["pop_affected"],
                    y=0,
                    text=label,
                    showarrow=True,
                    arrowhead=0,
                    arrowwidth=0.5,
                    arrowcolor="#ccc",
                    ax=0,
                    ay=yshift * -1.5,
                    font=dict(size=10, color="#555"),
                    bgcolor="rgba(255,255,255,0.7)",
                    borderpad=1,
                )
            )

    # Current storm
    fig.add_trace(
        go.Scatter(
            x=[current_pop],
            y=[0],
            mode="markers",
            marker=dict(
                size=16,
                color="#d9534f",
                symbol="diamond",
                line=dict(width=1, color="#333"),
            ),
            hovertemplate=(
                f"<b>{storm_name}</b><br>"
                f"{current_pop:,}<extra></extra>"
            ),
            name=storm_name,
        )
    )
    annotations.append(
        dict(
            x=current_pop,
            y=0,
            text=f"<b>{storm_name}</b>",
            showarrow=True,
            arrowhead=0,
            arrowwidth=0.8,
            arrowcolor="#d9534f",
            ax=0,
            ay=30,
            font=dict(size=11, color="#d9534f"),
            bgcolor="rgba(255,255,255,0.8)",
            borderpad=2,
        )
    )

    fig.update_layout(
        title=dict(
            text=(
                f"<b>{country}</b> -- pop exposed at"
                f" {kt_label} ({n_hist} historical storms)"
            ),
            font=dict(size=13),
        ),
        xaxis=dict(
            title="Cumulative population affected",
            tickformat=",",
            gridcolor="#eee",
            rangemode="tozero",
        ),
        yaxis=dict(
            visible=False,
            range=[-0.8, 0.8],
        ),
        annotations=annotations,
        height=160,
        margin=dict(l=20, t=40, b=40, r=20),
        plot_bgcolor="white",
        showlegend=False,
    )
    return fig


def plot_storm_context(
    current: pd.DataFrame,
    historical: pd.DataFrame,
    buffer: str = "buffer39",
) -> dict[str, go.Figure]:
    """One strip chart per affected country.

    Returns dict mapping country name to a plotly Figure.
    """
    cur = current[current["buffer"] == buffer].copy()
    hist = historical[historical["buffer"] == buffer].copy()

    if cur.empty:
        return {}

    storm_name = cur["name"].iloc[0]
    kt = "34 kt" if buffer == "buffer39" else "64 kt"

    figs = {}
    for _, row in cur.sort_values(
        "pop_affected", ascending=False
    ).iterrows():
        country = row["country"]
        iso3 = row["iso3"]
        pop = row["pop_affected"]

        h = hist[
            (hist["iso3"] == iso3) & (hist["name"] != storm_name)
        ]
        figs[country] = _country_strip(
            country, pop, storm_name, h, kt
        )

    return figs


# -- Main --------------------------------------------------------------------


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="GDACS storm monitor"
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Backtest date YYYY-MM-DD (default: live/current)",
    )
    args = parser.parse_args()

    label = f"date={args.date}" if args.date else "live"
    print(f"=== Current exposure ({label}) ===")
    current = get_current_exposure(target_date=args.date)
    if current.empty:
        print("No current exposure data. Exiting.")
        sys.exit(0)

    print(f"\n{len(current)} rows:")
    print(
        current.groupby(["name", "buffer"])["pop_affected"]
        .sum()
        .apply(lambda x: f"{x:,.0f}")
        .to_string()
    )

    print("\n=== Historical baseline ===")
    historical = load_historical()

    print("\n=== Generating plots ===")
    out_dir = Path("artefacts")
    out_dir.mkdir(exist_ok=True)

    for storm_name in current["name"].unique():
        storm_df = current[current["name"] == storm_name]
        storm_slug = storm_name.replace(" ", "_")
        for buf in ["buffer39", "buffer74"]:
            figs = plot_storm_context(
                storm_df, historical, buffer=buf
            )
            for country, fig in figs.items():
                iso3 = storm_df[
                    storm_df["country"] == country
                ]["iso3"].iloc[0]
                outfile = (
                    out_dir
                    / f"monitor_{storm_slug}_{buf}_{iso3}.html"
                )
                fig.write_html(str(outfile))
                print(f"  {outfile}")


if __name__ == "__main__":
    main()
