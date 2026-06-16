"""Three-source exposure distribution comparison (CHD / GDACS / ADAM).

Reads the cached panels from panel.py and produces, for each admin level:
  - a coverage report (which sources are present, on the FULL zero-filled
    panel — this is where join-filled-0 vs computed-0 lives);
  - pairwise magnitude stats on CO-POSITIVE rows (both sources > 0, since
    log/ratio of 0 is undefined) for all three pairs, per threshold;
  - an interactive HTML: pairwise log-log scatter (with y=x) and
    Bland-Altman (log2 ratio vs magnitude) grids.

Strict 3-way framing: all three pairs treated equally. GDACS≈ADAM (ADAM
ingests GDACS upstream) is left to show up as a *result* — expect the
GDACS-ADAM pair to sit near y=x.

Run:  python analysis.py   (after panel.py)
Outputs to out/: stats_adm{0,1}.csv, scatter_adm{0,1}.html
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from scipy.stats import spearmanr

import queries as q

OUT = Path(__file__).parent / "out"
PAIRS = [("chd", "gdacs"), ("chd", "adam"), ("gdacs", "adam")]
LABEL = {"chd": "CHD", "gdacs": "GDACS", "adam": "ADAM"}


# ── coverage: presence combos on the full panel (per threshold) ────────

def coverage(panel: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for kt, g in panel.groupby("wind_speed_kt"):
        combo = (g["chd_present"].astype(int).astype(str) + "/"
                 + g["gdacs_present"].astype(int).astype(str) + "/"
                 + g["adam_present"].astype(int).astype(str))
        vc = combo.value_counts()
        for k, v in vc.items():
            rows.append({"wind_speed_kt": kt, "CHD/GDACS/ADAM": k, "n_units": v})
    return pd.DataFrame(rows)


# ── pairwise magnitude stats on co-positive rows ───────────────────────

def pair_stats(panel: pd.DataFrame) -> pd.DataFrame:
    out = []
    for kt, g in panel.groupby("wind_speed_kt"):
        for a, b in PAIRS:
            m = (g[f"{a}_pop"] > 0) & (g[f"{b}_pop"] > 0)
            sub = g[m]
            n = len(sub)
            if n < 3:
                out.append({"wind_speed_kt": kt, "pair": f"{LABEL[a]}-{LABEL[b]}",
                            "n_copos": n, "spearman": np.nan,
                            "median_log2_ratio": np.nan, "iqr_log2_ratio": np.nan,
                            "pct_within_2x": np.nan})
                continue
            log2r = np.log2(sub[f"{b}_pop"] / sub[f"{a}_pop"])
            rho = spearmanr(sub[f"{a}_pop"], sub[f"{b}_pop"]).statistic
            out.append({
                "wind_speed_kt": kt, "pair": f"{LABEL[a]}-{LABEL[b]}",
                "n_copos": n, "spearman": round(rho, 3),
                "median_log2_ratio": round(log2r.median(), 3),
                "iqr_log2_ratio": round(log2r.quantile(.75) - log2r.quantile(.25), 3),
                "pct_within_2x": round(100 * (log2r.abs() < 1).mean(), 1),
            })
    return pd.DataFrame(out)


# ── figure: scatter + Bland-Altman grids (rows=thresholds, cols=pairs) ──

def make_figure(panel: pd.DataFrame, level: int, kts) -> go.Figure:
    nrows = len(kts)
    fig = make_subplots(
        rows=nrows * 2, cols=3,
        subplot_titles=[f"{LABEL[a]} vs {LABEL[b]} ({kt}kt) — {kind}"
                        for kind in ("scatter", "Bland-Altman")
                        for kt in kts for (a, b) in PAIRS],
        vertical_spacing=0.07, horizontal_spacing=0.07,
    )
    for ri, kt in enumerate(kts):
        g = panel[panel["wind_speed_kt"] == kt]
        for ci, (a, b) in enumerate(PAIRS):
            m = (g[f"{a}_pop"] > 0) & (g[f"{b}_pop"] > 0)
            sub = g[m]
            xa, xb = sub[f"{a}_pop"], sub[f"{b}_pop"]
            txt = sub["atcf_id"] + " " + sub["unit"].astype(str)
            # scatter (log-log) + y=x
            sr = ri + 1
            fig.add_trace(go.Scatter(x=xa, y=xb, mode="markers", text=txt,
                          marker=dict(size=5, opacity=0.5), showlegend=False),
                          row=sr, col=ci + 1)
            if len(sub):
                lo = max(1, min(xa.min(), xb.min()))
                hi = max(xa.max(), xb.max())
                fig.add_trace(go.Scatter(x=[lo, hi], y=[lo, hi], mode="lines",
                              line=dict(dash="dash", color="gray"),
                              showlegend=False), row=sr, col=ci + 1)
            fig.update_xaxes(type="log", title_text=LABEL[a], row=sr, col=ci + 1)
            fig.update_yaxes(type="log", title_text=LABEL[b], row=sr, col=ci + 1)
            # Bland-Altman: x=log10 geo-mean magnitude, y=log2(b/a)
            br = nrows + ri + 1
            if len(sub):
                mag = np.log10(np.sqrt(xa * xb))
                log2r = np.log2(xb / xa)
                fig.add_trace(go.Scatter(x=mag, y=log2r, mode="markers", text=txt,
                              marker=dict(size=5, opacity=0.5), showlegend=False),
                              row=br, col=ci + 1)
                fig.add_hline(y=log2r.median(), line=dict(color="red", dash="dot"),
                              row=br, col=ci + 1)
                fig.add_hline(y=0, line=dict(color="gray"), row=br, col=ci + 1)
            fig.update_xaxes(title_text="log10 mean pop", row=br, col=ci + 1)
            fig.update_yaxes(title_text="log2(b/a)", row=br, col=ci + 1)
    fig.update_layout(height=520 * nrows * 2, width=1300,
                      title_text=f"Source comparison — admin {level} "
                                 f"(co-positive rows; dashed=y=x, red=median bias)")
    return fig


def run_level(level: int):
    panel = pd.read_parquet(OUT / f"panel_adm{level}.parquet")
    kts = [k for k in q.COMMON_KT if k in panel["wind_speed_kt"].unique()]

    cov = coverage(panel)
    stats = pair_stats(panel)
    stats.to_csv(OUT / f"stats_adm{level}.csv", index=False)

    print(f"\n========== ADMIN {level} ==========")
    print("coverage (presence combos, full panel):")
    print(cov.to_string(index=False))
    print("\npairwise magnitude stats (co-positive rows):")
    print(stats.to_string(index=False))

    fig = make_figure(panel, level, kts)
    fig.write_html(OUT / f"scatter_adm{level}.html")
    print(f"  → wrote out/scatter_adm{level}.html")


def main():
    for level in (0, 1):
        run_level(level)


if __name__ == "__main__":
    main()
