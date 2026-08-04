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
import numpy as np
import ocha_stratus as stratus
import pandas as pd

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.lines import Line2D  # noqa: E402
from scipy.stats import gaussian_kde  # noqa: E402

OCHA_HISTORICAL_BLOB = (
    "ds-storm-impact-harmonisation/processed/adm0_ibtracs_exp_all.parquet"
)
BUFFER_TO_SPEED = {"buffer39": 34, "buffer74": 64}

SPEEDS = [34, 64]          # the two GDACS getimpact publishes

# ---------------------------------------------------------------------------
# HDX v2 design tokens (methods/style-guide.md in the team KB; mirror in
# ds-knowledge-base-internal/style-reference/tokens.md). Values are lifted
# rather than imported because email cannot pull the HDX CSS bundle — the
# style guide sanctions exactly this for apps that can't import wholesale.
# ---------------------------------------------------------------------------
INK = "#1f2324"        # --hdx-neutral-9
INK_2 = "#5e6a6b"      # --hdx-neutral-7
INK_3 = "#7e8e8f"      # --hdx-neutral-6
LINE = "#e2e8e8"       # --hdx-neutral-15
GROUND = "#f5f7f7"     # --hdx-neutral-05
PANEL = "#ffffff"      # --hdx-neutral-0
GREY_FILL = "#d8e0e1"  # --hdx-neutral-2
GREY_EDGE = "#9db1b3"  # --hdx-neutral-5

# GDACS alert levels map onto the HDX status ramp, which is what it is for.
ALERT_COLORS = {
    "green": "#2f9e6f",    # --hdx-success-5
    "orange": "#d48f2a",   # --hdx-warning-5
    "red": "#c44536",      # --hdx-error-5
}

# PDC severity is an ORDERED scale, so it takes a one-hue sequential ramp.
# The style guide's default sequential is the BRAND ramp, but brand is HDX
# teal-green and "widespread damage" rendered green reads as reassurance.
# Using the PRIMARY ramp instead — still an HDX token family, semantically
# neutral. Deviation is deliberate; noted here so it is not mistaken for drift.
ACCENT = "#134ead"     # --hdx-primary-6
SEQ_1 = "#a3c0ef"      # --hdx-primary-2  minor
SEQ_2 = "#1862d8"      # --hdx-primary-5  moderate
SEQ_3 = "#0a2756"      # --hdx-primary-8  widespread

# --hdx-font-body / --hdx-font-display, with email-safe fallbacks: webfonts
# are unreliable in mail clients, so the stacks degrade rather than vanish.
SANS = ("Roboto,-apple-system,BlinkMacSystemFont,'Segoe UI',"
        "Helvetica,Arial,sans-serif")
DISPLAY = "Merriweather,Georgia,'Times New Roman',serif"
MONO = "ui-monospace,SFMono-Regular,Menlo,Consolas,monospace"

# Light -> dark = minor -> widespread, monotonic in lightness by construction.
PDC_SEVERITY_COLORS = {"1": SEQ_1, "2": SEQ_2, "3": SEQ_3}
PDC_SEVERITY_LABEL = {"1": "minor", "2": "moderate", "3": "widespread"}


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
# Ridgeline panel (matplotlib -> PNG bytes)
# ----------------------------------------------------------------------------


def _kde_curve(vals: np.ndarray, grid: np.ndarray) -> np.ndarray | None:
    """Density over log10(pop), peak-normalised. None if too little to smooth."""
    v = np.log10(vals[vals > 0])
    if v.size < 5 or np.isclose(v.std(), 0):
        return None
    d = gaussian_kde(v, bw_method=0.25)(grid)
    return d / d.max() if d.max() > 0 else None


def build_ridgeline_png(
    iso3: str,
    country: str,
    historical: pd.DataFrame,
    current: dict[int, float],
    pdc_bands: pd.DataFrame | None = None,
    pdc_total: float | None = None,
) -> bytes:
    """One country panel: a density ridge per wind threshold, plus PDC.

    Each ridge is the density of this country's historical exposure at that
    threshold (log scale); a dot marks the current storm.

    PDC, when present, is a **stacked damage-class bar** on its own row with
    its RIGHT EDGE anchored to PDC's total on the shared population axis and a
    connector to the ridges. It is never a dot on a ridge: PDC's figure shares
    the population axis but is not a 34/64 kt quantity, because its damage
    classes are a modelled damage ratio, not wind rings (docs/pdc_api.md).

    Note the one compromise: a log axis cannot represent additive stacking, so
    only the bar's right edge carries an axis position and the segment widths
    are shares. The legend says so.
    """
    hist = historical[(historical["iso3"] == iso3) & (historical["pop_exposed"] > 0)]
    has_pdc = (
        pdc_total is not None and pdc_total > 0
        and pdc_bands is not None and not pdc_bands.empty
        and float(pdc_bands["pop_total"].sum()) > 0
    )

    pools = [hist[hist["speed"] == sp]["pop_exposed"].values for sp in SPEEDS]
    pool = np.concatenate([v for v in pools if v.size]) if any(
        v.size for v in pools) else np.array([1.0])
    lo, hi = np.log10(max(pool.min(), 1)) - 0.35, np.log10(pool.max()) + 0.35
    for v in list(current.values()) + ([pdc_total] if has_pdc else []):
        if v and v > 0:
            lo, hi = min(lo, np.log10(v) - 0.35), max(hi, np.log10(v) + 0.35)
    grid = np.linspace(lo, hi, 400)

    # Style guide: "Roboto for chart text". Not installed here, so the stack
    # degrades to the nearest grotesque rather than matplotlib's DejaVu default.
    plt.rcParams["font.family"] = "sans-serif"
    plt.rcParams["font.sans-serif"] = ["Roboto", "Helvetica Neue", "Helvetica",
                                       "Arial", "DejaVu Sans"]
    fig, ax = plt.subplots(figsize=(6.9, 3.15 if has_pdc else 2.6), dpi=150)
    fig.patch.set_facecolor(PANEL)
    row_h, gap, pdc_h = 1.0, 0.55, 0.34
    y0 = (pdc_h + 0.52) if has_pdc else 0.0
    lx = lo - (hi - lo) * 0.045

    for i, speed in enumerate(SPEEDS):
        base = y0 + (len(SPEEDS) - 1 - i) * (row_h + gap)
        vals = hist[hist["speed"] == speed]["pop_exposed"].values
        dens = _kde_curve(vals, grid)
        if dens is not None:
            ax.fill_between(grid, base, base + dens * row_h, color=GREY_FILL,
                            alpha=.85, linewidth=0, zorder=2)
            ax.plot(grid, base + dens * row_h, color=GREY_EDGE, lw=1, zorder=3)
        ax.plot([grid[0], grid[-1]], [base, base], color=LINE, lw=.8, zorder=1)
        ax.text(lx, base + row_h * .40, f"{speed} kt", ha="right", va="center",
                fontsize=10.5, color=INK, fontweight="bold")
        ax.text(lx, base + row_h * .13, f"{vals.size} storms", ha="right",
                va="center", fontsize=7.2, color=INK_3)

        cur = current.get(speed)
        if cur and cur > 0:
            ax.plot([np.log10(cur)], [base + .055], marker="o", ms=8, mfc=INK,
                    mec=PANEL, mew=1.4, zorder=6)
        else:
            ax.text(lx, base - row_h * .10, "not reached", ha="right",
                    va="center", fontsize=7, color=INK_3, style="italic")

    top = y0 + (len(SPEEDS) - 1) * (row_h + gap) + row_h

    if has_pdc:
        x_end, x_start = np.log10(pdc_total), lo
        span = x_end - x_start
        total = float(pdc_bands["pop_total"].sum())
        ax.plot([x_end, x_end], [pdc_h, top + .06], color=ACCENT, lw=1.2,
                ls=(0, (3, 2)), alpha=.75, zorder=5)
        left = x_start
        for lvl in ("1", "2", "3"):
            row = pdc_bands[pdc_bands["level"] == lvl]
            if row.empty or float(row["pop_total"].iloc[0]) <= 0:
                continue
            val = float(row["pop_total"].iloc[0])
            w = span * (val / total)
            ax.add_patch(plt.Rectangle((left, 0), w, pdc_h,
                                       facecolor=PDC_SEVERITY_COLORS[lvl],
                                       edgecolor=PANEL, lw=1.2, zorder=4))
            if w / span > .14:
                # White fails on the pale step (#b7d3f6 ~1.5:1); use ink there.
                txt = INK if lvl == "1" else PANEL
                ax.text(left + w / 2, pdc_h / 2,
                        f"{val / total * 100:.0f}% {PDC_SEVERITY_LABEL[lvl]}",
                        ha="center", va="center", fontsize=7.6, color=txt,
                        fontweight="bold", zorder=6)
            left += w
        ax.text(lx, pdc_h * .62, "PDC", ha="right", va="center", fontsize=10.5,
                color=ACCENT, fontweight="bold")
        ax.text(lx, pdc_h * .10, "damage class", ha="right", va="center",
                fontsize=7.2, color=INK_3)
        ax.text(x_end + (hi - lo) * .012, pdc_h / 2,
                _abbrev(pdc_total),
                ha="left", va="center", fontsize=8, color=ACCENT,
                fontweight="bold")

    decades = np.arange(np.floor(lo), np.ceil(hi) + 1)
    ax.set_xticks(decades)
    ax.set_xticklabels(
        [f"{10 ** d / 1e6:g}M" if d >= 6 else
         (f"{10 ** d / 1e3:g}K" if d >= 3 else f"{10 ** d:g}") for d in decades],
        fontsize=8, color=INK_3)
    ax.set_xlim(lo - (hi - lo) * .30, hi)
    ax.set_ylim(-0.34, top + .42)
    ax.set_yticks([])
    for side in ("left", "right", "top"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(LINE)
    ax.tick_params(axis="x", length=3, color=LINE)
    ax.set_xlabel("population exposed (log scale)", fontsize=8, color=INK_3)

    handles = [
        Line2D([], [], marker="o", ls="", mfc=INK, mec="white", ms=7,
               label="this storm (GDACS)"),
        Line2D([], [], color=GREY_EDGE, lw=6, alpha=.5,
               label=f"{country} storm history (GDACS)"),
    ]
    if has_pdc:
        handles.append(Line2D([], [], color=ACCENT, lw=6, alpha=.9,
                              label="PDC damage classes (width = % share)"))
    ax.legend(handles=handles, loc="upper left", bbox_to_anchor=(0, -0.30),
              ncol=3, frameon=False, fontsize=7.8, handlelength=1.6,
              labelcolor=INK_3)

    fig.tight_layout()
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight", facecolor=PANEL)
    plt.close(fig)
    return buf.getvalue()


def png_to_data_uri(png: bytes) -> str:
    return "data:image/png;base64," + base64.b64encode(png).decode("ascii")


# ----------------------------------------------------------------------------
# HTML assembly
#
# Email HTML: inline styles only, no external CSS, no flexbox. Light ground
# only — email dark-mode handling is too inconsistent to design against.
# ----------------------------------------------------------------------------


def _st(fam: str, size: str, weight: int = 400, lh: float = 1.5,
        color: str = "", extra: str = "") -> str:
    """Inline style string. Longhand only — mail clients strip the `font:`
    shorthand, which silently flattened this email's whole type hierarchy."""
    out = (f"font-family:{fam};font-size:{size};line-height:{lh};"
           f"font-weight:{weight};")
    if color:
        out += f"color:{color};"
    return out + extra


def _fmt(n: float) -> str:
    return f"{int(n):,}"


def _abbrev(n: float) -> str:
    """Three significant figures with a unit suffix: 149M, 11.9M, 93.5K.

    Full-precision counts crowd the country header and are not decision-
    relevant at this scale; the exact value stays in the tooltip-free table
    of the book and in the raw capture.
    """
    n = float(n)
    for div, suf in ((1e9, "B"), (1e6, "M"), (1e3, "K")):
        if abs(n) >= div:
            v = n / div
            if v >= 100:
                return f"{v:.0f}{suf}"
            return f"{v:.3g}{suf}"
    return f"{int(n):,}"


def _class_name(description: str) -> str:
    """Short PDC class name for headline use.

    PDC ships e.g. "Moderate Damage; 5% of value" — the clause after the
    semicolon is a damage-ratio gloss that reads as noise beside a number.
    Keep the name here; the full strings are defined in the footer.
    """
    return (description or "").split(";")[0].strip()


def _alert_chip(level: str) -> str:
    c = ALERT_COLORS.get(str(level).lower(), INK_3)
    return (
        f"<span style=\"display:inline-block;padding:2px 8px;border-radius:3px;"
        f"background:{c};color:#fff;{_st(MONO,'10px',600,1.6,"")}"
        f"letter-spacing:.08em;text-transform:uppercase;\">{level}</span>"
    )


def _landfall_html(meta: dict) -> str:
    """Landfall place / time / category. GDACS carries no equivalent field."""
    admin0, lf_time = meta.get("landfall_admin0"), meta.get("landfall_time")
    if not admin0 or not lf_time:
        return ""
    try:
        when = datetime.fromtimestamp(float(lf_time), timezone.utc)
    except (TypeError, ValueError):
        return ""
    hours, cat = meta.get("hours_landfall"), meta.get("category_landfall")
    cells = [("Where", admin0), ("When", when.strftime("%d %b %H:%M UTC"))]
    if hours:
        cells.append(("Lead time", f"{int(float(hours))} h"))
    if cat:
        cells.append(("At landfall", f"Category {int(float(cat))}"))
    tds = "".join(
        f"<td style=\"padding:0 22px 0 0;vertical-align:top;\">"
        f"<div style=\"{_st(MONO,'9.5px',500,1.6,"")}letter-spacing:.1em;"
        f"text-transform:uppercase;color:{INK_3};\">{k}</div>"
        f"<div style=\"{_st(SANS,'16px',600,1.3,INK)}\">{v}</div></td>"
        for k, v in cells
    )
    return (
        f"<table role=\"presentation\" cellpadding=\"0\" cellspacing=\"0\" "
        f"style=\"width:100%;margin:0 0 22px;background:#eef4fb;"
        f"border-left:3px solid {ACCENT};border-radius:0 6px 6px 0;\">"
        f"<tr><td style=\"padding:14px 18px;\">"
        f"<div style=\"{_st(MONO,'9.5px',600,1.6,"")}letter-spacing:.12em;"
        f"text-transform:uppercase;color:{ACCENT};margin-bottom:8px;\">"
        f"PDC landfall forecast</div>"
        f"<table role=\"presentation\" cellpadding=\"0\" cellspacing=\"0\">"
        f"<tr>{tds}</tr></table>"
        f"</td></tr></table>"
    )


def _pdc_facts(exposure_row: pd.Series) -> str:
    bits = []
    for label, key in (("hospitals", "capital_hospital"),
                       ("schools", "capital_school"),
                       ("households", "households"),
                       ("vulnerable", "pop_vulnerable")):
        v = exposure_row.get(key)
        if v is not None and not pd.isna(v) and v > 0:
            bits.append(
                f"<b style=\"color:{INK};font-weight:600;\">{_fmt(v)}</b> {label}"
            )
    if not bits:
        return ""
    return (
        f"<div style=\"{_st(MONO,'11.5px',400,1.7,INK_3)}"
        f"margin:2px 0 0;\">also in PDC footprint &nbsp;"
        + " &nbsp;&middot;&nbsp; ".join(bits) + "</div>"
    )


def _country_block_html(
    country: str, iso3: str, current: dict[int, float], png_uri: str,
    pdc_row: pd.Series | None, pdc_bands: pd.DataFrame | None,
) -> str:
    """Country header (GDACS | PDC side by side), panel, then PDC extras.

    PDC's class names are its own `exposureDescription` strings verbatim —
    "Moderate Damage; 5% of value" is the tell that these are damage ratios,
    and paraphrasing it to "moderate" throws that away.
    """
    lab = _st(MONO, "9.5px", 600, 1.6, INK_3,
              "letter-spacing:.1em;text-transform:uppercase;")

    gd_rows = "".join(
        f"<div style=\"margin-bottom:2px;\">"
        f"<b style=\"{_st(SANS, '17px', 700, 1.25, INK)}\">{_abbrev(v)}</b>"
        f"<span style=\"{_st(MONO, '11px', 400, 1.25, INK_3)}\">"
        f" at {sp} kt</span></div>"
        for sp, v in sorted(current.items()) if v and v > 0
    ) or (f"<div style=\"{_st(SANS, '13px', 400, 1.5, INK_3)}\">"
          f"no exposure</div>")

    if pdc_bands is not None and not pdc_bands.empty:
        order = {"3": 0, "2": 1, "1": 2}
        rows = sorted(
            (r for r in pdc_bands.itertuples() if float(r.pop_total) > 0),
            key=lambda r: order.get(str(r.level), 9),
        )
        pdc_rows = "".join(
            f"<div style=\"margin-bottom:2px;\">"
            f"<span style=\"display:inline-block;width:9px;height:9px;"
            f"border-radius:2px;background:"
            f"{PDC_SEVERITY_COLORS.get(str(r.level), INK_3)};\"></span>"
            f"<b style=\"{_st(SANS, '14px', 700, 1.3, INK)}\">"
            f" {_abbrev(r.pop_total)}</b>"
            f"<span style=\"{_st(SANS, '11.5px', 400, 1.3, INK_2)}\">"
            f" {_class_name(r.description)}</span></div>"
            for r in rows
        )
        pdc_cell = (
            f"<td style=\"vertical-align:top;padding-left:26px;\">"
            f"<div style=\"{lab}margin-bottom:5px;\">PDC damage class</div>"
            f"{pdc_rows}</td>"
        )
    else:
        pdc_cell = (
            f"<td style=\"vertical-align:top;padding-left:26px;\">"
            f"<div style=\"{lab}margin-bottom:5px;\">PDC</div>"
            f"<div style=\"{_st(SANS, '12.5px', 400, 1.5, INK_3)}\">"
            f"no exposure computed for this country</div></td>"
        )

    facts = _pdc_facts(pdc_row) if pdc_row is not None else ""
    return (
        f"<tr><td style=\"padding:18px 0 6px;border-top:1px solid {LINE};\">"
        f"<div style=\"{_st(SANS, '15px', 700, 1.4, INK)}margin-bottom:10px;\">"
        f"{country}"
        f"<span style=\"{_st(MONO, '11px', 400, 1.4, INK_3)}\"> {iso3}</span>"
        f"</div>"
        f"<table role=\"presentation\" cellpadding=\"0\" cellspacing=\"0\" "
        f"style=\"margin-bottom:12px;\"><tr>"
        f"<td style=\"vertical-align:top;\">"
        f"<div style=\"{lab}margin-bottom:5px;\">GDACS exposed</div>"
        f"{gd_rows}</td>"
        f"{pdc_cell}"
        f"</tr></table>"
        f"<img src=\"{png_uri}\" alt=\"{country} exposure vs history\" "
        f"style=\"display:block;width:100%;max-width:660px;height:auto;\" />"
        f"{facts}"
        f"</td></tr>"
    )


def _storm_section_html(
    storm: pd.Series, storm_exposure: pd.DataFrame, historical: pd.DataFrame,
    pdc: dict | None = None,
) -> str:
    """One storm card. PDC is additive: absent PDC renders the card unchanged."""
    by_iso: dict[str, dict[int, float]] = {}
    names: dict[str, str] = {}
    for r in storm_exposure.itertuples():
        sp = BUFFER_TO_SPEED.get(r.buffer)
        if sp is None or not r.pop_affected or r.pop_affected <= 0:
            continue
        by_iso.setdefault(r.iso3, {})[sp] = float(r.pop_affected)
        names[r.iso3] = r.country

    order = sorted(by_iso, key=lambda k: -max(by_iso[k].values()))
    rows = []
    for iso3 in order:
        pdc_bands = pdc_row = None
        pdc_total = None
        if pdc is not None:
            b = pdc["bands"][pdc["bands"]["iso3"] == iso3]
            e = pdc["exposure"][pdc["exposure"]["iso3"] == iso3]
            if not b.empty and float(b["pop_total"].sum()) > 0 and not e.empty:
                pdc_bands, pdc_row = b, e.iloc[0]
                pdc_total = float(e.iloc[0]["pop_total"])
        png = build_ridgeline_png(iso3, names[iso3], historical, by_iso[iso3],
                                  pdc_bands, pdc_total)
        rows.append(_country_block_html(names[iso3], iso3, by_iso[iso3],
                                        png_to_data_uri(png), pdc_row,
                                        pdc_bands))

    if not rows:
        rows = [
            f"<tr><td style=\"padding:14px 0;border-top:1px solid {LINE};"
            f"{_st(SANS,'13px',400,1.6,INK_3)}\">"
            f"No population exposed in this episode's GDACS footprint.</td></tr>"
        ]

    landfall = _landfall_html(pdc["meta"]) if pdc else ""
    frm = str(storm.get("from_date", ""))[:10]
    return (
        f"<table role=\"presentation\" cellpadding=\"0\" cellspacing=\"0\" "
        f"style=\"width:100%;background:#fff;border:1px solid {LINE};"
        f"border-radius:8px;margin:0 0 20px;\">"
        f"<tr><td style=\"padding:22px 24px 24px;\">"
        f"<div style=\"margin-bottom:4px;\">{_alert_chip(storm['alert_level'])}</div>"
        f"<div style=\"{_st(SANS,'22px',700,1.2,"")}letter-spacing:-.02em;"
        f"color:{INK};margin:6px 0 3px;\">{storm['name']}</div>"
        f"<div style=\"{_st(MONO,'11.5px',400,1.6,INK_3)}"
        f"margin-bottom:18px;\">GDACS {storm['eventid']} &middot; since {frm}</div>"
        f"{landfall}"
        f"<table role=\"presentation\" cellpadding=\"0\" cellspacing=\"0\" "
        f"style=\"width:100%;\">" + "".join(rows) + "</table>"
        "</td></tr></table>"
    )


def _header_html(ts: datetime, n: int) -> str:
    """Summary line only.

    Listmonk's OCHA template already renders the campaign subject as a header
    bar and an "automated message produced by the OCHA Centre for Humanitarian
    Data" strip above the body, so repeating a title and an OCHA eyebrow here
    duplicates the wrapper. This adds only what the wrapper cannot know.
    """
    return (
        f"<div style=\"{_st(SANS, '15px', 400, 1.6, INK_2)}"
        f"margin:0 0 22px;\">"
        f"{ts:%d %B %Y, %H:%M} UTC &nbsp;&middot;&nbsp; "
        f"<b style=\"color:{INK};font-weight:700;\">{n} active tropical "
        f"cyclone{'s' if n != 1 else ''}</b></div>"
    )


def _footer_html() -> str:
    def note(title, body):
        return (
            f"<div style=\"margin-bottom:12px;\">"
            f"<div style=\"{_st(MONO,'9.5px',600,1.6,"")}letter-spacing:.1em;"
            f"text-transform:uppercase;color:{INK_3};\">{title}</div>"
            f"<div style=\"{_st(SANS,'12px',400,1.65,INK_3)}\">{body}</div>"
            f"</div>"
        )
    return (
        f"<table role=\"presentation\" cellpadding=\"0\" cellspacing=\"0\" "
        f"style=\"width:100%;border-top:1px solid {LINE};margin-top:8px;\">"
        f"<tr><td style=\"padding:20px 4px 0;\">"
        + note("How to read the panel",
               "Each ridge is the density of that country's historical storm "
               "exposure at that wind threshold; the dot is this storm. "
               "&ldquo;Not reached&rdquo; means the swath at that threshold does "
               "not touch the country at all.")
        + note("PDC damage classes",
               "PDC's own definitions, in its words: <b>Minor Damage</b> "
               "&mdash; power out. <b>Moderate Damage</b> &mdash; 5% of value, "
               "i.e. roughly 5% of built capital value lost. <b>Widespread "
               "Damage and Above</b>. They are a modelled damage <i>ratio</i>, "
               "not wind thresholds, and cannot be mapped onto 34/64 kt.")
        + note("PDC",
               "The blue bar is PDC's damage-class split, its right edge placed "
               "at PDC's total on the same axis. Segment widths are shares, not "
               "axis distances &mdash; a log scale cannot stack additively. PDC is "
               "admin-0 only with no historical archive, so it carries "
               "no return period. Facility counts come from a merged inventory "
               "(OpenStreetMap/HOT, HIFLD, NDPBA, national sources) whose "
               "completeness varies by country.")
        + note("Sources",
               "Current values: GDACS live impact endpoint (symmetric max-radius "
               "corridor). History: OCHA in-house IBTrACS-based exposure, 2001 "
               "onward (asymmetric wind-radii polygons). The two use different "
               "wind-field reconstructions, so the comparison is indicative. "
               "See chapter 07 of the harmonisation book.")
        + "</td></tr></table>"
    )


def build_email_html(
    active_storms: pd.DataFrame, exposure: pd.DataFrame,
    historical: pd.DataFrame, timestamp: datetime,
    buffer: str = "buffer39", pdc_by_event: dict | None = None,
) -> str:
    """Assemble the monitor email. `buffer` is accepted for API compatibility;
    the ridgeline panel shows every threshold in BUFFER_TO_SPEED."""
    cards = []
    for _, storm in active_storms.iterrows():
        sub = exposure[exposure["eventid"] == storm["eventid"]]
        cards.append(_storm_section_html(
            storm, sub, historical, (pdc_by_event or {}).get(storm["eventid"])))
    return (
        f"<html><body style=\"margin:0;padding:0;background:{GROUND};\">"
        f"<table role=\"presentation\" cellpadding=\"0\" cellspacing=\"0\" "
        f"style=\"width:100%;background:{GROUND};\"><tr><td align=\"center\" "
        f"style=\"padding:32px 16px 48px;\">"
        f"<table role=\"presentation\" cellpadding=\"0\" cellspacing=\"0\" "
        f"style=\"width:100%;max-width:720px;text-align:left;\">"
        f"<tr><td>"
        + _header_html(timestamp, len(active_storms))
        + "".join(cards) + _footer_html()
        + "</td></tr></table></td></tr></table></body></html>"
    )


def build_stub_html(timestamp: datetime) -> str:
    """Placeholder for days with no active storms."""
    return (
        f"<html><body style=\"margin:0;padding:0;background:{GROUND};\">"
        f"<table role=\"presentation\" cellpadding=\"0\" cellspacing=\"0\" "
        f"style=\"width:100%;background:{GROUND};\"><tr><td align=\"center\" "
        f"style=\"padding:32px 16px 48px;\">"
        f"<table role=\"presentation\" cellpadding=\"0\" cellspacing=\"0\" "
        f"style=\"width:100%;max-width:720px;text-align:left;\"><tr><td>"
        + _header_html(timestamp, 0)
        + f"<div style=\"background:#fff;border:1px solid {LINE};"
        f"border-radius:8px;padding:24px;{_st(SANS,'14px',400,1.6,"")}"
        f"color:{INK_2};\">No active tropical cyclones in GDACS at this "
        f"issuance. The next check runs on the following synoptic cycle.</div>"
        + _footer_html()
        + "</td></tr></table></td></tr></table></body></html>"
    )
