"""Analyst-facing predictor for CERF rapid-response allocation size.

Run with: `uv run marimo run app/cerf_predictor.py`
Or edit with: `uv run marimo edit app/cerf_predictor.py`

Model: INFORM_Composite OLS on 2016+ 3RM data (see book chapter 02c).
"""

import marimo

__generated_with = "0.23.1"
app = marimo.App(width="medium")


@app.cell
def _setup():
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

    import marimo as mo
    from dotenv import load_dotenv

    _ = load_dotenv()  # assigned so marimo doesn't display the bool return
    return (mo,)


@app.cell
def _load_data():
    import calendar
    import datetime as _dt

    from src.datasets.inform import (
        COUNTRY_TO_ISO3,
        build_training_frame,
        calc_inform_composite,
        load_inform,
    )
    from src.models.cerf_inform import (
        ALLOWED_EMERGENCY_TYPES,
        fit_model,
        predict,
    )

    inform_df = load_inform()
    training_df = build_training_frame(inform=inform_df)
    model = fit_model(training_df)

    # Country dropdown: filter to the 86 3RM countries with nice names
    # (intersected with INFORM Risk coverage so lookups always succeed).
    available_iso3s = set(inform_df["iso3"].dropna().unique())
    country_options = {
        f"{name} ({iso3})": iso3
        for name, iso3 in sorted(COUNTRY_TO_ISO3.items())
        if iso3 in available_iso3s
    }

    year_options = sorted(inform_df["year"].unique().tolist())

    # Month dropdown: abbreviated (Jan, Feb, ...). "—" means annual / Risk-only.
    month_options: dict[str, str] = {"— (annual, Risk-only)": "none"}
    for m in range(1, 13):
        month_options[calendar.month_abbr[m]] = str(m)

    today = _dt.date.today()
    default_year = (
        str(today.year) if today.year in year_options else str(year_options[-1])
    )
    default_month_label = calendar.month_abbr[today.month]
    if default_month_label not in month_options:
        default_month_label = "— (annual, Risk-only)"

    refreshed = inform_df["refreshed_at"].iloc[0]
    return (
        ALLOWED_EMERGENCY_TYPES,
        calc_inform_composite,
        country_options,
        default_month_label,
        default_year,
        inform_df,
        model,
        month_options,
        predict,
        refreshed,
        training_df,
        year_options,
    )


@app.cell
def _intro(mo):
    mo.Html(
        """
<div style="font-family: inherit;">
  <h1 style="margin: 0 0 14px 0; color: #55b284; font-size: 2.6em;
             line-height: 1.15; font-weight: 700;">
    CERF rapid-response allocation predictor
  </h1>
  <div style="background: #f5f7fa; border-left: 3px solid #55b284;
              padding: 10px 14px; border-radius: 4px; font-size: 0.92em;
              line-height: 1.45; color: #333;">
    Estimate the expected CERF rapid-response allocation size for a given
    emergency. Pick an emergency type, a country and date, the funding
    being requested, and the number of people targeted &mdash; the model
    returns a median estimate and a 95% prediction interval showing how
    much individual allocations typically vary around that central value.
    <span style="color:#666;font-style:italic;">
      Intended as a rough guide, not a forecast.
    </span>
  </div>
</div>
"""
    )
    return


@app.cell
def _inputs(
    ALLOWED_EMERGENCY_TYPES,
    country_options,
    default_month_label,
    default_year,
    mo,
    month_options,
    year_options,
):
    emergency = mo.ui.dropdown(
        options=list(ALLOWED_EMERGENCY_TYPES),
        value="Storm",
        label="Emergency type",
    )
    country = mo.ui.dropdown(
        options=country_options,
        value=next(iter(country_options)),
        label="Country",
    )
    year = mo.ui.dropdown(
        options=[str(y) for y in year_options],
        value=default_year,
        label="Year",
    )
    month = mo.ui.dropdown(
        options=month_options,
        value=default_month_label,
        label="Month",
    )
    # No step= so any positive number is accepted; HTML otherwise snaps
    # typed values to start + N*step, which rejects small values.
    funding = mo.ui.number(
        start=1, value=5_000_000,
        label="Funding required (USD)",
    )
    targeted = mo.ui.number(
        start=1, value=200_000,
        label="People targeted",
    )

    form = mo.vstack(
        [
            mo.md("### Scenario inputs"),
            mo.hstack(
                [emergency, country], justify="start", align="center", gap=2,
            ),
            mo.hstack(
                [year, month], justify="start", align="center", gap=2,
            ),
            mo.hstack(
                [funding, targeted], justify="start", align="center", gap=2,
            ),
        ],
        gap=0.5,
        align="start",
    )
    return country, emergency, form, funding, month, targeted, year


@app.cell
def _derive_composite(
    calc_inform_composite,
    country,
    inform_df,
    mo,
    month,
    year,
):
    month_val = None if month.value == "none" else int(month.value)
    lookup = calc_inform_composite(
        inform_df,
        iso3=country.value,
        year=int(year.value),
        month=month_val,
    )

    if lookup is None:
        _body = (
            f"<div style='font-weight:600;margin-bottom:4px;color:#8a4500;'>"
            f"⚠ No INFORM Risk data for {country.value} in {year.value}.</div>"
            "<div style='color:#555;font-size:0.9em;'>"
            "Pick a different country/year combination.</div>"
        )
        _bg, _border = "#fff4e5", "#f0b070"
    else:
        _carried_note = ""
        if lookup["risk_carried"]:
            _carried_note = (
                "<div style='margin-top:6px;font-size:0.78em;color:#7a5d1a;"
                "background:#fff7e0;border-left:3px solid #d9a43a;"
                "padding:6px 9px;border-radius:3px;'>"
                f"Using the latest published INFORM Risk "
                f"(<b>{lookup['risk_year']}</b>) as the {year.value} "
                "assessment. INFORM Risk lags the calendar year by ~1 "
                "year; this is the standard analyst workaround and what "
                "the latest DRMKC workflow itself represents."
                "</div>"
            )

        if lookup["source"] == "blended":
            _badge_label = "blended"
            _badge_bg, _badge_fg = "#d9ead0", "#3e8f6b"
            _detail = (
                f"Risk <b>{lookup['risk']:.2f}</b> &nbsp;·&nbsp; "
                f"Severity <b>{lookup['severity']:.2f}</b>"
            )
            _explainer = (
                "Mean of INFORM Risk (forward-looking, annual) and INFORM "
                "Severity (current crisis conditions, monthly). Both "
                "available for this country-month."
            )
        else:
            _badge_label = "Risk-only"
            _badge_bg, _badge_fg = "#f6e3d9", "#b04a2a"
            _detail = (
                f"Risk <b>{lookup['risk']:.2f}</b> &nbsp;·&nbsp; "
                "<span style='color:#888;'>Severity unavailable</span>"
            )
            _explainer = (
                "INFORM Severity only exists from 2019 onward, and even "
                "within that window it's published only when ACAPS is "
                "actively tracking a crisis in that country-month &mdash; "
                "so it isn't available for every country-year-month. "
                "This combination has no Severity record, so the "
                "Composite falls back to Risk alone."
            )
        _body = (
            "<div style='display:flex;align-items:center;gap:8px;"
            "margin-bottom:2px;'>"
            "<span style='font-size:0.7em;text-transform:uppercase;"
            "letter-spacing:0.05em;color:#3e8f6b;font-weight:600;'>"
            "INFORM Composite</span>"
            f"<span style='font-size:0.65em;background:{_badge_bg};"
            f"color:{_badge_fg};padding:1px 8px;border-radius:10px;"
            f"font-weight:500;'>{_badge_label}</span>"
            "</div>"
            f"<div style='font-size:1.6em;font-weight:600;line-height:1.1;"
            f"color:#55b284;'>"
            f"{lookup['composite']:.2f} <span style='font-size:0.55em;"
            "color:#88a;font-weight:400;'>/ 10</span></div>"
            f"<div style='margin-top:10px;font-size:0.9em;'>{_detail}</div>"
            f"{_carried_note}"
            f"<div style='margin-top:8px;font-size:0.8em;color:#666;"
            f"line-height:1.35;'>{_explainer}</div>"
        )
        _bg, _border = "#edf6ef", "#a8cfb4"
    inform_panel = mo.Html(
        f"""<div style="
            padding: 12px 14px;
            background: {_bg};
            border: 1px solid {_border};
            border-radius: 6px;
            font-family: inherit;
        ">{_body}</div>"""
    )
    return inform_panel, lookup, month_val


@app.cell
def _top_layout(form, inform_panel, mo):
    mo.hstack(
        [form, inform_panel],
        widths=[2, 1],
        gap=2,
        align="start",
        justify="start",
    )
    return


@app.cell
def _predict_cell(
    emergency,
    funding,
    lookup,
    model,
    predict,
    targeted,
):
    if lookup is None or funding.value is None or targeted.value is None:
        result = None
    elif float(funding.value) <= 0 or float(targeted.value) <= 0:
        result = None
    else:
        result = predict(model, {
            "emergency_type": emergency.value,
            "inform_composite": lookup["composite"],
            "funding_required": float(funding.value),
            "people_targeted": float(targeted.value),
        })
    return (result,)


@app.cell
def _prediction_numbers(mo, result):
    def _fmt(v: float) -> str:
        if v >= 1e9:
            return f"${v / 1e9:.2f}B"
        if v >= 1e6:
            return f"${v / 1e6:.2f}M"
        if v >= 1e3:
            return f"${v / 1e3:.0f}K"
        return f"${v:.0f}"

    if result is None:
        numbers = mo.md(
            "*Prediction unavailable — no INFORM data for that country/year.*"
        )
    else:
        _median = result["point_usd_median"]
        _mean = result["point_usd_mean"]
        _lo = result["lower_usd"]
        _hi = result["upper_usd"]
        numbers = mo.Html(
            f"""
<div style="padding: 8px 4px; font-family: inherit;">
  <div style="font-size: 0.75em; text-transform: uppercase;
              letter-spacing: 0.05em; color: #666;">Median</div>
  <div style="font-size: 1.9em; font-weight: 600; line-height: 1.1;
              color: #F0635C;">{_fmt(_median)}</div>
  <div style="font-size: 0.8em; color: #888; margin-top: 2px;">
    ${_median:,.0f}
  </div>

  <div style="font-size: 0.75em; text-transform: uppercase;
              letter-spacing: 0.05em; color: #666; margin-top: 18px;">
    Mean (log-normal)
  </div>
  <div style="font-size: 1.15em; font-weight: 500; color: #333;">
    {_fmt(_mean)}
  </div>

  <div style="font-size: 0.75em; text-transform: uppercase;
              letter-spacing: 0.05em; color: #666; margin-top: 18px;">
    95% prediction interval
  </div>
  <div style="font-size: 1.05em; color: #333;">
    {_fmt(_lo)} &nbsp;—&nbsp; {_fmt(_hi)}
  </div>
</div>
            """
        )
    return (numbers,)


@app.cell
def _prediction_plot(mo, result):
    if result is None:
        chart = mo.md("")
    else:
        import matplotlib.pyplot as plt
        import numpy as np
        from matplotlib.ticker import FuncFormatter
        from scipy import stats

        _mu = result["log_prediction"]
        _sigma = result["log_sigma"]
        _median = result["point_usd_median"]
        _mean = result["point_usd_mean"]
        _lo = result["lower_usd"]
        _hi = result["upper_usd"]

        _dist = stats.lognorm(s=_sigma, scale=np.exp(_mu))
        # Plot a generous range around the 95% PI so the distribution
        # tails are visible without dominating the view.
        _x_min = max(_dist.ppf(0.005), _lo * 0.3)
        _x_max = _dist.ppf(0.995)
        _x = np.linspace(_x_min, _x_max, 400)
        _pdf = _dist.pdf(_x)

        fig, ax = plt.subplots(figsize=(6.5, 3.2))
        ax.plot(_x, _pdf, color="#2166ac", linewidth=1.8)
        _pi_mask = (_x >= _lo) & (_x <= _hi)
        ax.fill_between(
            _x[_pi_mask], _pdf[_pi_mask], alpha=0.25, color="#2166ac",
            label="95% prediction interval",
        )
        ax.axvline(_median, color="#F0635C", linewidth=2, label="Median")
        ax.axvline(_mean, color="#555555", linewidth=1.5, linestyle="--",
                   label="Mean")

        def _fmt_axis_usd(v, _pos):
            if v >= 1e9:
                return f"${v / 1e9:.1f}B"
            if v >= 1e6:
                return f"${v / 1e6:.1f}M"
            if v >= 1e3:
                return f"${v / 1e3:.0f}K"
            return f"${v:.0f}"

        ax.xaxis.set_major_formatter(FuncFormatter(_fmt_axis_usd))
        ax.set_xlabel("Allocation (USD)")
        ax.set_yticks([])
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.spines["left"].set_visible(False)
        ax.legend(loc="upper right", frameon=False, fontsize=9)
        fig.tight_layout()

        chart = mo.mpl.interactive(fig)
    return (chart,)


@app.cell
def _prediction_layout(chart, mo, numbers):
    mo.vstack(
        [
            mo.md("## Predicted allocation"),
            mo.hstack(
                [chart, numbers],
                widths=[3, 1],
                gap=2,
                align="center",
                justify="start",
            ),
        ],
        gap=0.5,
    )
    return


@app.cell
def _technical_note(mo, model, refreshed):
    mo.accordion({
        "Technical note": mo.md(
            f"""
**Model.** Ordinary least squares on ln(CERF allocation USD), fit on
{int(model.nobs)} rapid-response allocations from 2016 onward. Adjusted
R² = {model.rsquared_adj:.3f}. AIC = {model.aic:.1f}.

**Features.** Eight emergency-type dummies (base = "Other"): Storm,
Flood, Drought, OtherNatural, Cholera, Ebola, OtherHealth,
Displacement/Conflict. INFORM Composite (0–10). ln(funding required).
ln(people targeted).

**Data sources.** CERF 3RM v1.8 spreadsheet; INFORM Risk via DRMKC API;
INFORM Severity via ACAPS (blob). The Composite is the mean of Risk
and Severity for country-months with both available, otherwise Risk
alone.

**Back-transform.** The model predicts ln(USD). The **median** USD is
exp(ln-prediction). The **mean** applies a log-normal correction
(+σ²/2) and is somewhat higher because the distribution is
right-skewed. The **95% prediction interval** bounds are exponentiated
from the log-scale observation interval.

**Limitations.** The 2016+ training set skews toward historical funding
patterns. Predictions for emergencies or countries unlike those in the
training set should be treated cautiously. The interval reflects
model uncertainty only, not policy or political factors.

INFORM data refreshed {refreshed[:19]}.
"""
        ),
    })
    return


if __name__ == "__main__":
    app.run()
