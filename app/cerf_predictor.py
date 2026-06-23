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

    from src.datasets.conflict import load_conflict_training_frame
    from src.datasets.inform import (
        COUNTRY_TO_ISO3,
        build_training_frame,
        calc_inform_composite,
        load_inform,
    )
    from src.models import cerf_conflict
    from src.models.cerf_inform import (
        REGRESSORS,
        REGRESSORS_NO_TARGETED,
        fit_model,
        predict,
    )

    inform_df = load_inform()
    training_df = build_training_frame(inform=inform_df)
    # INFORM-base models: with and without LogTargeted.
    model = fit_model(training_df, regressors=REGRESSORS)
    model_no_t = fit_model(training_df, regressors=REGRESSORS_NO_TARGETED)

    # Conflict-specific models: read corrected live training frame from blob,
    # filter to Xuan-corrected sample, fit Models A (w/ Targeted) and B.
    conflict_df_full = load_conflict_training_frame()
    conflict_df = conflict_df_full[~conflict_df_full["xuan_refugee_excluded"]]
    model_conflict_a = cerf_conflict.fit_model(
        conflict_df, regressors=cerf_conflict.REGRESSORS_A
    )
    model_conflict_b = cerf_conflict.fit_model(
        conflict_df, regressors=cerf_conflict.REGRESSORS_B
    )

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

    # Labels shown in UI → underlying dummy-column name used by the model.
    emergency_labels = {
        "Storm": "Storm",
        "Flood": "Flood",
        "Drought": "Drought",
        "Other Natural Disaster": "OtherNatural",
        "Cholera": "Cholera",
        "Ebola": "Ebola",
        "Other Health Emergency": "OtherHealth",
        "Displacement and Conflict": "DisplConfl",
        "Any Other": "Other",
    }

    refreshed = inform_df["refreshed_at"].iloc[0]
    return (
        REGRESSORS,
        REGRESSORS_NO_TARGETED,
        calc_inform_composite,
        cerf_conflict,
        country_options,
        default_month_label,
        default_year,
        emergency_labels,
        inform_df,
        model,
        model_conflict_a,
        model_conflict_b,
        model_no_t,
        month_options,
        predict,
        refreshed,
        training_df,
        year_options,
    )


@app.cell
def _load_conflict_context():
    """Load small lookup parquets used only when emergency is DisplConfl.

    ACLED:  global/acled/monthly_fatalities.parquet (~25k rows)
    IDMC:   global/idmc/displacement_daily.parquet (Conflict-only, ~177k rows)

    Both are pre-aggregated by `scripts/refresh_acled_monthly.py` and
    `scripts/refresh_idmc_displacement.py`; the app just reads them.
    """
    import ocha_stratus as stratus

    acled_monthly = stratus.load_parquet_from_blob(
        "acled/monthly_fatalities.parquet",
        stage="dev", container_name="global",
    )
    idmc_daily = stratus.load_parquet_from_blob(
        "idmc/displacement_daily.parquet",
        stage="dev", container_name="global",
    )
    idmc_daily = idmc_daily[idmc_daily["displacement_type"] == "Conflict"].copy()
    return acled_monthly, idmc_daily


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
    returns a median estimate and an 80% prediction interval showing how
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
    country_options,
    default_month_label,
    default_year,
    emergency_labels,
    mo,
    month_options,
    year_options,
):
    emergency = mo.ui.dropdown(
        options=emergency_labels,
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
    # Default to 0 so the user must explicitly enter values. The predict
    # cell blocks on 0 and shows a "please enter values" message.
    # No step= because HTML would snap typed values to start + N*step.
    funding = mo.ui.number(
        start=0, value=0,
        label="Funding required (USD)",
    )
    targeted = mo.ui.number(
        start=0, value=0,
        label="People targeted (optional)",
    )

    _date_hint = mo.Html(
        "<div style='color:#888;font-size:0.8em;margin-top:-2px;'>"
        "Year and month default to today&rsquo;s date.</div>"
    )
    _targeted_hint = mo.Html(
        "<div style='color:#888;font-size:0.8em;margin-top:-2px;'>"
        "Leave <i>People targeted</i> at 0 to use the no-targeted model "
        "variant (lower fit, but suitable when targeted-population isn&rsquo;t "
        "yet known).</div>"
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
            _date_hint,
            mo.hstack(
                [funding, targeted], justify="start", align="center", gap=2,
            ),
            _targeted_hint,
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
def _derive_conflict_context(
    acled_monthly, country, emergency, idmc_daily, month, year,
):
    """Auto-lookup ACLED monthly fatalities + IDMC 30d at the alloc-period.

    Active only when emergency type == DisplConfl. Otherwise returns
    `{"active": False}` and the prediction path uses the INFORM-base model.

    For the IDMC lookup we need a specific date; user picks year + month.
    We use the **last day of the selected month** (most recent IDMC
    snapshot for that period). If month is "annual", we fall back to
    Dec 31 of the year.
    """
    import calendar as _cal
    import pandas as _pd

    if emergency.value != "DisplConfl":
        conflict_ctx = {"active": False, "fatalities": 0.0, "idps_30d": 0.0,
                        "missing_acled": False, "missing_idmc": False,
                        "lookup_date": None}
    else:
        _yr = int(year.value)
        _mo = 12 if month.value == "none" else int(month.value)
        _last_day = _cal.monthrange(_yr, _mo)[1]
        _lookup_date = _pd.Timestamp(year=_yr, month=_mo, day=_last_day)

        _ac = acled_monthly[
            (acled_monthly["iso3"] == country.value)
            & (acled_monthly["year"] == _yr)
            & (acled_monthly["month"] == _mo)
        ]
        _fat = float(_ac["fatalities"].iloc[0]) if len(_ac) else 0.0
        _missing_acled = len(_ac) == 0

        _idmc = idmc_daily[
            (idmc_daily["iso3"] == country.value)
            & (idmc_daily["date"] == _lookup_date)
        ]
        _idp = float(_idmc["displacement_30d"].iloc[0]) if len(_idmc) else 0.0
        _missing_idmc = len(_idmc) == 0

        conflict_ctx = {
            "active": True,
            "fatalities": _fat,
            "idps_30d": _idp,
            "missing_acled": _missing_acled,
            "missing_idmc": _missing_idmc,
            "lookup_date": _lookup_date,
        }
    return (conflict_ctx,)


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
    REGRESSORS,
    REGRESSORS_NO_TARGETED,
    cerf_conflict,
    conflict_ctx,
    emergency,
    funding,
    lookup,
    model,
    model_conflict_a,
    model_conflict_b,
    model_no_t,
    predict,
    targeted,
):
    """Pick the right model and produce a prediction.

    Routing matrix:
        emergency_type     | targeted > 0  | model
        ───────────────────┼───────────────┼──────────────────
        DisplConfl         | yes           | conflict A
        DisplConfl         | no            | conflict B
        anything else      | yes           | INFORM (with targeted)
        anything else      | no            | INFORM (no targeted)
    """
    is_conflict = emergency.value == "DisplConfl"
    has_targeted = targeted.value is not None and float(targeted.value) > 0

    state = "ok"
    result = None
    active_model = None

    if lookup is None:
        state = "no_inform"
    elif funding.value is None or float(funding.value) <= 0:
        state = "missing_inputs"
    else:
        if is_conflict:
            if has_targeted:
                _chosen = model_conflict_a
                _regs = cerf_conflict.REGRESSORS_A
                _label = "Conflict, with Targeted"
            else:
                _chosen = model_conflict_b
                _regs = cerf_conflict.REGRESSORS_B
                _label = "Conflict, without Targeted"
            result = cerf_conflict.predict(
                _chosen,
                {
                    "inform_composite": lookup["composite"],
                    "funding_required": float(funding.value),
                    "people_targeted": float(targeted.value or 0),
                    "monthly_fatalities": float(conflict_ctx["fatalities"]),
                    "idps_30d": float(conflict_ctx["idps_30d"]),
                },
                alpha=0.20, regressors=_regs,
            )
        else:
            if has_targeted:
                _chosen = model
                _regs = REGRESSORS
                _label = "INFORM-base, with Targeted"
            else:
                _chosen = model_no_t
                _regs = REGRESSORS_NO_TARGETED
                _label = "INFORM-base, without Targeted"
            result = predict(
                _chosen,
                {
                    "emergency_type": emergency.value,
                    "inform_composite": lookup["composite"],
                    "funding_required": float(funding.value),
                    "people_targeted": float(targeted.value or 0),
                },
                alpha=0.20, regressors=_regs,
            )
        active_model = {
            "label": _label,
            "n": int(_chosen.nobs),
            "adj_r2": float(_chosen.rsquared_adj),
            "aic": float(_chosen.aic),
        }
    return active_model, result, state


@app.cell
def _prediction_numbers(mo, result, state):
    def _fmt(v: float) -> str:
        if v >= 1e9:
            return f"${v / 1e9:.2f}B"
        if v >= 1e6:
            return f"${v / 1e6:.2f}M"
        if v >= 1e3:
            return f"${v / 1e3:.0f}K"
        return f"${v:.0f}"

    if state == "missing_inputs":
        numbers = mo.md(
            "*Enter funding required and people targeted to see a prediction.*"
        )
    elif state == "no_inform":
        numbers = mo.md(
            "*Prediction unavailable — no INFORM data for that country/year.*"
        )
    else:
        _median = result["point_usd_median"]
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
    80% prediction interval
  </div>
  <div style="font-size: 1.05em; color: #333;">
    {_fmt(_lo)} &nbsp;—&nbsp; {_fmt(_hi)}
  </div>
</div>
            """
        )
    return (numbers,)


@app.cell
def _prediction_plot(mo, result, state):
    if state != "ok":
        chart = mo.md("")
    else:
        import matplotlib.pyplot as plt
        import numpy as np
        from matplotlib.ticker import FuncFormatter
        from scipy import stats

        _mu = result["log_prediction"]
        _sigma = result["log_sigma"]
        _median = result["point_usd_median"]
        _lo = result["lower_usd"]
        _hi = result["upper_usd"]

        _dist = stats.lognorm(s=_sigma, scale=np.exp(_mu))
        # Plot a generous range around the predictive distribution so the
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
            label="80% prediction interval",
        )
        ax.axvline(_median, color="#F0635C", linewidth=2, label="Median")

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
def _conflict_context_panel(conflict_ctx, country, mo):
    """Display the auto-looked-up ACLED + IDMC values when in conflict mode."""
    def _fmt(v: float) -> str:
        if v >= 1e6:
            return f"{v / 1e6:.2f}M"
        if v >= 1e3:
            return f"{v / 1e3:.1f}K"
        return f"{v:.0f}"

    if not conflict_ctx["active"]:
        conflict_panel = mo.md("")
    else:
        _date_label = (conflict_ctx["lookup_date"].strftime("%b %Y")
                       if conflict_ctx["lookup_date"] is not None else "—")
        _fat_note = (" <span style='color:#c46;font-size:0.85em;'>(no events recorded)</span>"
                     if conflict_ctx["missing_acled"] else "")
        _idp_note = (" <span style='color:#c46;font-size:0.85em;'>(no IDMC record)</span>"
                     if conflict_ctx["missing_idmc"] else "")
        conflict_panel = mo.Html(
            f"""
<div style="background:#fdf6ec;border-left:3px solid #d9a43a;
            padding:10px 14px;border-radius:4px;font-size:0.9em;
            color:#333;margin-bottom:8px;display:inline-block;">
  <div style="font-size:0.7em;text-transform:uppercase;letter-spacing:0.05em;
              color:#7a5d1a;font-weight:600;margin-bottom:4px;">
    Auto-looked-up conflict covariates &nbsp;·&nbsp; {country.value} &nbsp;·&nbsp; {_date_label}
  </div>
  <span><b>ACLED monthly fatalities:</b> {_fmt(conflict_ctx['fatalities'])}{_fat_note}</span>
  &nbsp;&nbsp;·&nbsp;&nbsp;
  <span><b>IDMC IDPs (30-day rolling):</b> {_fmt(conflict_ctx['idps_30d'])}{_idp_note}</span>
</div>
"""
        )
    return (conflict_panel,)


@app.cell
def _model_banner(active_model, mo):
    if active_model is None:
        banner = mo.md("")
    else:
        banner = mo.Html(
            f"""
<div style="background:#eef3f8;border-left:3px solid #2166ac;
            padding:8px 12px;border-radius:4px;font-size:0.88em;
            color:#234;margin-bottom:6px;display:inline-block;">
  <b>Model in use:</b> {active_model['label']} &nbsp;·&nbsp;
  n = {active_model['n']} &nbsp;·&nbsp;
  Adj R² = {active_model['adj_r2']:.3f} &nbsp;·&nbsp;
  AIC = {active_model['aic']:.1f}
</div>
"""
        )
    return (banner,)


@app.cell
def _prediction_layout(banner, chart, conflict_panel, mo, numbers):
    mo.vstack(
        [
            mo.md("## Predicted allocation"),
            conflict_panel,
            banner,
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
def _technical_note(mo, model, model_conflict_a, model_conflict_b, model_no_t, refreshed):
    mo.accordion({
        "Technical note": mo.md(
            f"""
**Four model variants** are pre-fit at startup. Which one runs depends on
the **emergency type** and whether **People targeted** is provided
(non-zero):

| Emergency | People targeted | Model | n | Adj R² | AIC |
|---|---|---|---|---|---|
| any (non-conflict) | provided | INFORM-base, with Targeted | {int(model.nobs)} | {model.rsquared_adj:.3f} | {model.aic:.1f} |
| any (non-conflict) | left at 0 | INFORM-base, without Targeted | {int(model_no_t.nobs)} | {model_no_t.rsquared_adj:.3f} | {model_no_t.aic:.1f} |
| Displacement & Conflict | provided | Conflict, with Targeted | {int(model_conflict_a.nobs)} | {model_conflict_a.rsquared_adj:.3f} | {model_conflict_a.aic:.1f} |
| Displacement & Conflict | left at 0 | Conflict, without Targeted | {int(model_conflict_b.nobs)} | {model_conflict_b.rsquared_adj:.3f} | {model_conflict_b.aic:.1f} |

The two **without-Targeted** variants are useful when a targeted-population
estimate isn't yet available at allocation-decision time. The fit is
materially weaker (larger prediction interval), so prefer the with-Targeted
variant whenever possible.

The **conflict-specific** variants (Models A and B) are fit on a separate
training set of 97 conflict-typed allocations from 2018 onward
(ch. 02d), with monthly ACLED fatalities and 30-day IDMC IDPs as
additional regressors. The vulnerability index is `inform_composite`
(substituting for Finn's CIRV; ~1% Adj R² gap per ch. 02d).

**Features (INFORM-base).** Eight emergency-type dummies (base = "Any
Other"): Storm, Flood, Drought, Other Natural Disaster, Cholera, Ebola,
Other Health Emergency, Displacement and Conflict. INFORM Composite
(0–10). ln(funding required). ln(people targeted) when applicable.

**Features (Conflict).** INFORM Composite. ln(funding required).
ln(people targeted) when applicable. ln(monthly fatalities + 1).
ln(IDPs 30d + 1).

**Data sources.** CERF 3RM v1.8 spreadsheet; CERF conflict-model xlsx
(Zimmermann 2025); INFORM Risk via DRMKC API; INFORM Severity via ACAPS
(blob); ACLED conflict events via hdx-signals; IDMC daily IDP updates
(refreshed by `scripts/refresh_idmc_displacement.py`). The INFORM
Composite is the mean of Risk and Severity for country-months with both
available, otherwise Risk alone.

**Back-transform.** The model predicts ln(USD). The **median** USD is
exp(ln-prediction). The **80% prediction interval** bounds are
exponentiated from the log-scale observation interval.

### Caveats — when to use with caution

- **Country.** Only countries CERF has allocated to since 2016 are
  listed in the dropdown. A country you don't see here is outside
  the model's training set.
- **Emergency type.** For types CERF rarely responds to (e.g.,
  tsunami, wildfires), the model is unreliable even under "Any
  Other".
- **Funding required.** Reliable range is roughly **$2M to $2.5B**.
  Outside this range the model is extrapolating — use with caution.
- **People targeted.** Reliable range is roughly **2,500 to 45M**.
  Same caveat.
- **Large-allocation bias.** For predicted amounts above ~$20M, the
  model tends to *underestimate* — CERF has occasionally allocated
  more than the model suggests for catastrophic events.

### Interpreting the output

- Treat the median as **an input to decision-making**, not the
  final amount.
- The 80% prediction interval is the range 80% of comparable
  historical allocations fall within.

INFORM data refreshed {refreshed[:19]}.
"""
        ),
    })
    return


if __name__ == "__main__":
    app.run()
