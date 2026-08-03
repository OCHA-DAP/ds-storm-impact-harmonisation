"""Conflict-allocation models A and B for CERF rapid-response.

Production specs from `book/02d-analysis-conflict.qmd`. Both fit on the
Xuan-corrected live training frame from
`src.datasets.conflict.load_conflict_training_frame()`.

- Model A — w/ LogTargeted: preferred when targeted-people figure is
  known at allocation-decision time.
- Model B — w/o LogTargeted: fallback for early-decision contexts.

Vulnerability index is `inform_composite` (mean of Risk + Severity, fall
back to Risk alone where Severity is unavailable). The chapter's
historical fits used Finn's CIRV; we substitute inform_composite here
so the app can compute the index live (CIRV needs additional
non-INFORM pillars we don't pull).
"""

from __future__ import annotations

from typing import TypedDict

import numpy as np
import pandas as pd
import statsmodels.api as sm

REGRESSORS_A = [
    "inform_composite", "LogTargeted", "LogRequired",
    "LogMonthlyFatalities", "LogIDPs30d",
]
REGRESSORS_B = [
    "inform_composite", "LogRequired",
    "LogMonthlyFatalities", "LogIDPs30d",
]
TARGET = "LogApproved"


class ConflictPredictionInput(TypedDict, total=False):
    inform_composite: float
    funding_required: float          # USD
    people_targeted: float           # count; omit or 0 ⇒ Model B
    monthly_fatalities: float        # ACLED, allocation-month sum
    idps_30d: float                  # IDMC 30-day rolling sum at alloc date


class ConflictPredictionResult(TypedDict):
    point_usd_median: float
    point_usd_mean: float
    lower_usd: float
    upper_usd: float
    log_prediction: float
    log_sigma: float
    contributions: dict[str, float]


def fit_model(
    df: pd.DataFrame, regressors: list[str] = REGRESSORS_A,
) -> sm.regression.linear_model.RegressionResultsWrapper:
    """Fit OLS for the given conflict-model regressor list (A or B)."""
    sub = df.dropna(subset=regressors + [TARGET])
    X = sm.add_constant(sub[regressors].astype(float))
    return sm.OLS(sub[TARGET].astype(float), X).fit()


def _design_row(
    inputs: ConflictPredictionInput, regressors: list[str]
) -> pd.DataFrame:
    funding = float(inputs["funding_required"])
    if funding <= 0:
        raise ValueError("funding_required must be > 0")
    fatalities = float(inputs.get("monthly_fatalities") or 0)
    idps_30d = float(inputs.get("idps_30d") or 0)
    if fatalities < 0 or idps_30d < 0:
        raise ValueError("fatalities and idps_30d must be >= 0")

    row = {
        "inform_composite": float(inputs["inform_composite"]),
        "LogRequired": np.log(funding),
        "LogMonthlyFatalities": np.log(fatalities + 1),
        "LogIDPs30d": np.log(idps_30d + 1),
    }
    if "LogTargeted" in regressors:
        targeted = float(inputs.get("people_targeted") or 0)
        if targeted <= 0:
            raise ValueError("people_targeted must be > 0 for Model A")
        row["LogTargeted"] = np.log(targeted)

    X = pd.DataFrame([row], columns=regressors)
    return sm.add_constant(X, has_constant="add")


def predict(
    model: sm.regression.linear_model.RegressionResultsWrapper,
    inputs: ConflictPredictionInput,
    alpha: float = 0.05,
    regressors: list[str] = REGRESSORS_A,
) -> ConflictPredictionResult:
    X = _design_row(inputs, regressors)
    pred = model.get_prediction(X).summary_frame(alpha=alpha)
    log_pred = float(pred["mean"].iloc[0])
    log_lower = float(pred["obs_ci_lower"].iloc[0])
    log_upper = float(pred["obs_ci_upper"].iloc[0])
    log_sigma = float(np.sqrt(model.scale))
    row_values = X.iloc[0].to_dict()
    contributions = {
        name: float(model.params[name]) * float(row_values[name])
        for name in regressors
    }
    return {
        "point_usd_median": float(np.exp(log_pred)),
        "point_usd_mean": float(np.exp(log_pred + log_sigma**2 / 2)),
        "lower_usd": float(np.exp(log_lower)),
        "upper_usd": float(np.exp(log_upper)),
        "log_prediction": log_pred,
        "log_sigma": log_sigma,
        "contributions": contributions,
    }
