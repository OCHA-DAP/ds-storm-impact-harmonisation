"""CERF rapid-response allocation model: INFORM_Composite, 2016+.

Production spec. Target: LogApproved = ln(allocation USD). Regressors:
- 8 emergency-type dummies (base = "Other")
- inform_composite (0-10, mean of Risk + Severity or Risk alone)
- LogRequired, LogTargeted

Fitting and prediction are kept here so the book and the analyst app
call the same code.
"""

from __future__ import annotations

from typing import Literal, TypedDict

import numpy as np
import pandas as pd
import statsmodels.api as sm

EMERGENCY_DUMMIES = [
    "Storm", "Flood", "Drought", "OtherNatural",
    "Cholera", "Ebola", "OtherHealth", "DisplConfl",
]
# "Other" is the implicit base category: all 8 dummies = 0.
ALLOWED_EMERGENCY_TYPES = (*EMERGENCY_DUMMIES, "Other")
EmergencyType = Literal[
    "Storm", "Flood", "Drought", "OtherNatural",
    "Cholera", "Ebola", "OtherHealth", "DisplConfl", "Other",
]

REGRESSORS = [
    *EMERGENCY_DUMMIES,
    "inform_composite",
    "LogRequired",
    "LogTargeted",
]
TARGET = "LogApproved"


class PredictionInput(TypedDict):
    emergency_type: EmergencyType
    inform_composite: float
    funding_required: float  # USD
    people_targeted: float   # count


class PredictionResult(TypedDict):
    point_usd_median: float
    point_usd_mean: float
    lower_usd: float
    upper_usd: float
    log_prediction: float
    log_sigma: float
    contributions: dict[str, float]


# ── Fit ──────────────────────────────────────────────────────────────

def fit_model(df: pd.DataFrame) -> sm.regression.linear_model.RegressionResultsWrapper:
    """Fit OLS on the training frame.

    Equivalent to `fit_3rm(df, ["inform_composite"])` in
    book/02c-analysis-inform.qmd:500-505. Drops rows with NaN in any
    required column.
    """
    sub = df.dropna(subset=REGRESSORS + [TARGET])
    X = sm.add_constant(sub[REGRESSORS].astype(float))
    return sm.OLS(sub[TARGET].astype(float), X).fit()


# ── Predict ──────────────────────────────────────────────────────────

def _design_row(inputs: PredictionInput) -> pd.DataFrame:
    """Build the one-row design matrix matching fit_model's regressors."""
    etype = inputs["emergency_type"]
    if etype not in ALLOWED_EMERGENCY_TYPES:
        raise ValueError(
            f"emergency_type must be one of {ALLOWED_EMERGENCY_TYPES}, "
            f"got {etype!r}"
        )
    funding = float(inputs["funding_required"])
    targeted = float(inputs["people_targeted"])
    if funding <= 0 or targeted <= 0:
        raise ValueError("funding_required and people_targeted must be > 0")

    row = {dummy: 1.0 if etype == dummy else 0.0 for dummy in EMERGENCY_DUMMIES}
    row["inform_composite"] = float(inputs["inform_composite"])
    row["LogRequired"] = np.log(funding)
    row["LogTargeted"] = np.log(targeted)

    X = pd.DataFrame([row], columns=REGRESSORS)
    return sm.add_constant(X, has_constant="add")


def predict(
    model: sm.regression.linear_model.RegressionResultsWrapper,
    inputs: PredictionInput,
    alpha: float = 0.05,
) -> PredictionResult:
    """Predict CERF allocation USD with 95% prediction interval.

    Returns both median (exp of log prediction) and mean (with σ²/2
    correction for log-normal back-transform) on the USD scale. The
    95% PI bounds are exponentiated from the log-scale observation CI.
    """
    X = _design_row(inputs)
    pred = model.get_prediction(X).summary_frame(alpha=alpha)

    log_pred = float(pred["mean"].iloc[0])
    log_lower = float(pred["obs_ci_lower"].iloc[0])
    log_upper = float(pred["obs_ci_upper"].iloc[0])
    log_sigma = float(np.sqrt(model.scale))

    # Per-feature contribution to the log prediction (excluding const).
    # Useful for driver-bar visualization.
    row_values = X.iloc[0].to_dict()
    contributions = {
        name: float(model.params[name]) * float(row_values[name])
        for name in REGRESSORS
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
