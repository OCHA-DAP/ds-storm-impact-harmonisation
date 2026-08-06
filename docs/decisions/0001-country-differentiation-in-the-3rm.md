---
status: proposed
date: 2026-06-16
decision-makers: ["@zackarno"]
consulted: []
informed: []
---

# Do not add country-level differentiation to the 3RM (for now)

> **Moved.** The 3RM work this ADR governs left this repo — see
> [ADR 0007](0007-move-the-3rm-work-to-its-own-repo.md). The live copy is in
> [`OCHA-DAP/ds-cerf-allocation-patterns`](https://github.com/OCHA-DAP/ds-cerf-allocation-patterns)
> at `docs/decisions/0001-country-differentiation-in-the-3rm.md`, with paths and
> chapter numbers updated for that repo. Amend it there, not here. This copy is
> kept as the historical record of a decision taken in this repo; the file and
> chapter paths it cites (`src/models/…`, `book/02b`–`02e`) no longer exist here.

## Context and Problem Statement

The CERF Rapid-Response Regression Model (3RM; Rost 2025, replicated and
extended in book chapters `02b`/`02c`/`02d`) predicts `LogApproved`
(ln of CERF allocation USD) from a pooled OLS: 8 emergency-type dummies,
a vulnerability term (`inform_composite` / CIRV), `LogRequired`, and
`LogTargeted`. The model is **pooled across countries** — it allows a
different baseline per *emergency type* but never per *country*. Two
countries with identical covariates get the same predicted allocation.

Chapter `02d` showed countries differ a lot in their conflict-allocation
behaviour (per-country ACLED slopes from −0.46 to +0.71), and `02b`'s
appendix found a country-level "average annual RR spending" feature that
looked like a strong predictor of storm allocations. Both hinted that a
**country-level differentiator** might improve the model. Neither was
tested out-of-sample. This ADR records whether to add one.

Two principled ways to inject country information were evaluated:

1. A **country random intercept** `(1 | iso3)` — partial pooling of the
   per-country *baseline*, learned internally from the model's own outcomes.
2. An **external country covariate** — a country's baseline CERF
   engagement, borrowed from a broader slice of CERF data.
3. A **country random slope** — the *effect* of a predictor, not just the
   baseline, varies by country (motivated by `02d`'s −0.46→+0.71 ACLED slopes).

## Decision Drivers

* Out-of-sample predictive accuracy under the realistic CERF use case —
  "known country, next event" — not in-sample fit (which `02b`'s appendix
  and `02d`'s fixed-effects gains relied on).
* Leakage-free evaluation: a predictor must not contain the outcome it
  predicts.
* Robustness at the sample sizes actually available (full 3RM n≈422;
  TC-only subset n≈35–47).
* Production simplicity: the 3RM is consumed by the book and the analyst
  app via `src/models/cerf_inform.py`; added parameters and data
  dependencies have a maintenance cost.

## Considered Options

* **Option 1 — Keep the 3RM pooled** (no country differentiation).
* **Option 2 — Country random intercept** on the full 3RM (partial
  pooling), evaluated in `artefacts/16_3rm_country_random_intercept.py`.
* **Option 3 — External de-leaked country-baseline covariate** on the
  TC-only subset, evaluated in `artefacts/17_tc_country_baseline_feature.py`.
* **Option 4 — `02b`'s appendix "average annual RR" feature** as-is.
* **Option 5 — Country random slopes**: full 3RM (slope on `LogRequired`) and
  conflict (slope on ACLED).

## Decision Outcome

Chosen option: **Option 1 — keep the 3RM pooled**, because neither
country-differentiation approach improved honest out-of-sample prediction,
and the one feature that looked promising (Option 4) was shown to be
largely data leakage.

* **Option 2 adds nothing.** A full-data mixed fit gives an intraclass
  correlation of **0.044** — only ~4% of post-control residual variance
  lives between countries (`sigma_country` = 0.099 log-units vs
  `sigma_resid` = 0.460). Under forward-chaining temporal CV (train past →
  predict next year), the random intercept never improves a fold and is
  marginally worse overall: all held-out rows R² 0.627 (pooled) vs 0.616
  (+intercept); known-country rows only (n=214) 0.604 vs 0.592. Mechanism:
  `LogRequired`/`LogTargeted` already encode the country's allocation tier,
  so the random intercept only adds shrunken offsets that fit
  non-transferable in-sample noise.

* **Option 4 is leakage.** On a leakage-free, same-rows comparison (n=35),
  `02b`'s feature drops from an apparent LOO-CV R² of 0.259 (its own
  47-row sample) to **0.002**. Its apparent signal lives entirely in the
  ~12 rows lacking any non-storm history — countries whose only CERF
  engagement is storms, where "average all-type RR" is essentially their
  own storm spending predicting itself.

* **Option 3 is, at best, modest and not yet bankable.** A properly
  de-leaked, as-of (prior-years, non-storm-only) country baseline retains
  a small out-of-sample signal (LOO-CV R² ≈ 0.118 at n=35), but it costs
  ~25% of storms (no prior non-storm history → selection toward
  chronically-engaged countries), and TC-subset estimates are too unstable
  to trust: exposure's LOO-CV R² alone swings from −0.15 to +0.36 across
  overlapping subsamples.

* **The conflict subset agrees** (artefact 18,
  `18_conflict_country_differentiation.py`; n=97, within-country temporal
  CV — the test `02d`'s leave-one-country-out could not provide). Conflict
  ICC is **0.0003** (even less between-country variance than the full 3RM),
  the random intercept is again marginally *worse* than the pooled
  production model (R² 0.622 vs 0.633), and ACLED carries no
  allocation-*size* signal even under this fair CV (S0→S1: 0.5800→0.5802).
  The only country lever with a consistent positive sign is again the
  external as-of baseline (S2→S3: +0.013 R²) — small and not decisive at
  n=43 held-out. (IDMC, separately, *is* a useful out-of-sample predictor
  here.)

* **Random slopes (Option 5): dead in the full model, one open question in
  conflict.** A random slope on `LogRequired` (full 3RM) is noise — the
  parsimonious slope-only fit is worse on AIC than pooled, LRT p≈0.31, and the
  raw per-country slope spread collapses ~7:1 under shrinkage. But a random
  slope on **ACLED in the conflict model** is a genuine in-sample effect: the
  slope-only form is the **best AIC (114.95 vs pooled 119.47, ΔAIC −4.5)**, LRT
  p=0.011, lifting conditional (in-sample) R² to ≈0.77 (+0.05 over pooled) — it
  clears the in-sample bar the production OLS was itself selected on. Its
  *out-of-sample* payoff is unconfirmed, though: temporal-CV RMSE points the
  right way (0.381 vs 0.402) but paired Wilcoxon p=0.34 at n≈43, and ~79% of the
  raw spread is shrinkage-removed noise. We keep production pooled and record
  this as an explicit **open question** (see Consequences).

### Consequences

* Good: the production 3RM stays simple, well-conditioned, interpretable,
  and free of new data dependencies; we avoid overfitting a country signal
  the data can't support.
* Good: `02b`'s appendix feature is explicitly retired, so it is not
  mistaken for a validated predictor (a cross-reference note is added to
  `02b`).
* Neutral / deferred: we leave on the table any genuinely
  country-conditional signal — in particular the untested hypothesis that
  wind exposure and country-baseline engagement are **complementary across
  regimes** (exposure for severe storms in rarely-funded countries,
  baseline for chronically-engaged ones).
* **Primary open question — the conflict ACLED random slope.** It is the one
  form of differentiation that earns its keep in-sample. Promote it to a
  deployment candidate in `src/models/cerf_conflict.py` only once an
  out-of-sample gain clears significance (more data, or a use case that values
  the explanatory regime-split — episodic-violence countries respond to
  fatality spikes, chronic ones do not). Note a random-slope production model
  also adds real complexity: per-country BLUP slopes, an unseen-country
  fallback, and prediction intervals with an extra variance component.
* Other open threads, narrower: revisit when **either** (a) materially more
  allocations accrue, lifting the TC/conflict subsets out of the unstable-n
  regime and letting the small external as-of-baseline signal be confirmed or
  rejected; or (b) a use case requires prediction for **unseen** countries —
  noting `02d`'s LOCO already showed country fixed effects *hurt* there, so this
  would argue against, not for, differentiation.

### Confirmation

The full analysis is consolidated into — and re-run at render by — the book
chapter `book/02e-country-differentiation.qmd` (the standalone scripts
`artefacts/16–19_*.py` were the scratch it was distilled from). Confirm there:
full 3RM ICC ≈ 0.044 (REML) and no temporal-CV improvement from a random
intercept; the leaky feature collapsing to ~0 on leakage-free same-rows CV;
conflict ICC ≈ 0.0003; and the conflict ACLED slope-only model's AIC 114.95 vs
pooled 119.47 (LRT p=0.011) with an unconfirmed out-of-sample gain. Any future
change adding country differentiation to `src/models/cerf_inform.py` or
`src/models/cerf_conflict.py` should supersede this ADR with evidence of an
out-of-sample gain under the same within-country temporal-CV protocol.

## Pros and Cons of the Options

### Option 1 — Keep the 3RM pooled

* Good, because it matches the evidence (no out-of-sample gain from
  differentiation).
* Good, because it keeps the production model and analyst app simple.
* Bad, because it ignores real (but non-transferable) between-country
  heterogeneity documented in `02d`.

### Option 2 — Country random intercept

* Good, because partial pooling is the principled way to handle
  per-country baselines and degrades gracefully (shrinks to the mean) for
  thin countries.
* Bad, because ICC ≈ 0.04 leaves almost nothing to capture, and it is
  marginally worse out-of-sample.
* Bad, because it is under-identified on the smaller subsets (the
  n=140 early CV fold failed to converge even after centering).

### Option 3 — External de-leaked country-baseline covariate

* Good, because it imports cross-hazard country information the storm-only
  controls can't see, and is leakage-free by construction (non-storm,
  prior-years-only).
* Good, because it is defined for any country with prior CERF engagement,
  including those with no prior *storm* allocations.
* Bad, because the TC-only signal is modest (~0.12) and fragile at
  n≈35, with a ~25% coverage cost and a selection bias toward
  chronically-engaged countries.

### Option 4 — `02b`'s appendix "average annual RR" feature

* Good, because it is trivial to compute and looked strong in-sample.
* Bad, because it leaks: the outcome contributes to its own predictor, and
  on honest same-rows CV its signal collapses to ~0.

### Option 5 — Country random slopes

* Good, because it targets the heterogeneity `02d` actually found (the ACLED
  effect varies by conflict regime), not baseline levels the controls absorb.
* Good (conflict only), because the parsimonious slope-only form is the best
  AIC and clears the in-sample bar the production model was selected on.
* Bad, because the full-model slope is noise, and the conflict effect's
  out-of-sample gain is unconfirmed at n≈97 (most raw spread shrinkage-removed).
* Bad, because a random-slope production model adds non-trivial complexity
  (per-country BLUPs, unseen-country fallback, prediction intervals).

## More Information

* Consolidated deep-dive (re-runs the full analysis at render):
  `book/02e-country-differentiation.qmd`.
* Upstream book chapters: `book/02b-analysis-cerf-api.qmd` (TC wind exposure +
  3RM replication, the appendix feature), `book/02c-analysis-inform.qmd`
  (INFORM vs CIRV), `book/02d-analysis-conflict.qmd` (conflict 3RM,
  country heterogeneity, LOCO).
* Analysis artefacts (scratch, superseded by chapter `02e`):
  `artefacts/16–19_*.py`.
* Production models: `src/models/cerf_inform.py`,
  `src/models/cerf_conflict.py`.
* Source model: Rost (2025), "CERF 3RM — RR Regression Model".
