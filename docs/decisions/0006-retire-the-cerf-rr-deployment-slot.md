---
status: accepted
date: 2026-08-03
decision-makers: Zack Arno
consulted: []
informed: OCHA CHD Data Science team
---

# Retire the cerf-rr deployment slot; the CERF predictor is now a production repo

> **Option D has since been executed.** This ADR retired the deployment but
> deferred removing `app/cerf_predictor.py`. That removal happened in
> [ADR 0007](0007-move-the-3rm-work-to-its-own-repo.md), which moved the whole
> 3RM section out of this repo. The "two copies may drift" risk recorded below
> is therefore closed.

## Context and Problem Statement

The CERF rapid-response allocation estimator (`app/cerf_predictor.py`) was
developed in this repo and deployed to the `cerf-rr` slot of the
`chd-ds-seas5-viz` Azure App Service, via
`.github/workflows/merge-cerf-exposure_chd-ds-seas5-viz(cerf-rr).yml`.

That was always an awkward home. `chd-ds-seas5-viz` is a **shared** App Service
whose production site and `development` slot serve an unrelated app from
`ocha-dap/ds-seas5-viz`; the CERF predictor was a lodger on a slot of someone
else's service, deployed from a feature branch of a research repo.

The app has since been ported into its own production repository,
`ocha-dap/ds-cerf-3rm-app`, deployed to its own App Service `CERF-3RM` at
<https://cerf-3rm.azurewebsites.net>. The ported code (`app/cerf_predictor.py`,
`src/datasets/inform.py`, `src/models/cerf_inform.py`,
`scripts/refresh_inform_composite.py`) was adapted there, and that deployment
also picked up a weekly INFORM refresh (`refresh-inform.yml`) that this repo's
deployment never had.

Two copies of the same app served from two Azure services is worse than one,
and the trunk merge (#3) made this urgent: the workflow had been retargeted
from the now-deleted `merge-cerf-exposure` branch to `main`, so **every push to
main was redeploying the superseded app**.

## Decision Drivers

* One app, one deployment. Two live copies invite drift and confuse whoever
  finds the second URL.
* The replacement is verified live; the old slot is not (see Confirmation).
* This repo is research-grade; a production app should not be deployed from it.
* Nothing should be deleted that is not demonstrably superseded.
* The `development` slot and the production site on the same App Service belong
  to a different team's app and must not be disturbed.

## Considered Options

* **A — Retire the slot.** Delete the deploy workflow here and delete the
  `cerf-rr` slot in Azure.
* **B — Delete the workflow, leave the slot.** Stop deploying, leave the slot
  allocated.
* **C — Keep both deployments.** Continue deploying from here as a staging copy
  of the production app.
* **D — Retire the slot and also remove `app/` from this repo.**

## Decision Outcome

Chosen: **A — retire the slot.**

The deploy workflow is deleted here and the `cerf-rr` slot is deleted from the
`chd-ds-seas5-viz` App Service. Explicitly scoped: **only** the `cerf-rr` slot.
The production site and the `development` slot on that App Service serve
`ds-seas5-viz` and are untouched.

B was rejected because an allocated slot with no deployment is exactly the kind
of orphan nobody can later identify or safely remove — it decays into "is this
load-bearing?". C was rejected because this repo has no business running a
production deployment, and a staging copy that nobody redeploys is not staging.

D is deferred, not rejected. Removing `app/` is a larger change: chapter 02c and
`docs/README.md` still describe the predictor's data lineage, and the INFORM
sources feeding it are documented here. Retiring the *deployment* is
self-contained; deciding whether the *source* should stay as reference or be
deleted as a duplicate of `ds-cerf-3rm-app` deserves its own pass.

### Consequences

* Good, because there is now exactly one CERF predictor deployment, in a repo
  built for it, with an INFORM refresh this one never had.
* Good, because pushes to `main` stop redeploying a superseded app.
* Good, because this repo no longer holds credentials-bearing deploy config for
  an App Service it does not own.
* Bad, because `app/cerf_predictor.py` is now code with no deployment in this
  repo — a duplicate of the production copy. Accepted for now and recorded as
  option D above; the risk is that the two drift and someone edits the dead one.
* Neutral, because the slot was already returning HTTP 503, so no working URL
  is lost.

### Confirmation

Verified 2026-08-03 before deleting:

| | |
|---|---|
| `https://cerf-3rm.azurewebsites.net` (replacement) | **HTTP 200**, App Service `CERF-3RM` Running |
| `chd-ds-seas5-viz-cerf-rr-…azurewebsites.net` (this slot) | **HTTP 503** |

After the change: no reference to `cerf-rr` or `seas5-viz` remains anywhere in
this repo, and `az webapp deployment slot list` for `chd-ds-seas5-viz` shows
`development` still present.

## More Information

* Replacement app: `ocha-dap/ds-cerf-3rm-app` — KB page `apps/cerf-3rm-app.md`.
* The KB's `infrastructure/deployments.md` and
  `pipelines/storm-impact-harmonisation.md` both listed this slot as live; both
  are being corrected alongside this decision.
* Same shape as [0004](0004-retire-the-cyclone-exposure-dashboard.md) —
  a deployment whose job is now done elsewhere — but with the opposite call on
  deletion, because here the code demonstrably lives on in a maintained repo
  rather than only on an unmerged branch.
