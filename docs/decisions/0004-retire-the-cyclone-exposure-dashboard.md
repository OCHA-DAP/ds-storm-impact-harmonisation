---
status: proposed
date: 2026-07-31
decision-makers: Zack Arno
consulted: Tristan Downing
informed: Hannah Ker, OCHA CHD Data Science team
---

# Retire the Cyclone Exposure Dashboard rather than republish it

## Context and Problem Statement

The Cyclone Exposure Dashboard was the first thing this project published, and it
established the Pages site. Its GDACS pipeline was adapted into
`ds-storms-pipeline` and is still in use.

Its country-level exposure view is now covered by the storm exposure comparison
app at `/compare/`, which extends the same idea across three sources with
configurable triggers. The dashboard is no longer actively developed.

Its source was never on `main`. It lives on the `gdacs-adam-data`
branch (PR #2), reached by an `actions/checkout` step pinned to that branch — a
step whose own comment noted that the deploy stops working, silently, if the
branch is ever deleted.

Rebuilding the Pages site around a landing page (see
[0003](0003-manifest-driven-multi-product-pages-site.md)) forced a decision,
because every way of continuing to publish it carried an ongoing cost.

## Decision Drivers

* The site should not depend on a branch that is not scheduled to merge.
* `main` contains no committed datasets, and that is worth preserving.
* Nothing published should start returning 404 without a sensible landing point.
* The author's work should remain reachable, and the decision reversible.
* No part of this should block on someone no longer working on the project.

## Considered Options

* **A — Retire it.** Do not publish it. Close PR #2, leaving the branch intact.
* **B — Vendor its four static files** into the new `pages/products/` layout and
  publish it at `/dashboard/`, marked deprecated.
* **C — Keep the branch checkout**, adding a `branch` source type to the generator.
* **D — Merge PR #2** so the dashboard becomes an ordinary in-repo product.
* **E — Regenerate its data at deploy time**, matching how the comparison app works.

## Decision Outcome

Chosen option: **A, retire it.**

Each alternative keeps a recurring cost in the system in exchange for continuing
to serve a page whose job is now done elsewhere. B commits 1.5 MB of frozen JSON
to a `main` that has none. C keeps an inactive branch on the critical path of
every deploy. D brings 86,337 lines onto `main` — the branch is a complete
project in its own right, and only four of its files are the dashboard — and
overlaps PR #3 on four config files. E is the largest: `join_historical_exposure.py`
depends on `constants.py` and both upstream pipelines (~1,196 lines), plus
`ocha_stratus`, `pycountry` and `requests` in CI, plus live GDACS and ADAM API
calls, plus **prod** database credentials in a workflow that currently holds only
dev ones — a credential escalation, where a failure in that path would block
every other product's deploy.

Retiring the page removes the branch dependency, the committed data, and the only
product whose source was not on `main`, in one step.

**Nothing is deleted.** PR #2 is closed rather than merged; closing removes
neither the branch, the commits, nor the PR's diff, all of which stay reachable
on GitHub indefinitely. The decision is reversible by reopening.

A dependency audit confirmed nothing in the workspace imports code existing only
on that branch. The related identifiers in `ds-storms-alerts`
(`fetch_adam_historical_exposure`, `fetch_gdacs_historical_exposure`) are locally
defined functions issuing SQL against Postgres — what is shared is table names,
not code. The GDACS pipeline logic was independently adapted into
`ds-storms-pipeline`.

### Consequences

* Good, because the deploy no longer checks out a feature branch, eliminating the
  silent-failure mode when that branch is deleted.
* Good, because `main` keeps its property of containing no committed datasets.
* Good, because the landing page work does not block on an unavailable colleague.
* Good, because the deprecated `status` field in the manifest schema is now
  unused, so the site has no retired products presented as if current.
* Bad, because the dashboard is no longer viewable without checking out the
  branch and running it locally — three commands, documented below. Accepted:
  the analysis it presented is covered by the Quarto book, and its function by
  `/compare/`.
* Neutral, because the URL it occupied (`/`) now serves the landing page, so
  existing links resolve to an index of current work rather than a 404.

### Confirmation

The assembled site contains no `dashboard` route, the workflow contains no
`actions/checkout` step referencing `gdacs-adam-data`, and `git log --stat`
shows no committed JSON. `/`, `/compare/` and `/slides/aac/` serve correctly.

## More Information

**Where the dashboard now lives.** Branch `gdacs-adam-data`, PR
[#2](https://github.com/OCHA-DAP/ds-storm-impact-harmonisation/pull/2), closed
unmerged. The dashboard itself is four files at the branch root — `index.html`,
`assets/styles.css`, `assets/exposure_data.json`,
`assets/adm1_exposure_data.json` — and is self-contained: it fetches only those
two JSON files, links that stylesheet, and otherwise loads only Google Fonts. To
view it:

```bash
git fetch origin gdacs-adam-data
git worktree add /tmp/dashboard origin/gdacs-adam-data
python3 -m http.server -d /tmp/dashboard 8000
```

The branch also carries `constants.py` and three pipeline scripts
(`adam_historical_exposure.py`, `gdacs_historical_exposure.py`,
`join_historical_exposure.py`) which regenerate those JSON files. They were
confirmed to have no dependents but were not reviewed for reference value; the
GDACS one has an adapted descendant at
`scripts/rebuild_gdacs_historical_exposure.py`.

Hannah Ker should be told once PR #2 is closed — not for approval, but so it is
not discovered cold.
