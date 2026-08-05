---
status: accepted
date: 2026-08-05
decision-makers: Zack Arno
consulted: []
informed: OCHA CHD Data Science team
---

# Move the 3RM / CERF-prediction work out of this repo

## Context and Problem Statement

This repo accumulated two only-loosely-related bodies of work. One is storm
impact harmonisation: cyclone population exposure, the CHD/GDACS/ADAM
comparison, and the GDACS/PDC monitoring pipeline. The other is the **3RM** —
replicating and extending Rost (2025)'s CERF rapid-response regression model,
which grew from a storm-exposure feature experiment into four book chapters, its
own data loaders, two production model specs, and a deployed predictor app.

The two share almost nothing at runtime. The 3RM chapters do not use the
exposure pipeline, and the exposure work does not use INFORM, ACLED or IDMC.
The overlap is historical — the 3RM entered via the question "does cyclone
exposure improve the model for storms?" — plus one shared file,
`src/datasets/cerf.py`.

Meanwhile `ds-cerf-allocation-patterns` already existed as a CERF-focused
analysis repo, and the predictor app had already been moved to its own
production repo (ADR 0006). Keeping the 3RM here meant two repos both partly
about CERF allocations, and a maintainer of either having to reason about the
other.

## Decision Drivers

* Each repo should have one subject a reader can state in a sentence.
* Code should live next to the analysis that uses it and the scripts that feed
  it — not split across repos.
* Nothing should be deleted from here that is not verified present elsewhere.
* The live storm-exposure comparison app and the monitor email must not be
  disturbed.

## Considered Options

* **A — Move the whole CERF Predictions section out** (chapters, loaders,
  models, refresh scripts, API docs) to `ds-cerf-allocation-patterns`.
* **B — Leave it here and stop developing it.** Freeze the section as a
  historical record.
* **C — Move the chapters only,** leaving the loaders and models here as a
  shared library both repos import.
* **D — Move the 3RM here from `ds-cerf-allocation-patterns`** instead,
  consolidating in this direction.

## Decision Outcome

Chosen: **A — move the whole section out.**

Removed from this repo: `book/02b`–`02e`, `src/datasets/{inform,conflict,
conflict_features}.py`, `src/models/`, `app/cerf_predictor.py` (+ its
`pyproject.toml`/`uv.lock`), the four `scripts/refresh_*` producers,
`scripts/cache_conflict_engineered_features.py`, and
`docs/{cerf_gms_api,inform_risk_api,fts_hpc_api}.md` plus the CERF half of
`docs/README.md`.

B was rejected because a frozen section still has to be understood by anyone
touching `src/`, and its undeclared `plotnine` dependency means it cannot
actually be re-rendered without work — a "frozen" section that cannot be built
is a trap, not a record. C was rejected because a two-repo shared library is
the worst of both: a cross-repo dependency to version, for code with exactly
one consumer. D was rejected because the exposure pipeline, the DB, the monitor
email and the Pages deployment are all rooted here and are much heavier to move
than four chapters.

This also executes **option D of [ADR 0006](0006-retire-the-cerf-rr-deployment-slot.md)**,
which retired the predictor's deployment but explicitly deferred deleting its
source. That deferral was on the grounds that chapter 02c and `docs/README.md`
still described the predictor's lineage — both of which are resolved by moving
them too.

### What stayed, and why

`src/datasets/cerf.py` **stays here.** It is genuinely shared:
`01-data-merge.qmd` and `02-analysis.qmd` import `build_analysis_dataset`,
`DEFAULT_MANUAL_OVERRIDES`, `ISO2_TO_ISO3`, `pivot_ocha_wide`,
`CERFCODE_TO_SID`, `remove_col_outliers_iqr` and `load_3rm_cirv` from it. The
new repo carries only its 3RM/API half; the storm sid-matching and exposure
paths remain here. The two copies are expected to diverge and that is fine —
they serve different questions.

`app/` also stays: `app.js`, `index.html` and `style.css` are the live storm
exposure comparison app that `deploy-app.yml` ships to Pages. Only the
predictor's files were removed from inside it.

### Consequences

* Good, because each repo now has one subject, and the 3RM loaders sit next to
  the chapters and refresh scripts that use them.
* Good, because the four `refresh_*` scripts moved *with* their consumers. They
  are the only producers of the blobs the 3RM code reads; leaving them here
  would have left the chapters rendering off blobs nobody could regenerate.
  `refresh_acled_monthly.py` moved for the same reason even though it has no
  consumer in either book — the deployed app reads its output.
* Good, because `book/index.qmd` and the book subtitle were refocused on what
  remains, in a separate commit on this branch. The landing page had been framed
  entirely as Phase 1 (`02b`) / Phase 2 (`02c`), and the subtitle "Linking CERF
  allocations with cyclone population exposure" no longer described the book. It
  now carries a pointer to the new repo, since that is what many readers will
  arrive looking for.
* Good, because `plotnine` was declared in the same pass. It had been imported
  by six chapters and declared nowhere, rendering only from `_freeze`; four of
  the six left with the 3RM, but `01-data-merge.qmd` and `02-analysis.qmd` still
  import it and would have failed on the next edit.
* Bad, because `02-analysis.qmd` is now the only remaining CERF-allocation
  analysis and it is not listed in `book/_quarto.yml` — it is an orphan file
  that is never rendered, yet it is the reason `src/datasets/cerf.py` must keep
  `load_3rm_cirv`. Pre-existing, surfaced by this work, not resolved by it:
  deciding whether to wire it into the book or delete it is a separate call.

### Confirmation

Verified before deleting anything:

| Check | Result |
|---|---|
| Four chapters present in the new repo as `book/11`–`14` | diff vs originals is import paths, chapter xrefs, and one improvement (ch13 auto-builds its feature cache) |
| Six modules present there | symbol sets identical except `cerf.py` (deliberate split) and `inform.py` (`_3RM_BLOB` now shared) |
| Predictor app + its modules preserved | `OCHA-DAP/ds-cerf-3rm-app`, branch `port-app` |
| Refresh scripts import cleanly there | all four execute their module body and expose `main()` |
| No dangling references left here | `git grep` for the deleted modules returns nothing outside `docs/decisions/` |
| Remaining code still imports | `src.datasets.{cerf,gdacs,pdc}` import, with every symbol the two remaining CERF chapters need |

## More Information

* New home: [`OCHA-DAP/ds-cerf-allocation-patterns`](https://github.com/OCHA-DAP/ds-cerf-allocation-patterns)
  — chapters 11–14 under "Part F: CERF Allocation Prediction (3RM)".
* Deployed predictor: `OCHA-DAP/ds-cerf-3rm-app` — note its `main` currently
  holds only `.gitignore`; the app lives on the unmerged `port-app` branch.
  Worth merging so the default branch reflects what is deployed.
* [ADR 0001](0001-country-differentiation-in-the-3rm.md) is kept here as the
  historical record of a decision taken in this repo, with a banner pointing at
  the live copy in the new repo.
* [ADR 0006](0006-retire-the-cerf-rr-deployment-slot.md) — retired the
  deployment; this ADR completes its deferred option D.
