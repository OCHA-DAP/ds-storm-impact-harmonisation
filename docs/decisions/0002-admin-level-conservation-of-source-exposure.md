---
status: accepted
date: 2026-06-24
decision-makers: ["@zackarno"]
consulted: []
informed: []
---

# Admin-level conservation of per-source exposure: CHD = observed-final only; accept residual GDACS/ADAM non-conservation

## Context and Problem Statement

The three-source comparison (`book/09`) and the MAX consolidation rule
(`book/10`, mirrored by the alert pipeline) compare and combine CHD, GDACS and
ADAM exposure at both the national (adm0) and subnational (adm1) level. Two
implicit assumptions underlie that:

1. **Like-for-like values** — each source's number means the same thing (each
   storm's final realized footprint), so a ratio or a MAX is meaningful.
2. **Admin-level conservation** — a source's adm0 figure equals the sum of its
   adm1 units (`adm0 = Σ adm1`), so national and subnational views reconcile and
   `sum-of-maxes ≥ max-of-sums` holds for the MAX screen.

Auditing the harmonised panels (`build_exposure`) against these assumptions
surfaced a clear bug on our side and a residual inconsistency on the external
sources' side:

- **CHD did not conserve.** `fetch_chd` defined CHD as `observed-final +
  forecast-only-final`, taking each unit's *latest* `nhc_tracks_fcastonly_exposure`
  row. Forecast-only is a *per-issuance* quantity — the forecast wind buffer minus
  the cumulative observed swath at that `issued_time` — that ramps then decays to
  ~0 as a storm completes. A unit stops getting rows once the storm passes it, so
  its "latest" row is a stale pre-observation peak. Summing those per-unit peaks
  over-counted at adm1 (e.g. Isaias/USA 34 kt: adm0 forecast-only **0.2M** vs
  Σ adm1 **20M**), inflating CHD's adm1 total to **1.35×** its adm0 and breaking
  conservation.
- **GDACS / ADAM do not fully conserve either.** GDACS adm1 sums to **~0.94×** its
  adm0 (FieldMaps matching loss + a native getimpact-vs-admin1 mismatch); ADAM is
  ~**1.00×** on average but with per-country spread. There is also a GDACS
  `adm0 = NaN` / `adm1 = 0` mismatch: ~**731** recent-era subnational units carry a
  true-zero GDACS value in countries GDACS does not report at adm0.

The question is what to fix and what to accept.

## Decision Drivers

* Apples-to-apples comparison across sources and admin levels.
* Conservation (`adm0 = Σ adm1`) so national and subnational figures tie out and
  the MAX screen behaves predictably.
* Do **not** fabricate or apportion external-source data we cannot verify.
* Production simplicity — these panels feed the harmonised workbook
  (`scripts/build_workbook.py`) and the alert MAX; corrections must flow through
  `build_exposure` without bespoke per-source reconciliation logic.

## Considered Options

* **Option 1 — Fix CHD (observed-final only); accept and document the residual
  GDACS/ADAM non-conservation.**
* **Option 2 — Also reconcile GDACS/ADAM** by apportioning each adm0 figure down
  to its adm1 units (or scaling adm1 to match adm0) so all three conserve.
* **Option 3 — Fix at source/pipeline level** — re-derive GDACS/ADAM subnational
  footprints and the FieldMaps matching so the stored values conserve natively.
* **Option 4 — Drop the subnational (adm1) comparison** and compare only adm0,
  sidestepping conservation entirely.

## Decision Outcome

Chosen option: **Option 1.** CHD is redefined as **observed-final only**
(`fetch_chd`, committed) — the realized track footprint, latest `valid_time` per
unit, with no forecast term. The GDACS/ADAM admin-level non-conservation is
**accepted as a known limitation and deferred**, surfaced in the chapters rather
than papered over.

Rationale:

* **CHD's was a true bug in our own combine logic**, with a clean fix. The
  pipeline never combines observed + forecast-only (they are separate, orthogonal
  bands); `observed + forecast-only-final` was a harmonisation invention. Dropping
  it makes CHD conserve **1.000×** and puts it on the same footing as GDACS/ADAM,
  which carry no forecast term (all three are each storm's final converged
  footprint — they ramp then plateau at the realized value).
* **GDACS/ADAM non-conservation originates in external data and the FieldMaps
  matching**, not in a combine bug we own. Apportioning (Option 2) would fabricate
  a subnational *distribution* the source never provided; we would rather expose
  the limitation than invent numbers.
* **The effect is modest and we already route around it.** Magnitude is reported
  on the **both-positive ratio** (≈ CHD 0.5× GDACS), not the comparable-set
  aggregate, which is the statistic the non-conservation distorts. GDACS's 0.94×
  and the bounded `adm0=NaN/adm1=0` mismatch do not change any headline.
* Option 3 is the *right* long-term fix but is a pipeline/source change of
  uncertain size (it touches `ds-storms-pipeline` and the FieldMaps lookups);
  Option 4 throws away the subnational layer that is a core deliverable.

### Consequences

* Good: CHD conserves exactly; the three-source comparison and the MAX are
  like-for-like; the harmonised workbook's CHD column is the realized footprint.
* Limitation (documented, not hidden): GDACS/ADAM subnational sums do **not** tie
  to their national totals, so the MAX screen will not perfectly reconcile across
  levels in *either* direction — **overshoot** (Σ adm1 MAX > adm0 MAX, ~33% of
  recent country-storms) from per-unit source-switching, and **shortfall**
  (Σ adm1 MAX < adm0 MAX, ~23%) where GDACS/ADAM set the national MAX but their
  sparser adm1 footprints cannot recover it. CHD never causes either.
* Open / deferred — **Option 3 as a follow-up** before the workbook circulates
  widely or if subnational reconciliation becomes a hard requirement: investigate
  (a) the GDACS FieldMaps adm1 matching loss (0.94×) and (b) the GDACS
  `adm0 = NaN` / `adm1 = 0` mismatch (the 731-unit, ~328M-CHD set). This likely
  warrants an issue in `ds-storms-pipeline` and/or the FieldMaps lookup.

### Confirmation

Re-run at render. The per-source conservation check (recent era, 34/64 kt) gives
**CHD 1.000×, ADAM 1.003×, GDACS 0.953×**; Isaias/USA reads observed-final
49.1M (adm0 = Σ adm1), not the 69.2M observed+forecast. `book/09`'s
*Appendix: comparing aggregate ratios across admin levels* shows the
comparable-set decomposition (0.37 adm0 → 0.39 same-countries → 0.86 all-adm1,
the gap being 731 units / 327.8M CHD / 0 GDACS). `book/10` §1 shows both
off-diagonal directions of the level (in)consistency. Any future change that
makes GDACS/ADAM conserve at source should supersede the deferral in this ADR.

## Pros and Cons of the Options

### Option 1 — Fix CHD; accept/document GDACS/ADAM

* Good, because it removes the one inconsistency we actually own, with a clean
  one-source fix that conserves exactly.
* Good, because it keeps `build_exposure` simple — no per-source reconciliation
  logic — and is honest about the residual limitation.
* Bad, because the subnational ↔ national views still will not perfectly tie out
  for GDACS/ADAM.

### Option 2 — Apportion GDACS/ADAM to conserve

* Good, because every source would then conserve and the MAX screen would tie out.
* Bad, because it **fabricates** a subnational distribution the source never
  reported; an apportioned adm1 figure is a modelling assumption masquerading as a
  measurement.

### Option 3 — Fix at source/pipeline level

* Good, because it is the principled fix — make the stored GDACS/ADAM subnational
  data conserve natively.
* Bad, because it is a cross-repo change (`ds-storms-pipeline` + FieldMaps
  matching) of uncertain scope, and the residual is small enough not to block the
  current deliverables. Recorded as the deferred follow-up.

### Option 4 — adm0-only comparison

* Good, because it sidesteps conservation entirely.
* Bad, because the subnational layer is a core output of the harmonisation and a
  primary use of the workbook.

## More Information

* The CHD fix: `src/source_exposure/queries.py` (`fetch_chd`); commit
  "fix: CHD exposure = observed-final only (drop forecast-only)".
* Source tables: `storms.nhc_tracks_obsv_exposure`,
  `storms.nhc_tracks_fcastonly_exposure` (forecast-only definition in
  `ds-storms-pipeline/src/schemas/sql/nhc_tracks_fcastonly_buffers.sql`),
  `storms.gdacs_exposure`, `storms.adam_exposure`.
* Chapters: `book/09-source-comparison.qmd` (Appendix), `book/10-max-methodology.qmd`
  (§1). Harmonised workbook export: `scripts/build_workbook.py`.
