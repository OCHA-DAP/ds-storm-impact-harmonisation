# Three-source storm-exposure source comparison (`src/source_exposure`)

Compares storm-level population-exposure estimates from three sources —
**CHD** (our NHC-derived observed + forecast-only exposure), **GDACS**, and
**ADAM (WFP)** — at **admin-0** and **admin-1**, on each storm's **final
("last-ballot")** estimate.

## What it produces

- **`out/historical_tropical_cyclone_pop_exposure_estimates_AL_EP_basins.xlsx`**
  — a styled workbook for colleagues. Tabs: `storms` (every NHC storm from 2001
  on — which sources report exposure, which have it on record, and a
  `note_gdacs_adam` explaining every GDACS/ADAM gap), `adm0_exposure`,
  `adm1_exposure`, `caveats`, and a README tab. Built by `workbook.py`.
- **A source-coverage diagnostic** (`source_diagnostics.py`) — for every GDACS
  and ADAM NOAA event, *why* we do or don't hold its exposure (`have_exposure`
  / `reported_zero` / `partial_no_final` / `unservable` / `csv_403` / …).
  Read-only; the canonical copy is uploaded to blob at
  `dev:projects/ds-storm-impact-harmonisation/processed/adam_gdacs_per_storm_source_diagnostics.csv`.
- **A reusable admin-1 matching module** (`fm_matching.py`) — maps GDACS/ADAM
  admin units onto FieldMaps pcodes and attaches the harmonisation caveat. It is
  agnostic to time-selection, cross-source aggregation, and blanking (each
  consumer keeps those), so it is **vendored into `ds-storms-alerts`** (PR #13)
  and the alert pipeline + this comparison match admins through one tested path.

## Files

| file | role |
|---|---|
| `workbook.py` | builds the styled Excel workbook (the headline deliverable) |
| `fm_matching.py` | shared pure-pandas GDACS/ADAM → FieldMaps adm1 matcher + caveat helpers (vendored to ds-storms-alerts) |
| `source_diagnostics.py` | GDACS/ADAM coverage diagnostic; uploads to blob; `load_status()` feeds the storms tab |
| `queries.py` | vendored/forked storms.* SQL (storm-final snap); adm1 matching delegates to `fm_matching` |
| `build.py` | reproducibility driver (probe → blob → workbook, across the two envs) |
| `style.py` | openpyxl styling (matches the OCHA book theme) |
| `out/` | generated outputs (gitignored) |

## Run

Two environments by design — the diagnostic probe needs `ocha_lens`
(the ds-storms-pipeline venv), the workbook needs `openpyxl` (the harmonisation
venv); the blob-persisted diagnostic is the clean hand-off. `build.py`
orchestrates both:

Each step runs as a module from the repo root (so `from src.source_exposure
import …` resolves):

```bash
python -m src.source_exposure.build                       # rebuild the workbook from the canonical blob diagnostic (fast)
python -m src.source_exposure.build --refresh-diagnostic  # re-run the ~15-20 min GDACS/ADAM probe first, re-upload to blob
```

Needs `DSCI_AZ_*` env + dev DB/blob access.

## Design decisions (the non-obvious ones)

- **Snap = storm-final.** Each unit's latest `valid_time` (no advisory window;
  `DISTINCT ON … ORDER BY valid_time DESC`). This is the one real divergence
  from the alert pipeline (which is advisory-window) — and exactly why the
  matching is factored into `fm_matching` (time-agnostic) while the snap stays
  in the caller (`queries.py`).
- **Join unit:** `iso3` at adm0 (the unit *is* the country, which every source
  carries cleanly); `fm_pcode` at adm1 (lookups essential — GDACS/ADAM admins
  with no FieldMaps match surface as orphan rows; the workbook drops them, ~1.5%
  of GDACS / ~3% of ADAM adm1 exposed population).
- **Zero vs blank:** CHD is our own NHC DB, so a missing value for a storm we
  have is a true 0. GDACS/ADAM show 0 only when the source *reported* the storm;
  where a source is blank the storms tab's `note_gdacs_adam` says why (reported
  zero / GDACS server error 500 / WFP access denied 403 / no record).
- **Thresholds:** 34 & 64 kt are common to all three; GDACS has no 50 kt, so
  50 kt is CHD-vs-ADAM only.

## Early findings (from an exploratory stats pass)

1. **GDACS ≈ ADAM** — Spearman 0.96–0.997; near-duplicates, since ADAM ingests
   GDACS upstream (in `storm_id_lookup` every `adam_eventid` equals its
   `gdacs_eventid`).
2. **CHD systematically lower than GDACS/ADAM, and the gap widens with wind
   threshold** — median GDACS/CHD ≈ 1.3× at 34 kt → ~2–3× at 64 kt; rank
   ordering still agrees well.
3. **adm1 coverage differs sharply** — ADAM has the broadest subnational
   coverage, GDACS is sparse at adm1, CHD has its own units.

## Keeping `fm_matching` in sync

`fm_matching.py` is the **source of truth** here
(`src/source_exposure/fm_matching.py`); **ds-storms-alerts** carries a
byte-identical **vendored** copy (`src/fm_matching.py`, differing only by a
provenance docstring). Any edit here must be mirrored there — tracked in
ds-storms-alerts#14 / ds-storm-impact-harmonisation#8. A
`book/NN-source-distribution.qmd` chapter can read the workbook / `out/*`
directly when one is wanted.
