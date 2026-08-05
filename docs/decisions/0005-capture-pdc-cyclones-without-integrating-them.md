---
status: accepted
date: 2026-08-03
decision-makers: ["@zackarno"]
consulted: []
informed: []
---

# Capture PDC cyclones continuously without integrating them into the exposure pipeline

## Context and Problem Statement

`book/08` evaluated the PDC Hazards API as a third cyclone-exposure source
alongside ADAM and GDACS and recommended against integrating it. That
recommendation rested on a single observation — Tropical Storm Sinlaku, a
`category = RESPONSE` record hand-entered by an analyst after the storm had
dissipated, carrying one Point, no track, and zero exposure. The chapter set an
explicit re-evaluation trigger: *the next non-manual cyclone in the feed*.

The trigger fired in the 2026 Northern Hemisphere season. Probing on
2026-08-03 returned three cyclones, two with `category = EVENT` and automated
ingestion from official forecast centres (Typhoon Dolphin via JTWC,
Post-Tropical Cyclone Genevieve via NHC). Two of chapter 08's three supporting
arguments do not survive contact with that data:

- Automated cyclones carry a full forecast track with standard 34/50/64 kt
  quadrant wind radii and computed per-country exposure with ISO3 codes.
- They carry an ATCF ID (`WP122026`), which joins exactly to IBTrACS
  `USA_ATCF_ID` — a stronger key than the GDACS path, which matches on name
  plus season and needs a hand-maintained exceptions list for unnamed storms.

Compared at the same advisory (31), PDC and GDACS returned **identical**
position, intensity, and all twelve quadrant radii: both relay the same JTWC
bulletin unaltered. Their exposure estimates for Japan differ by about 8%
(PDC 1.42M vs GDACS 1.32M at 34 kt).

The third argument not only survives but is worse than chapter 08 described.
PDC serves no archive, and a cyclone detail object contains **no track history
at all** — Dolphin at advisory 31 returned nine positions of which the earliest
was the current synoptic hour, with advisories 1-30 absent entirely.

So the question is no longer "is PDC good data" but "what is it for, and what
do we do about the fact that its record is being destroyed continuously".

## Decision Drivers

- PDC history is **perishable**: unpolled advisories are permanently
  unrecoverable, and the feed drops storms essentially as soon as they end.
  (Corrected 2026-08-05: this driver originally said "~30 days after they
  end" — the real window is far tighter, which strengthens rather than
  changes the decision. See `docs/pdc_api.md`.)
- The historical harmonisation this book supports needs coverage back to CERF's
  2006 baseline, which PDC can never provide.
- Integration into a production pipeline is a durable commitment; the cyclone
  schema is still only observed across three storms, none of which made
  landfall.
- Capture is cheap (a few hundred KB per poll) and reversible; integration is
  neither.

## Considered Options

1. **Capture continuously; do not integrate.** Run a scheduled poller into raw
   blob storage, parse separately, defer any integration decision.
2. **Integrate as a third exposure source now**, alongside ADAM and GDACS.
3. **Do nothing, revisit later.** Keep chapter 08's recommendation as-is.
4. **Integrate directly into `ds-storms-alerts`** as a real-time corroborating
   source.

## Decision Outcome

Chosen: **option 1 — capture continuously, do not integrate.**

`scripts/poll_pdc_cyclones.py` runs every three hours via GitHub Actions,
writing raw unparsed JSON to
`ds-storm-impact-harmonisation/raw/pdc/cyclones/` as a poll log plus a
version store keyed on `(hazard.uuid, updatedAt)`. `src/datasets/pdc.py`
parses that archive separately.

Three-hourly rather than daily because the payload is forecast-only: the
synoptic advisory cycle is 6-hourly, PDC's ingest lag behind JTWC/NHC is
undocumented, and over-polling is free since an unchanged hazard rewrites the
same blob. A daily poll would silently discard roughly three of every four
advisories.

Capture and parsing are deliberately separate programs. PDC data cannot be
re-fetched once the window drops it, so parsing at capture time would bake
today's schema assumptions — formed from three storms — into an archive that
can never be rebuilt.

### Consequences

- Good: the historical record starts accumulating immediately, at the only
  cadence that can reconstruct per-advisory behaviour. Waiting has a permanent
  cost; capturing has a trivial one.
- Good: no production surface area is committed while the schema is still
  being learned. Nothing downstream depends on PDC.
- Good: raw storage means a later schema correction is a re-parse, not a
  re-collection.
- Bad: a scheduled job exists that no downstream consumer reads, which is a
  maintenance obligation with no immediate return, and will look like dead
  weight to anyone who does not read this ADR.
- Bad: the archive is only as good as the workflow's reliability, and the
  workflow depends on two repo secrets (`PDC_API_KEY`,
  `DSCI_AZ_BLOB_DEV_SAS_WRITE`) whose expiry would silently end capture.
- Neutral: PDC's role is reframed from historical exposure source to candidate
  real-time corroborating source — relevant to `ds-storms-pipeline` /
  `ds-storms-alerts`, not to this book's harmonisation work.

### Confirmation

- `book/11-pdc-2026-season.qmd` documents the evidence, pinned to a tracked
  snapshot in `book/_cache/11-pdc-2026-season/`.
- Capture is confirmed by the presence of successive `polls/<ts>/_list.json`
  entries in dev blob; a gap in that log means the run failed, which is why an
  empty poll still writes a list.
- Re-assessment is due after the first PDC cyclone that makes landfall, which
  would expose the `landfallAdmin0` / `hoursLandfall` / `categoryLandfall`
  fields and the band-to-wind-threshold mapping, both still unobserved.

## Pros and Cons of the Options

### Option 1 — Capture continuously; do not integrate

- Good: preserves a perishable record at near-zero cost.
- Good: keeps the integration decision open and evidence-based.
- Bad: an unused pipeline is a maintenance liability.

### Option 2 — Integrate as a third exposure source now

- Good: would give the comparison chapters a third opinion immediately.
- Bad: PDC cannot cover the historical period the comparison is about, so it
  would be a third source over a 2026-onward sliver only.
- Bad: commits to a schema observed on three storms, none landfalling.

### Option 3 — Do nothing, revisit later

- Good: no maintenance burden at all.
- Bad: strictly destroys value. Every day without polling is history that
  cannot be recovered, and "revisit later" would begin from the same empty
  archive.

### Option 4 — Integrate directly into `ds-storms-alerts`

- Good: matches what the data actually is — a real-time product with an
  independent exposure estimate over identical geometry.
- Bad: premature. No landfall has been observed, the damage-band semantics are
  undocumented, and alerting is the least forgiving place to learn a schema.
- Deferred, not rejected; this is the likely destination if the evidence holds.

## More Information

- `book/08-pdc-evaluation.qmd` — the original evaluation and its open questions.
- `book/11-pdc-2026-season.qmd` — the 2026-season re-evaluation.
- `docs/pdc_api.md` — endpoint and schema reference.

Incidental finding recorded during this work, not part of this decision:
`gdacs.get_active_cyclones()` omits the `alertlevel` parameter by default and
GDACS then returns only orange and red events, despite the docstring promising
"all levels". Typhoon Dolphin — a Category 2 typhoon that GDACS itself scored
at 1.3M exposed — is a Green event and does not appear. See chapter 11.
