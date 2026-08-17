---
status: accepted
date: 2026-08-17
decision-makers: ["@zackarno"]
consulted: []
informed: []
---

# Match GDACS storms to PDC hazards by position, not name

## Context and Problem Statement

The daily GDACS & PDC monitor email pairs each active GDACS cyclone with its
PDC hazard record. The original matcher compared name tokens (GDACS
`DOLPHIN-26` ↔ PDC `Typhoon Dolphin`), which worked for every named storm in
the capture archive — and then silently failed on 2026-08-17: GDACS carried
the Central Pacific storm as `ONE-C-26` (its pre-naming ATCF designation,
which GDACS keeps for the event's entire lifetime) while PDC carried the same
storm as `Tropical Storm Lala` (the CPHC-assigned name). The one storm PDC had
fresh data for rendered in the email as "not in PDC's feed" — a false negative
indistinguishable from real absence. Any storm that is named mid-life hits
this.

## Decision Drivers

- The email must not report a storm as absent from PDC when PDC carries it.
- Real absence (PDC closes cyclones once the agency stops issuing advisories;
  GDACS keeps events active through their date window) must still be reported
  as information, per the repo's fail-loudly rule.
- Matching should use data already present in the list calls the email makes.

## Considered Options

1. Match by position + recency, name kept as a corroboration signal
2. Match by ATCF ID
3. Keep name matching, add a designation→name exceptions mapping

## Decision Outcome

Chosen option 1: **nearest `category == "EVENT"` PDC record within 500 km of
the GDACS storm position** (`match_gdacs_storm` in `src/datasets/pdc.py`),
with `names_agree` retained as a corroboration check whose disagreement is
printed loudly, not acted on.

Both feeds republish the issuing agency's advisory position, so an in-sync
pair is metres apart (ONE-C/Lala were bit-identical: 20.4°N 163.4°W on both
sides) and an out-of-sync pair is off by at most one 6-hour advisory step
(~200 km). Simultaneous cyclones in one basin sit many hundreds of km apart,
so 500 km is generous without being ambiguous. Distances use haversine, which
handles the dateline crossing that WP/CP storms routinely make.

### Consequences

- Good: designation-stage storms (ONE-C, FIFTEEN, …) match without any name
  knowledge; both position sources are in the list responses already fetched.
- Good: a proximity match whose names disagree is logged as a NOTE, so a
  genuinely wrong pairing would be visible in the run log rather than silent.
- Bad: `fetch_active_cyclones` now raises if a PDC feature lacks Point
  geometry (position became load-bearing); a PDC schema change would fail the
  email run rather than degrade it — intended, per the fail-loudly rule.

### Confirmation

`tests/test_pdc_matching.py` covers same-coordinate, one-advisory-offset,
beyond-threshold, RESPONSE-exclusion, nearest-of-several and dateline cases.
Live run 2026-08-17: ONE-C-26 matched Lala at 0 km (advisory 21, CP012026);
HERNAN-26 and NANGKA-26 correctly reported as closed in PDC.

## Pros and Cons of the Options

### Match by ATCF ID

PDC exposes `atcf_id` only in the per-hazard detail response, and GDACS
exposes no ATCF field at all (`sourceid` is empty on every TC probed; `glide`
is a GLIDE number, assigned late). The ID would have to be parsed out of
GDACS's *name* — a number-word table ("ONE-C" → CP01) plus basin inference —
and a PDC detail fetch made per candidate before comparing. Precise when it
works, but one-sided, derivation-heavy, and no more precise in practice than
position.

### Keep name matching + exceptions mapping

A hand-maintained designation→name table is exactly the failure mode ADR 0005
noted in the GDACS→IBTrACS name-matching path: it silently goes stale and
needs a human to notice each new unnamed storm. Rejected.

## More Information

Root-cause investigation in the 2026-08-17 session log; PDC schema notes in
`docs/pdc_api.md`. Related: [0005](0005-capture-pdc-cyclones-without-integrating-them.md).
