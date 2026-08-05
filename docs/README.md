# API & data-source reference

Permanent reference for the external APIs and data sources this repo depends on.
Verified against live endpoints on the dates noted in each file. If you come back
to this in 6 months, re-verify before relying on edge cases — APIs drift.

> **The CERF / 3RM material moved.** The INFORM Risk, CERF GMS and FTS/HPC
> references, the ACAPS INFORM Severity notes, the 3RM v1.8 column gotchas and
> the predictor data-flow diagram now live in
> [`OCHA-DAP/ds-cerf-allocation-patterns`](https://github.com/OCHA-DAP/ds-cerf-allocation-patterns)
> under `docs/`, alongside the loaders and refresh scripts they document. See
> [ADR 0007](decisions/0007-move-the-3rm-work-to-its-own-repo.md).

## Storm exposure sources

Behind the three-source comparison (chapters 09–10) and the AAC deck.

| Source | What it gives us | Doc |
|---|---|---|
| **NHC GIS archive** | Per-advisory wind-radii polygons, cones, watch/warning segments — NHC's own geometry, no auth | [`nhc_gis_archive.md`](nhc_gis_archive.md) |
| **GDACS / ADAM footprint method** | Why GDACS reads systematically higher than CHD: max-radius circle vs quadrant polygon, **with citable sources** | [`gdacs_adam_wind_footprint.md`](gdacs_adam_wind_footprint.md) |
| **PDC Hazards API** | Candidate third exposure source. Evaluated ch. 08, re-evaluated on the 2026 season ch. 11; captured 3-hourly but not integrated (ADR 0005) | [`pdc_api.md`](pdc_api.md) |

## What this repo operates

One outbound pipeline, documented here because it is ours rather than an
external source.

| What | Notes | Doc |
|---|---|---|
| **GDACS & PDC monitor email** | 4×/day to Listmonk list 101. Two silent-failure traps in the GDACS query, PDC queried live rather than from the archive, and the `[test]` prefix is deliberate | [`monitor_email.md`](monitor_email.md) |

## What's NOT here

- Authentication / SAS-token rotation (see project root `CLAUDE.md`).
- Anything CERF-allocation or 3RM related — see the pointer above.
- Deployment pipeline (see `.github/workflows/deploy-app.yml`).
