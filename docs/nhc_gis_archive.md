# NHC GIS archive — advisory wind radii, cones and watch/warning geometry

NHC publishes **every archived advisory as shapefiles**, including the wind-radii
polygon it actually issued. That makes NHC's own depiction of a storm a
queryable geometry rather than a screenshot — which is what lets us settle
"whose footprint is right" arguments against the forecaster's own product.

No auth, no key, no rate limit encountered. Verified **2026-07-31** against
`al082023` (Franklin). If you come back in 6 months, re-verify before relying on
edge cases.

| | |
|---|---|
| Listing (wind field) | `https://www.nhc.noaa.gov/gis/archive_forecast_info_results.php?id=<basin><num>&year=<YYYY>` |
| Listing (track/cone) | `https://www.nhc.noaa.gov/gis/archive_forecast_results.php?id=<basin><num>&year=<YYYY>` |
| Download | `https://www.nhc.noaa.gov/gis/forecast/archive/<file>.zip` |
| `id` format | `al08`, `ep15`, … (basin + 2-digit storm number, **not** the full ATCF id) |
| Licence | US Government work, public domain |

## The two packages

### `<atcf>_fcst_NNN.zip` — the wind field ← **the useful one**

| Layer | What it is |
|---|---|
| `<atcf>_<YYYYMMDDHH>_initialradii.shp` | The wind field **at advisory time** (TAU 0). One feature per wind threshold present. |
| `<atcf>_<YYYYMMDDHH>_forecastradii.shp` | Forecast radii at each TAU (0, 12, 24, 36, 48, 60, 72 h). Its TAU-0 row duplicates `initialradii`. |

Attributes on both:

| Field | Notes |
|---|---|
| `RADII` | wind threshold in kt — **34, 50, 64**. Filter on this; a storm can have all three. |
| `ADVNUM` | advisory number; matches the `NNN` in the filename |
| `VALIDTIME` / `SYNOPTIME` | `YYYYMMDDHH`, UTC |
| `TAU` | forecast hour (0 = analysis) |
| `NE` `SE` `SW` `NW` | the four quadrant radii, **nautical miles** |
| `STORMID` `BASIN` `STORMNUM` | `al082023`, `al`, `8` |

The geometry is the quadrant polygon itself — NHC has already done the
interpolation, so you can use it directly instead of rebuilding from the radii.

### `<atcf>_5day_NNN.zip` — track, cone and warnings

| Layer | What it is |
|---|---|
| `<atcf>-NNN_5day_lin` | forecast track line |
| `<atcf>-NNN_5day_pgn` | cone of uncertainty polygon |
| `<atcf>-NNN_5day_pts` | forecast points — `MAXWIND`, `GUST`, `MSLP`, `SSNUM`, `DVLBL`, `FCSTPRD` |
| `<atcf>-NNN_ww_wwlin` | **watch/warning coastal segments** — field `TCWW` carries the class (`TWA` tropical-storm watch, `TWR` tropical-storm warning; hurricane equivalents likewise) |

`ww_wwlin` is the machine-readable answer to "was this coastline under a
warning?" — far better than parsing the TCR text.

## Gotchas

1. **The CRS is a sphere, not WGS84.** The `.prj` reads
   `GCS_Sphere / D_Sphere / SPHEROID["Sphere", 6371200, 0]`; geopandas surfaces it
   as *"Unknown datum based upon the Authalic Sphere"* and will refuse to overlay
   it with EPSG:4326 layers. At these scales the difference is immaterial, so:

   ```python
   g = gpd.read_file(shp).set_crs(4326, allow_override=True)
   ```

   Use `set_crs(..., allow_override=True)`, **not** `to_crs` — there is nothing to
   reproject, the datum label is simply wrong for our purposes.

2. **Advisory numbering is not guaranteed contiguous.** The listing pages show
   intermediate advisories with a letter suffix (`001A`, `002A`), and the plain
   `_fcst_NNN` sequence can have holes. Loop with a `try/except` per advisory
   rather than assuming `1..N` all exist.

3. **`initialradii` ≠ the cumulative swath.** Each advisory is an instantaneous
   wind field. To get the storm's whole footprint you must union across
   advisories — which is what makes it comparable to a CHD/GDACS storm buffer.

4. **A storm can be missing a threshold.** If it never reached 64 kt there is no
   `RADII == 64` feature; filter, don't index positionally.

## Worked example — Franklin 2023 (`al082023`)

Used in [`gdacs_adam_wind_footprint.md`](gdacs_adam_wind_footprint.md) to test
whose buffer geometry NHC's own product supports.

```python
import glob, geopandas as gpd, pandas as pd
from shapely.ops import unary_union

parts = []
for shp in sorted(glob.glob("al082023_fcst_*/*initialradii.shp")):
    g = gpd.read_file(shp).set_crs(4326, allow_override=True)
    parts.append(g[g.RADII == 34])
nhc_footprint = unary_union(pd.concat(parts).geometry.tolist())
```

Findings (20 advisories):

- Every advisory during the Hispaniola crossing carried `NE 100, SE 100, SW 0,
  NW 0` — **zero 34 kt extent to the west**, where Haiti is.
- The unioned NHC footprint covers **0.0% of Haiti** and 53.1% of the Dominican
  Republic — i.e. it matches CHD's asymmetric polygon (0.0% / 57.6%), **not** the
  GDACS max-radius circle (45.2% / 87.1%).
- From `ww_wwlin`: advisories **008–014 carried tropical-storm warning (`TWR`)
  segments touching Haiti's coast** (008 also a `TWA` watch). So NHC warned
  Haiti while placing no 34 kt wind field over it — a warning is a statement
  about risk, not an analysis of the wind field. Worth keeping straight when
  someone says "but NHC warned them".

Reproduce: `uv run python artefacts/slides/franklin_case.py` (caches the archive
to `artefacts/slides/cache/nhc_gis/`).

## Relationship to what we already have

`storms.nhc_tracks_geo` already carries `quadrant_radius_{34,50,64}` per track
point, ingested via `ocha-lens`, and `ocha_lens.utils.storm` builds buffers from
them — so for routine pipeline work **you do not need this archive**.

Reach for it when you need *NHC's own rendering* rather than a reconstruction:
adjudicating a footprint disagreement, checking whether a coastline was under a
warning, or pulling the cone. `ocha-lens` has no loader for it today; if this
becomes a recurring need, that is where it belongs (`ocha_lens.datasources.nhc`).

## What's NOT here

- NHC best-track / post-storm data — that is IBTrACS, already in
  `storms.ibtracs_tracks_geo`.
- Tropical Cyclone Reports (the post-storm PDFs):
  `https://www.nhc.noaa.gov/data/tcr/<ATCF>_<Name>.pdf`. Useful for observed
  winds; `pdftotext -layout` extracts them cleanly.
- Wind speed probability products (a different, probabilistic product) — see
  `storms.nhc_wsp_*` and book chapter `04-nhc-wsp-exploration.qmd`.
