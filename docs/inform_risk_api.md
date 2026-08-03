# DRMKC INFORM Risk API

Source for INFORM Risk scores (HA, VU, CC, overall INFORM) used as the
vulnerability input to the predictor. Operated by the EU JRC.

| | |
|---|---|
| Base URL | `https://drmkc.jrc.ec.europa.eu/inform-index/API/InformAPI` |
| Auth | none |
| CORS | `Access-Control-Allow-Origin: *` |
| Content type | `application/json; charset=utf-8` |
| Cache | `Cache-Control: no-cache` (server doesn't cache; add your own) |
| Rate limits | none observed (10 sequential calls in ~7 s, all 200) |

## Endpoints

| Endpoint | Purpose |
|---|---|
| `GET /Workflows` | Array of all 142 workflow definitions. |
| `GET /Workflows/Default` | Single object — currently `WorkflowId=505` "INFORM Risk 2026", `GNAYear=2025`, published 2026-03-23. |
| **`GET /Countries/Trends/?WorkflowId={id}`** | **The endpoint we use.** Time-series of pillar scores across years for all countries. Trailing slash is mandatory. |
| `GET /Indicators` | 345 indicator definitions (pillars + sub-pillars + ~330 leaf indicators). |
| `GET /Countries` | 6,887 admin units (242 ADMIN0 ISO3 + 3,157 ADMIN1 + regional categories). |

`/Methodologies`, `/Regions`, `/Categories`, `/Workflows/{id}`,
`/Indicators/Trends`, `/Countries/Profile`, `/Countries/Compare` all return
404. The `/Workflows/Default/Countries` path appears to be a no-op alias —
returns the same default workflow object, not a country list.

## The publishing-lag pattern (important)

Each annual edition is named for the year it's *for* (`INFORM Risk 2026`),
but its data goes only through `GNAYear = year - 1`. So on 2026-04-27:

- WF 505 ("INFORM Risk 2026") → returns `GNAYears 2016–2025`. There is no
  GNAYear 2026 row.
- WF 469 ("INFORM Risk 2024") → `GNAYears 2015-2024`.

The `/Workflows/Default` endpoint always points at the latest *published*
edition and rolls over each March or so. Predictions for the current
calendar year therefore have to forward-carry the last published GNAYear —
this is what `scripts/refresh_inform_composite.py` does.

### Workflow IDs we've encountered

| ID | Name | Published? | GNAYears returned |
|---|---|---|---|
| 469 | INFORM Risk 2024 | yes | 2015-2024 |
| 480 | INFORM Risk Mid 2024 | yes | mid-year refresh, single year |
| 481 | INFORM Risk 2025 | **no** (`FlagGnaPublished is null`) | `[]` empty — never released |
| 482 | INFORM Risk 2025 with UCDP | yes | 2015-2024 |
| 493 | INFORM 2025 2nd edition | yes | 2015-2024 |
| 503 | INFORM Risk Mid 2025 | yes | 2025 only (mid-year refresh) |
| **505** | **INFORM Risk 2026** | **yes (default)** | 2016-2025 |
| 506-514 | INFORM Risk 2026 — {year} | yes | per-year sub-workflows; usually you don't want these — the umbrella ID 505 returns everything in one call |

To list only queryable workflows, filter on `FlagGnaPublished is not None`.
Don't filter on `Version == "Trends"` — only WF 493 and 505 carry that
field, but older published workflows still serve Trends data.

## Quirks of `/Countries/Trends/`

1. **`IndicatorId` parameter is silently ignored.** Whether you pass
   `IndicatorId=INFORM,HA,VU,CC`, just `IndicatorId=INFORM`, or omit the
   param entirely, you get the same 7,640-row response containing all four
   pillars. This is harmless for our use (we want all four) but worth
   knowing — sub-pillar codes like `HA.NAT`, `VU.SE`, `CC.IN` cannot be
   retrieved through this endpoint. For sub-pillar breakdowns you'd need
   the GIS download or bulk Excel.
2. **Trailing slash required.** `/Countries/Trends` (no slash) → 404.
3. **No null `IndicatorScore` values observed** for WF 505.
4. **Country coverage = 191 ADMIN0 ISO3 codes** per pillar per year (less
   than the 242 in `/Countries`).
5. Score scale 0–10. Verified e.g. SGP/INFORM/2025 = 0.7.

## Field semantics in the Trends response

| Field | Meaning | Useful? |
|---|---|---|
| `IndicatorId` | Short code: `INFORM`, `HA`, `VU`, `CC` | yes — pivot key |
| `FullName` | Display name ("Hazard & Exposure Index" etc.) | yes — for plots |
| `Iso3` | ISO3 country code | yes |
| `CountryName` | always empty in Trends rows | no — use `/Countries` lookup |
| `GNAYear` | int year (2016, 2017…) | yes — primary join key |
| `IndicatorScore` | float 0–10 | yes |
| `WorkflowId` | echo of request param | mostly for sanity-checks |
| `MethodologyId` | one per (workflow, GNAYear); unique within a workflow but not human-meaningful | provenance only |
| `StepNumber` | aggregation step in INFORM tree (`INFORM=17`, pillars=16) | only matters if you expose deeper levels |
| `ParentLevel` | always 0 for the four pillars | reserved for nested indicators not exposed here |
| `Ranking` | always 0 | reserved/UI; ignore |
| `ShortDescription` | always empty in Trends rows | ignore |

## Working snippets

```python
import requests
import pandas as pd

BASE = "https://drmkc.jrc.ec.europa.eu/inform-index/API/InformAPI"

# 1. Resolve current default workflow
wf = requests.get(f"{BASE}/Workflows/Default", timeout=30).json()
print(wf["Name"], "→ latest GNAYear", wf["GNAYear"])

# 2. Fetch the full panel of pillar scores
r = requests.get(
    f"{BASE}/Countries/Trends/",
    params={"WorkflowId": wf["WorkflowId"]},
    timeout=60,
)
r.raise_for_status()
df = pd.DataFrame(r.json())
wide = (
    df.pivot_table(
        index=["Iso3", "GNAYear"],
        columns="IndicatorId",
        values="IndicatorScore",
    )
    .reset_index()
)

# 3. List only published workflows
wfs = requests.get(f"{BASE}/Workflows", timeout=30).json()
published = [
    w for w in wfs
    if w["FlagGnaPublished"] is not None and w["System"] == "INFORM"
]
```

## Reference implementation

`src/datasets/inform.py::fetch_inform_risk()`. Despite the silently-ignored
`IndicatorId` param, the function works correctly because it asks for the
4 pillars it actually wants.
