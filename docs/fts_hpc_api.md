# FTS / HPC API

OCHA's Financial Tracking Service & Humanitarian Planning Cycle API.
Source for HRP/Flash Appeal funding requirements and individual donor
flows. Not currently wired into the predictor app. Reference scripts at
`/Users/zackarno/Documents/CHD/repos/ds-cerf-allocation-patterns/artefacts/scripts/`.

| | |
|---|---|
| Base URL | `https://api.hpc.tools/` |
| Auth | none for public endpoints |
| Backend version (verified) | HPC v3.89.0 (2026-04-27) |
| CORS | restricted; assume server-to-server only |
| Cache | `Cache-Control: no-store` |
| Rate limit | none observed at ~3 req/s; no `X-RateLimit-*` headers. Be polite. |

## Plan endpoints (`/v2/public/plan/...`)

Humanitarian Response Plans (HRPs), Flash Appeals, Regional Migrant
Response Plans, GHRP COVID supplements — all returned as "plans".

| Endpoint | Returns |
|---|---|
| `GET /v2/public/plan/country/{iso3}` | All plans ever published for a country. |
| `GET /v2/public/plan/{plan_id}` | Single plan. Same shape as country list entries. |
| `GET /v2/public/plan/year/{year}` | All plans active in `{year}`. |
| `GET /v1/public/plan/id/{plan_id}` | v1 alias of `/v2/public/plan/{plan_id}`. |
| `GET /v1/public/global-cluster` | List of 22 global clusters (id, name, code, type, parentId). |

`/v2/public/plan/{id}/governingEntities` returns 404. **There is no public
v2 path that returns plan-level cluster breakdowns directly** — but there
*is* a way via the flow endpoint, see "Cluster-level requirements" below.

### Plan response gotchas

1. **`origRequirements` and `revisedRequirements` are TOP-LEVEL fields**
   on each plan, NOT nested in `planVersion`. The internal prototype
   script `fts_api_prototype.py` (lines 104-105) reads them from
   `planVersion` — that returns `None` every time. **Bug to fix in any
   downstream script.**
2. **`years[*].year` is a string**, not int (`"2017"`, not `2017`).
   Integer comparison silently filters everything out.
3. `planVersion` holds metadata only: `name`, `shortName`, `subtitle`,
   `startDate`, `endDate`, `code`, plus version housekeeping.
4. Plan type is in `categories[*]` where `group == "planType"` and `code`
   is `FA` (Flash Appeal), `HRP`, `RMRP`, etc. More reliable than
   string-matching on `name`.

### Why country-level HRP totals don't match 3RM

For a sample of 12 3RM allocations:
- **1 matched exactly** — Madagascar 2017 Cyclone Enawo Flash Appeal
  ($20,067,549 ≈ $20M revised). The CERF allocation was the entire
  humanitarian response.
- **11 didn't** — country-level HRP totals are 10-50× larger than 3RM's
  `Total Amount Required` for the same country-year. (Somalia 2018 HRP
  $1.5B vs 3RM $24M; Afghanistan 2022 HRP $4.4B vs 3RM $110M.)

So `/plan/country/{iso3}` does **not** reproduce 3RM's value for protracted
crises. The next bullet is the actionable lead.

## Cluster-level requirements (KEY for 3RM reconstruction)

Cluster-level requirement breakdowns ARE available via the **flow**
endpoint, not the plan endpoint:

```python
import httpx
r = httpx.get(
    "https://api.hpc.tools/v1/public/fts/flow",
    params={"planid": 590, "groupby": "cluster"},
    timeout=30,
)
req = r.json()["data"]["requirements"]
# req["totalRevisedReqs"] -> 20_067_549
# req["objects"] -> [
#   {"id": ..., "name": "WASH", "objectType": "Cluster",
#    "origRequirements": ..., "revisedRequirements": ...}, ...
# ]
```

Tested for plan 590 (Enawo): 9 clusters returned, summing exactly to
the plan total. Yemen HRP 2023 (plan_id=1116) returned 18 clusters
summing to $4.34B.

**Important: `groupby=cluster` only returns cluster requirements when
scoped to a single `planid`.** Combining `countryISO3 + groupby=cluster`
returns `{totalRevisedReqs: null, objects: []}`.

If a future dynamic pipeline wants to approximate 3RM's
`Total Amount Required`: for each CERF allocation, get the project's
`ProjectSectorName` from CERF GMS, find the HRP/Flash-Appeal active in
that country-year, then look up the matching cluster's
`revisedRequirements`. This is a reconstruction, not a guarantee — the
match for the Enawo case worked because the Flash Appeal was scoped to
the same crisis. For protracted-crisis allocations the mapping is fuzzier.

## Flow endpoint (`/v1/public/fts/flow`)

Individual donor flows / contributions.

### Default response

```python
GET /v1/public/fts/flow?countryISO3={iso3}&year={year}
```

Top-level `data` keys: `incoming`, `outgoing`, `internal`, `flows`.
`incoming.fundingTotal` is the country-year aggregate of incoming flows.

### Grouped response

Add `&groupby=organization|cluster|plan|country|globalcluster` and the
shape changes:
- `data` keys become `report1, report2, report3, report4` and `requirements`.
- Totals at `data.report1.fundingTotals.total`.
- **`incoming` is absent**.

### Pagination — IMPORTANT

**`limit` is silently capped at 1000.** Requesting `limit=5000` or
`10000` still returns 1000 rows. Always use `meta.nextLink` to detect
more pages:

```python
all_flows, page = [], 1
while True:
    r = httpx.get(
        "https://api.hpc.tools/v1/public/fts/flow",
        params={"countryISO3": "YEM", "year": 2023, "limit": 1000, "page": page},
        timeout=60,
    )
    body = r.json()
    all_flows.extend(body["data"]["flows"])
    if not body.get("meta", {}).get("nextLink"):
        break
    page += 1
```

### Flow record structure

Each flow has `sourceObjects` and `destinationObjects` — lists of typed
dicts with discriminator `type`. Types include `Organization`, `Location`,
`UsageYear`, `GlobalCluster`, `Cluster`, `Plan`, `Emergency`, `Project`.

CERF-specific:
- CERF source `org_id = 4762`, `name = "Central Emergency Response Fund"`.
- Window flag in `flow["keywords"]` — values include `"Underfunded"`,
  `"Rapid Response"`.

Other useful fields: `amountUSD`, `flowType` (`Standard`, `Parked` —
Parked = preliminary CERF allocations not yet disbursed),
`status` ∈ {`commitment`, `paid`, `pledge`}, `date`, `decisionDate`,
`firstReportedDate`.

### Filter parameters that work

| Param | Notes |
|---|---|
| `countryISO3` | yes |
| `year` | yes |
| `planid` | all-lowercase |
| `organizationid` | all-lowercase; `organizationId` also accepted |
| `emergencyid` / `emergencyId` | yes |
| `boundary=incoming\|destination\|outgoing` | toggles which side of the country boundary to count |
| `groupby=organization\|cluster\|plan\|country\|globalcluster` | returns reports, not raw flows |

These return HTTP 400 "Invalid parameter":
`sourceOrganization`, `destinationCluster`, `donorId`, `fundingSourceId`.

### Combination gotcha

`organizationid=4762 & countryISO3=YEM & year=2023` returns **0 flows**
even though 55 CERF-sourced flows exist for that country-year. The
filters appear to AND on the country's own organization list rather than
the source-side. **Reliable pattern**: pull all country-year flows, then
post-filter by source org id client-side.

### No single-flow detail endpoint

`/v1/public/fts/flow/{flow_id}` → 404. `?id=...` → 400. You only get full
flow records as items in the list response.

## Requirements vs commitments vs flows — terminology

| Concept | Field | Endpoint |
|---|---|---|
| **Requirements** (planning ask, set by appeal document) | `origRequirements`, `revisedRequirements` | plan endpoint, or flow `groupby=cluster` with a `planid` |
| **Funding / commitments** (what donors actually committed) | `fundingTotal`, `amountUSD`, `status: commitment\|paid\|pledge` | flow endpoint |
| **Pledges** (announced, not yet committed) | `pledgeTotal` | flow endpoint, separate from commitments |

A country's funding-coverage % would be
`incoming.fundingTotal / sum(plan.revisedRequirements for plans active that year)`.
There is no single endpoint that returns this ratio; compose client-side.

## Sample queries

```python
import httpx

# (a) HRP totals for a country-year
plans = httpx.get(
    "https://api.hpc.tools/v2/public/plan/country/MDG", timeout=30
).json()["data"]
mdg17 = [p for p in plans if any(y["year"] == "2017" for y in p["years"])]
for p in mdg17:
    print(p["id"], p["planVersion"]["name"], p["revisedRequirements"])

# (b) All flows for a country-year (paginate)
flows, page = [], 1
while True:
    j = httpx.get(
        "https://api.hpc.tools/v1/public/fts/flow",
        params={"countryISO3": "MDG", "year": 2017, "limit": 1000, "page": page},
        timeout=60,
    ).json()
    flows.extend(j["data"]["flows"])
    if not j.get("meta", {}).get("nextLink"):
        break
    page += 1

# (c) Single plan with cluster-level requirements (via flow endpoint)
j = httpx.get(
    "https://api.hpc.tools/v1/public/fts/flow",
    params={"planid": 590, "groupby": "cluster"},
    timeout=30,
).json()
for c in j["data"]["requirements"]["objects"]:
    print(c["name"], c["origRequirements"], c["revisedRequirements"])

# (d) CERF-only flows (post-filter; combined source+country filter is broken)
CERF = "4762"
cerf_flows = [
    f for f in flows
    if any(
        o["type"] == "Organization" and str(o["id"]) == CERF
        for o in f.get("sourceObjects", [])
    )
]
```

## Other gotchas

- **`meta.count` ≠ `incoming.flowCount`.** `meta.count` is total rows in
  the paginated set (incoming + outgoing + internal). `incoming.flowCount`
  is just incoming. For YEM 2023: `meta.count=1388`,
  `incoming.flowCount=1250`, `outgoing.flowCount=9`,
  `internal.flowCount=129`.
- **Param case is loose** (`organizationid` and `organizationId` both
  work) but error messages always lowercase the param name.
- **`releasedDate=null`** plans (like Enawo) appear to be structurally
  the same as released plans, but I didn't exhaustively verify.
- **Sub-cluster (governingEntity) breakdowns** aren't surfaced under
  `/v2/public/`. The flow endpoint's `requirements.objects` only returns
  `objectType: "Cluster"`. If you need finer than cluster-level, the API
  doesn't expose it publicly.
