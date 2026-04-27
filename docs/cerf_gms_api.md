# CERF GMS API

OCHA's Grant Management System public XML/JSON API for CERF allocations
and projects. Not currently wired into the predictor app, but the obvious
path if we want a dynamic training pipeline. Reference implementation in
`src/datasets/cerf.py::load_cerf_api_data()`.

| | |
|---|---|
| Base URL | `https://cerfgms-webapi.unocha.org/v1/` |
| Auth | none |
| Format | XML default; `.json` variants exist for the main `All` endpoints |
| Cache | 6 hours (`Cache-Control: max-age=21600`); use `/nocache/` paths to bypass |
| Discovery | self-documenting index at `https://cerfgms-webapi.unocha.org/` lists 80+ endpoints |
| OpenAPI / Swagger | none |

## Two main endpoints

| Endpoint | Rows | Granularity |
|---|---|---|
| `/v1/application/All.xml` | 1,601 (1,178 RR + 423 UF) | one row per CERF application/allocation, 43 fields |
| `/v1/project/All.xml` | 8,502 | one row per project (apps split per implementing agency × sector), 63 fields |

**Gotcha: `/v1/project/All.xml` requires `Accept-Encoding: gzip`.** The
uncompressed payload is ~28 MB and the server closes the connection unless
gzip is negotiated. `application/All.xml` works either way.

## Useful filtered endpoints

| Endpoint | Purpose |
|---|---|
| `application/year/{YYYY}.xml` | Apps for a single year. **Returns 42 fields, not 43** (drops `EmergencyCategoryName`). |
| `application/GetByQry/{value}/{field}.xml` | Filter where `field ∈ {window, countrycode, emergencyglobalreporting}`. e.g. `RR/window`, `SOM/countrycode`. |
| `application/nocache/All.xml` | Bypass the 6-hour CDN cache. |
| `project/year/{YYYY}.xml` | Projects for a single year. |
| `project/GetByQry/{value}/{field}.xml` | Filter where `field ∈ {window, countrycode, agency, sector, emergencyglobalreporting}`. |
| `project/projectcode/{code}.xml` | Different schema (returns `ArrayOfProjectResult` with `ProjectCode`, `ProjectAgency`, `ProjectObjective`, `ProjectBeneficiary`, `Activities*`). Not a passthrough. |
| `project/id/{ApplicationID}.xml` | All projects under one application — note this takes the numeric `ApplicationID`, not `ApplicationCode`. |
| `hdxproject/All.xml` | Slimmed 17-field variant. |
| `donorcontribution.xml` (+ `id/`, `GetByYearCountryCode/`, `GetByDate/`, `GetByQry/`) | Donor contributions. |
| `KeyFigures/All.xml` | Global totals (`AmountAllocated`, `NoOfCountriesAllocated`, `AmountReceived`, `MemberStateContribution`). |
| `iati/...` | IATI-format variants. |
| `nrbeneficiarybyapplicationsector/All.xml`, `nrbeneficiarybyproject/All.xml` etc. | Sub-grant / beneficiary reporting. |
| Reference tables: `agency`, `country`, `emergencytype`, `region`, `subregion`, `window`, `cerfsector`, `gendermarker`, `year` etc. | Small lookup tables, all support `id/{n}` and `All.xml`. |

**Endpoints that don't exist**: `/sector/All`, `/report/All`, single-app
lookup by `ApplicationCode` (only `ApplicationID` works for `project/id/`),
free-form date filters on applications, query-param filtering on `/All`
(returns HTTP 500).

## Field semantics — `/application/All.xml` (43 fields)

**Identifiers**: `TableName` (constant `M`), `ApplicationID` (numeric internal),
`ApplicationCode` (e.g. `CERF-CUB-25-RR-1495`), `applicationgrouping`.

**Geography**: `CountryID`, `CountryName`, `CountryCode` (ISO3),
`RegionID/Name`, `SubRegionID/Name`, `ContinentID/Name`.

**Window/year**: `Year` (0 for "Under Review"), `WindowID`, `WindowFullName`
(`Rapid Response` | `Underfunded Emergencies`), `AllocationStatus`
(`Report Available` 1423, `Completed` 120, `Under Implementation` 39,
`Under Review` 19).

**Emergency typing**: `EmergencyTypeID/Name`, `EmergencyGroupID/Name`,
`EmergencyCategoryID/Name`, `EmergencyGroupForGlobalReporting`.

**Narratives**: `ApplicationTitle`, `ApplicationSummary`,
`OverviewoftheHumanitarianSituation`, `RationaleforCERFAllocation`,
`CERFSStrategicAddedValue`, `CN_Summary`.

**Financial / scale**:

| Field | Notes |
|---|---|
| `TotalAmountApproved` | The only authoritative amount. USD. |
| `CN_AmountRequested` | **Always equals `TotalAmountApproved` in 100% of records** (1,176/1,176 RR with `Year > 0`). Treat as redundant. The API has lost any "requested vs approved" signal. |
| `TotalIndividualsAffected` | Broad humanitarian-context number for the crisis. |
| `TotalIndividualPlanned` | Sum of project-level beneficiary plans. **Matches 3RM `Total Individual targeted` in 420/422 rows exactly.** Coverage: 0/344 in 2007-10, 110/302 in 2011-15, **245/245 in 2016-20, 285/285 in 2021+**. |
| `TotalIndividualReached` | Post-implementation; sparser. |

**Dates**: `FirstProjectApprovedDate`, `LastProjectApprovedDate`,
`CN_ERC_EndorsementDate`, `CERFFeedbackDt`, `InterimDueDate`, `ReportDueDate`.

**Other**: `AgencyShortName` (semicolon-joined list of recipient agencies),
`BeneficiaryType` (semicolon-joined classifications, e.g. `Refugees;IDPs`).

For predictive modeling the directly useful application-level fields are:
`TotalAmountApproved`, `TotalIndividualsAffected`, `TotalIndividualPlanned`,
`WindowFullName`, `EmergencyTypeName`, `Year`, `CountryCode`,
`FirstProjectApprovedDate`, `AllocationStatus`.

## Field semantics — `/v1/project/All.xml` (extras over application/All)

- **`ProjectSectorName`** — single sector per row (apps with multiple
  sectors expand to multiple project rows). Application-level `projectsectors`
  is generally null; if you need a sector-flat view, hit `/project/All`.
- **`ProjectTypeName`** — `HRP` (1,550) vs `NON-HRP` (6,952). This is the
  HRP-linkage flag.
- **`AgencyShortName`** — single value here (vs `;`-joined on
  `application/All`). Use this for clean joins.
- Beneficiary breakdowns (`PlannedGirls/Boys/Children/Women/Men/Female/Male/Adults/TotalPeoplePlanned`):
  ~99% populated for 2020+ projects. Reached counterparts ~61% (gap = projects still under implementation).
- `ProjectCountryBudget` is **100% null**. Vestigial.

## Sample queries

```python
# 1) All applications (works without gzip)
import httpx, xml.etree.ElementTree as ET
import pandas as pd

NIL = "{http://www.w3.org/2001/XMLSchema-instance}nil"
r = httpx.get(
    "https://cerfgms-webapi.unocha.org/v1/application/All.xml", timeout=60
)
r.raise_for_status()
root = ET.fromstring(r.content)
df = pd.DataFrame(
    {c.tag: (None if c.get(NIL) == "true" else c.text) for c in app}
    for app in root
)
df["Year"] = pd.to_numeric(df["Year"], errors="coerce")
rr_funded = df[(df["WindowFullName"] == "Rapid Response") & (df["Year"] > 0)]

# 2) All projects (~28 MB — REQUIRES gzip header)
with httpx.Client(
    headers={"Accept-Encoding": "gzip", "User-Agent": "Mozilla/5.0"},
    timeout=httpx.Timeout(600.0),
) as client:
    r = client.get("https://cerfgms-webapi.unocha.org/v1/project/All.xml")
    r.raise_for_status()
projects = pd.DataFrame(
    {c.tag: (None if c.get(NIL) == "true" else c.text) for c in p}
    for p in ET.fromstring(r.content)
)

# 3) Smaller filtered slice — country or window
url = "https://cerfgms-webapi.unocha.org/v1/application/GetByQry/{val}/{field}.xml"
r = httpx.get(url.format(val="SOM", field="countrycode"), timeout=60)
```

## Gotchas / data-quality

1. **Gzip required** for `/project/All`.
2. **`Year=0` is a sentinel for "Under Review"** (10 records). Always filter
   `Year > 0` or `AllocationStatus != "Under Review"` for analysis.
3. **`CN_AmountRequested == TotalAmountApproved` always.** No real
   "requested vs approved" signal. If you need original CN amounts, parse
   the narrative `OverviewoftheHumanitarianSituation` / `CN_Summary` text or
   source elsewhere.
4. **`ProjectCountryBudget` is 100% null.**
5. **`application/year/{YYYY}.xml` returns 42 fields**, not 43. Don't assume
   identical schemas between sibling endpoints.
6. **`ProjectSectorName` at app level uses `;` joins**; at project level
   it's a single string.
7. **`CERFGenderMarkerName` coding is inconsistent**: mixed `2`/`2a`/`2b`,
   plus separate `NA` and `Not Applicable` entries. Normalise before use.
8. **`Reached*` fields lag `Planned*`** by ~38pp. Don't drop rows on
   reached-null when modeling planned beneficiary impact.
9. **`ApplicationCode` format changed mid-2024.** Legacy `YY-RR-ISO3-NNNNN`
   (e.g. `23-RR-MOZ-57965`) vs new `CERF-ISO3-YY-RR-NNNN` (e.g.
   `CERF-CUB-25-RR-1495`). Both coexist; don't parse with a single regex.
10. **No single-application lookup by `ApplicationCode`** exists.

## What it CAN'T give you

- The `Total Amount Required` value 3RM uses for `LogRequired`. The API has
  no "humanitarian appeal total" or sector-level requirement field. The
  closest workaround would be to look up the project's
  `ProjectSectorName` and join to FTS cluster-level requirements per HRP —
  see [`fts_hpc_api.md`](fts_hpc_api.md).
