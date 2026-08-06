# ds-storm-impact-harmonisation

Harmonising storm impact data from multiple sources for OCHA humanitarian analysis.

## Project Inputs

All datasets are stored in Azure Blob Storage (`projects` container, `dev` stage) and accessed via `ocha_stratus`.

| Dataset | Blob Path | Description |
|---|---|---|
| CERF storms | `ds-storm-impact-harmonisation/processed/cerf-storms-with-sids-2024-02-27.csv` | CERF-funded storm events matched with IBTrACS storm IDs |
| ADAM exposure | `ds-cyclone-exposure/adam_historical_national_exposure.csv` | WFP ADAM national population exposure at 60/90/120 km/h wind thresholds |
| GDACS exposure | `ds-cyclone-exposure/gdacs_historical_national_exposure.csv` | GDACS national population exposure at 34 kt/64 kt wind thresholds |

External API and data-source reference notes live in [`docs/`](docs/README.md) —
check there before re-crawling an API. Architecture decisions are in
[`docs/decisions/`](docs/decisions/) (MADR).

## Project Structure

```
├── src/                 # Reusable source code
│   └── datasets/        # Dataset-specific loading and wrangling modules
├── book/                # Quarto book (analysis chapters)
├── scripts/             # Pipelines, cache refreshers, scheduled jobs
├── docs/                # External source reference notes + ADRs
├── app/                 # Static JS exposure comparison app
├── artefacts/           # Exploratory scripts, notebooks, scratch work
├── pyproject.toml       # Project config (uv-managed)
└── .env                 # Azure credentials (not tracked)
```

## Setup

```bash
uv sync
cp .env.example .env  # fill in Azure credentials
```

## Apps

- `app/` — **static JS storm exposure comparison app** (no Python at runtime,
  no plotly). Compares CHD NHC-based exposure (fcastonly + obsv) against GDACS
  and ADAM with a configurable trigger threshold (wind level + population),
  live-updating sliders, country ranking, per-storm forecast evolution, and
  per-issued-time track-buffer maps (Leaflet). Data is pre-exported to
  `app/data/` (gitignored):

  ```
  uv run python export_app_data.py   # regenerate app/data/ from the DB
  cd app && python3 -m http.server 8590   # or any static file server
  ```

- `adm0_exp_app.py` — marimo storm exposure map app (deployed to Azure).
- `storm_impact_app.py` — marimo NHC storm impact app (track buffers, WSP).
- `compare_exposure.py` — marimo CHD vs GDACS vs ADAM scatter comparison.

> **The CERF predictor is no longer in this repo.** The CERF rapid-response
> allocation estimator lives in
> [`OCHA-DAP/ds-cerf-3rm-app`](https://github.com/OCHA-DAP/ds-cerf-3rm-app),
> served at <https://cerf-3rm.azurewebsites.net>. Its deployment was retired
> here by [ADR 0006](docs/decisions/0006-retire-the-cerf-rr-deployment-slot.md)
> and the source was removed by
> [ADR 0007](docs/decisions/0007-move-the-3rm-work-to-its-own-repo.md), which
> also moved the 3RM book chapters and models to
> [`OCHA-DAP/ds-cerf-allocation-patterns`](https://github.com/OCHA-DAP/ds-cerf-allocation-patterns).

## Scheduled jobs

| Workflow | Cadence | What it does |
|---|---|---|
| `daily-gdacs-monitor-email.yml` | 6-hourly (03/09/15/21 UTC) | Renders and sends the GDACS storm monitor email via Listmonk |
| `pdc-cyclone-poll.yml` | 3-hourly | Archives raw PDC cyclone responses to blob. PDC serves no archive and no track history, so missed polls are unrecoverable — see ADR 0005 |
| `deploy-app.yml` | push / schedule | Exports app data and deploys the Pages site |
