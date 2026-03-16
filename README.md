# ds-storm-impact-harmonisation

Harmonising storm impact data from multiple sources for OCHA humanitarian analysis.

## Project Inputs

All datasets are stored in Azure Blob Storage (`projects` container, `dev` stage) and accessed via `ocha_stratus`.

| Dataset | Blob Path | Description |
|---|---|---|
| CERF storms | `ds-storm-impact-harmonisation/processed/cerf-storms-with-sids-2024-02-27.csv` | CERF-funded storm events matched with IBTrACS storm IDs |
| ADAM exposure | `ds-cyclone-exposure/adam_historical_national_exposure.csv` | WFP ADAM national population exposure at 60/90/120 km/h wind thresholds |
| GDACS exposure | `ds-cyclone-exposure/gdacs_historical_national_exposure.csv` | GDACS national population exposure at 34 kt/64 kt wind thresholds |

## Project Structure

```
├── src/                 # Reusable source code
│   └── datasets/        # Dataset-specific loading and wrangling modules
├── artefacts/           # Exploratory scripts, notebooks, scratch work
├── pyproject.toml       # Project config (uv-managed)
└── .env                 # Azure credentials (not tracked)
```

## Setup

```bash
uv sync
cp .env.example .env  # fill in Azure credentials
```
