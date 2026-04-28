# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a data science project by OCHA-DAP (UN Office for the Coordination of Humanitarian Affairs) focused on harmonising storm impact data from different sources.

## Project Structure

- **`src/`** — Reusable source code (custom functions, utilities)
  - **`src/datasets/`** — Dataset-specific loading and wrangling modules, each named after the dataset it handles
- **`artefacts/`** — Exploratory scratch work (notebooks, one-off scripts, drafts). Use this as a sandbox to iterate on ideas before distilling finalized logic into `src/`.
- **`.env`** — Azure credentials (not tracked in git)

### Workflow

1. Explore and prototype in `artefacts/`
2. Once logic is proven, refactor reusable parts into `src/` (or `src/datasets/` for dataset-specific code)

## Key Data Inputs

All in Azure Blob Storage (`projects` container, `dev` stage):

| Dataset | Blob Path |
|---|---|
| CERF storms | `ds-storm-impact-harmonisation/processed/cerf-storms-with-sids-2024-02-27.csv` |
| ADAM exposure | `ds-cyclone-exposure/adam_historical_national_exposure.csv` |
| GDACS exposure | `ds-cyclone-exposure/gdacs_historical_national_exposure.csv` |

## Build & Dev Commands

- **Install deps**: `uv sync`
- **Add dependency**: `uv add <package>` (or `uv add --dev <package>` for dev deps)
- **Run script**: `uv run python <script.py>`
- **Lint**: `uv run ruff check`
- **Format**: `uv run ruff format`
- **Lint fix**: `uv run ruff check --fix`

## Book Chapter Caches

Book chapters that fetch from external APIs cache responses locally to avoid re-fetching on every render. Caches live in `book/_cache/<chapter>/` and are populated by scripts in `scripts/`.

| Chapter | Cache script | Cache dir |
|---|---|---|
| `06-gdacs-episodes.qmd` | `scripts/cache_gdacs_episodes.py` | `book/_cache/06-gdacs-episodes/` |
| `08-pdc-evaluation.qmd` | `scripts/cache_pdc_sinlaku.py` | `book/_cache/08-pdc-evaluation/` |

To refresh a cache: `uv run python scripts/<cache-script>.py`

The `.qmd` files load from cache by default. Live API call code is kept in `eval: false` cells for reference.

## ocha_stratus — Azure Blob & Database Access

The internal `ocha_stratus` package (repo: `OCHA-DAP/ocha-stratus`) provides helpers for reading/writing data to Azure Blob Storage and Azure PostgreSQL. Install with `uv add ocha-stratus` (available on the OCHA-DAP private index or GitHub).

### Environment Variables

Blob storage SAS tokens (set in `.env`, loaded via `python-dotenv`):
- `DSCI_AZ_BLOB_DEV_SAS` / `DSCI_AZ_BLOB_PROD_SAS` — read access
- `DSCI_AZ_BLOB_DEV_SAS_WRITE` / `DSCI_AZ_BLOB_PROD_SAS_WRITE` — write access

Database credentials:
- `DSCI_AZ_DB_DEV_PW`, `DSCI_AZ_DB_DEV_UID`, `DSCI_AZ_DB_DEV_HOST` (and `_WRITE` variants)
- `DSCI_AZ_DB_PROD_PW`, `DSCI_AZ_DB_PROD_UID`, `DSCI_AZ_DB_PROD_HOST` (and `_WRITE` variants)

### Typical Usage Pattern

```python
import ocha_stratus as stratus
from dotenv import load_dotenv

load_dotenv()

# Read CSV from blob (defaults to dev stage, "projects" container)
df = stratus.load_csv_from_blob("ds-project-name/file.csv")

# Write CSV back
stratus.upload_csv_to_blob(df, "ds-project-name/output.csv")
```

### Blob Storage Functions

All blob functions accept `stage="dev"|"prod"` (default `"dev"`) and `container_name` (default `"projects"`).

| Function | Description |
|---|---|
| `load_csv_from_blob(blob_name, **kwargs)` | Load CSV → `pd.DataFrame` (extra kwargs passed to `pd.read_csv`) |
| `upload_csv_to_blob(df, blob_name, **kwargs)` | Upload DataFrame as CSV |
| `load_parquet_from_blob(blob_name)` | Load Parquet → `pd.DataFrame` |
| `upload_parquet_to_blob(df, blob_name)` | Upload DataFrame/GeoDataFrame as Parquet |
| `load_geoparquet_from_blob(blob_name)` | Load GeoParquet → `gpd.GeoDataFrame` |
| `load_shp_from_blob(blob_name, shapefile=)` | Load zipped shapefile → `gpd.GeoDataFrame` |
| `upload_shp_to_blob(gdf, blob_name)` | Upload GeoDataFrame as zipped shapefile |
| `open_blob_cog(blob_name, chunks=)` | Open COG raster → `xr.DataArray` (via rioxarray) |
| `upload_cog_to_blob(da, blob_name)` | Upload DataArray as COG |
| `load_blob_data(blob_name)` | Load raw bytes |
| `upload_blob_data(data, blob_name)` | Upload raw bytes |
| `list_container_blobs(name_starts_with=)` | List blob names with optional prefix filter |

### Database Functions

| Function | Description |
|---|---|
| `get_engine(stage, write=)` | SQLAlchemy engine for Azure PostgreSQL |
| `postgres_upsert(table, conn, keys, data_iter, constraint=)` | Upsert helper for `df.to_sql(..., method=postgres_upsert)` |

### Datasource Modules

`stratus.codab`, `stratus.cerf`, `stratus.emdat` — specialized loaders for common humanitarian datasets.
