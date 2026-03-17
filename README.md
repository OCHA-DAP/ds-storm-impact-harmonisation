# ds-storm-impact-harmonisation

Harmonises historical storm impact data from WFP ADAM and GDACS into a combined
country-level exposure dataset, and serves it via a simple dashboard.

## Setup

Install dependencies with [uv](https://docs.astral.sh/uv/):

```bash
uv sync
```

Install pre-commit hooks:

```bash
uv run pre-commit install
```

## Running the pipelines

Run scripts in order. Each saves output to Azure blob storage under the `ds-storm-impact-harmonisation/` prefix.

```bash
uv run python pipelines/adam_historical_national_exposure.py
uv run python pipelines/gdacs_historical_national_exposure.py
uv run python pipelines/join_historical_national_exposure.py
```

The final script also writes `assets/exposure_data.json` for the dashboard.

## Dashboard

Serve the dashboard locally with Python's built-in HTTP server (required because the
page loads a local JSON file via `fetch`):

```bash
uv run python -m http.server 8000
```

Then open http://localhost:8000 in your browser.