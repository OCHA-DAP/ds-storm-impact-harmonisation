# ds-storm-impact-harmonisation

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
