"""Upload the compare app's data (app/data/) to dev blob for the Pages deploy.

GitHub runners cannot reach the database since its public access was turned
off (2026-09-30), so the deploy workflow no longer runs export_app_data.py.
Instead the export runs on a laptop over the Databricks SSH tunnel, and this
script parks the result on blob as one archive that the workflow downloads:

    # Open the DB tunnel (internal KB infrastructure/local-db-access.md), then
    export DSCI_AZ_DB_DEV_HOST=127.0.0.1:<dev tunnel port>
    uv run python export_app_data.py         # DB -> app/data/ (cleared first)
    uv run python scripts/upload_app_data.py # app/data/ -> blob
    gh workflow run deploy-app.yml           # blob -> Pages

Needs DSCI_AZ_BLOB_DEV_SAS_WRITE in the environment.
"""

import io
import tarfile
from pathlib import Path

import ocha_stratus as stratus

APP_DIR = Path(__file__).parent.parent / "app"
CONTAINER = "projects"
# Keep in step with the download URL in .github/workflows/deploy-app.yml.
BLOB_NAME = "ds-storm-impact-harmonisation/processed/app_data/app_data.tar.gz"


def main() -> None:
    data = APP_DIR / "data"
    if not (data / "core.json").is_file():
        raise SystemExit(f"{data}/core.json is missing; run export_app_data.py first")
    files = [p for p in data.rglob("*") if p.is_file()]
    buf = io.BytesIO()
    # Members are stored as data/..., so extracting into app/ restores app/data/.
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        tar.add(data, arcname="data")
    stratus.upload_blob_data(
        buf.getvalue(), BLOB_NAME, stage="dev", container_name=CONTAINER
    )
    print(
        f"uploaded {len(files)} files ({buf.tell() / 1e6:.1f} MB compressed) "
        f"to {CONTAINER}/{BLOB_NAME}"
    )


if __name__ == "__main__":
    main()
