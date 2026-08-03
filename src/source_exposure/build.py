"""Reproducibly rebuild the storm source-comparison dataset.

Two steps, two Python environments — by design, no single env has BOTH
`ocha_lens` (needed to probe GDACS/ADAM) and `openpyxl` (needed to write the
Excel workbook); the blob-persisted diagnostic is the clean hand-off between
them:

  1. diagnostic  source_diagnostics  probe GDACS/ADAM coverage, upload the
                                      result to blob (canonical copy)
                                      → runs in the PIPELINE venv (ocha_lens)
  2. workbook    workbook            read the diagnostic (local or blob) +
                                      query the DB, write the xlsx
                                      → runs in the HARMONISATION venv (openpyxl)

Each step runs as a module (`python -m src.source_exposure.<step>`) from the
repo root, so its `from src.source_exposure import …` imports resolve.

Usage
-----
  python -m src.source_exposure.build                       # workbook only (fast)
  python -m src.source_exposure.build --refresh-diagnostic  # re-probe first

build.py itself uses only the stdlib, so run it with any interpreter. The two
step interpreters are resolved from the workspace layout; override with env vars
PIPELINE_PYTHON / WORKBOOK_PYTHON if your venvs live elsewhere.
"""

import os
import subprocess
import sys
from pathlib import Path

# Run the step modules from the repo root so `src.source_exposure` imports.
# build.py lives at <repo>/src/source_exposure/build.py → parents[2] == repo root.
REPO_ROOT = Path(__file__).resolve().parents[2]
# parents[4] == the workspace root holding the sibling repos (for their venvs).
ROOT = Path(__file__).resolve().parents[4]

PIPELINE_PY = os.environ.get(
    "PIPELINE_PYTHON", str(ROOT / "ds-storms-pipeline" / ".venv" / "bin" / "python"))
WORKBOOK_PY = os.environ.get(
    "WORKBOOK_PYTHON",
    str(ROOT / "ds-storm-impact-harmonisation" / ".venv" / "bin" / "python"))


def _run(py: str, module: str, label: str) -> None:
    if not Path(py).exists():
        raise SystemExit(
            f"interpreter not found for '{label}': {py}\n"
            f"set the matching env var (PIPELINE_PYTHON / WORKBOOK_PYTHON).")
    print(f"\n=== {label} ===\n{py} -m {module}", flush=True)
    subprocess.run([py, "-m", module], cwd=str(REPO_ROOT), check=True)


def main(refresh_diagnostic: bool) -> None:
    if refresh_diagnostic:
        _run(PIPELINE_PY, "src.source_exposure.source_diagnostics",
             "step 1/2: diagnostic (probe GDACS/ADAM + upload to blob)")
    else:
        print("(skipping diagnostic refresh; workbook will reproduce it from "
              "the canonical blob copy — pass --refresh-diagnostic to re-probe)")
    _run(WORKBOOK_PY, "src.source_exposure.workbook", "step 2/2: workbook")
    print("\nDone.")


if __name__ == "__main__":
    main(refresh_diagnostic="--refresh-diagnostic" in sys.argv)
