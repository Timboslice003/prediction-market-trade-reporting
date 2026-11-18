"""Paths and environment for the Dagster code location.

Every path defaults to a folder inside the repo (git-ignored) and can be
overridden by an environment variable, so `clone -> install -> run` works on
any machine:

    DBT_DATA_ROOT             data/synthetic            the extract dbt reads
    DBT_STAGING_PARQUET_ROOT  work/staging_parquet      external_parquet models
    PM_DUCKDB_PATH            work/pm_reporting.duckdb  the DuckDB file
    PM_FIXML_DIR              work/fixml                FIXML sample output

The defaults are turned into absolute paths here, so dbt (run by dagster-dbt
as a subprocess) resolves them the same way whatever its working directory.
Dagster's own state goes to DAGSTER_HOME; set it (e.g. to work/dagster) for
run history and the failure-alert sensor, or Dagster uses a temporary one.
"""
import os
import sys
from pathlib import Path

# Put the venv's own executables (dbt) first on PATH: dagster-dbt looks up
# `dbt` on PATH, and the venv may not be activated.
_VENV_BIN = str(Path(sys.executable).parent)
if os.environ.get("PATH", "").split(os.pathsep)[0] != _VENV_BIN:
    os.environ["PATH"] = _VENV_BIN + os.pathsep + os.environ.get("PATH", "")

from dagster_dbt import DbtProject  # noqa: E402

REPO_DIR = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = REPO_DIR / "scripts"

_DEFAULTS = {
    "DBT_DATA_ROOT": "data/synthetic",
    "DBT_STAGING_PARQUET_ROOT": "work/staging_parquet",
    "PM_DUCKDB_PATH": "work/pm_reporting.duckdb",
    "PM_FIXML_DIR": "work/fixml",
    "PM_REPO_ROOT": ".",
}
for var, default in _DEFAULTS.items():
    value = Path(os.environ.get(var, default))
    os.environ[var] = str(value if value.is_absolute() else (REPO_DIR / value).resolve()).replace("\\", "/")

ALERT_DIR = Path(os.environ.get("DAGSTER_HOME") or (REPO_DIR / "work" / "dagster")) / "alerts"
ALERT_LOG = ALERT_DIR / "run_failures.log"

dbt_project = DbtProject(project_dir=REPO_DIR, profiles_dir=REPO_DIR, target="dev")
# `dagster dev` re-parses the dbt project on load; other entry points (the
# CLI `dagster job execute`, the daemon) use target/manifest.json as it is.
dbt_project.prepare_if_dev()
if not dbt_project.manifest_path.exists():
    raise RuntimeError(f"{dbt_project.manifest_path} not found: run `dbt parse` first")
