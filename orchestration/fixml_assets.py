"""The FIXML sample trade reports as an asset, with its validation as an asset check.

Runs scripts/render_fixml.py (dbt can't write XML). It reads
mart_daily_trade_report and fct_fills from DuckDB read-only and writes the
sample files to PM_FIXML_DIR (default work/fixml). scripts/check_fixml.py
(schema validation + round trip against the mart) is the asset check; it needs
the schema from scripts/fetch_fixml_schema.py and reports a WARN (not a pass)
when the schema is missing. With PM_FIXML_SKIP_SCHEMA=1 (CI) it checks the
round trip only, on purpose, and says so in the check's metadata.
"""
import json
import os
import subprocess
import sys
from pathlib import Path

from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetExecutionContext,
    Failure,
    MaterializeResult,
    MetadataValue,
    asset,
    asset_check,
)
from dagster_dbt import get_asset_key_for_model

from .dbt_assets import dbt_routine
from .project import REPO_DIR, SCRIPTS_DIR

FIXML_DIR = Path(os.environ["PM_FIXML_DIR"])   # absolute, set in project.py


def _run(script, *args):
    return subprocess.run(
        [sys.executable, str(SCRIPTS_DIR / script), *args],
        cwd=REPO_DIR, capture_output=True, text=True, encoding="utf-8",
        env={**os.environ, "PYTHONIOENCODING": "utf-8"},
    )


@asset(
    deps=[
        get_asset_key_for_model([dbt_routine], "mart_daily_trade_report"),
        get_asset_key_for_model([dbt_routine], "fct_fills"),
    ],
    group_name="regulatory_output",
    kinds={"python", "xml"},
    description=(
        "Sample FIX TradeCaptureReport files (FIXML TrdCaptRpt in a regulator-style Batch), one "
        "per chosen trade date, rendered by scripts/render_fixml.py into PM_FIXML_DIR. Modeled "
        "on the standard, not certified against any regulator's specification. See "
        "docs/FIXML_MAPPING.md."
    ),
)
def fixml_sample_trade_reports(context: AssetExecutionContext):
    proc = _run("render_fixml.py", "--out-dir", str(FIXML_DIR))
    context.log.info(proc.stdout)
    if proc.returncode != 0:
        raise Failure(description="render_fixml.py failed", metadata={"stderr": proc.stderr[-4000:]})
    manifest = json.loads((FIXML_DIR / "selection_manifest.json").read_text(encoding="utf-8"))
    files = manifest["files"]
    return MaterializeResult(
        metadata={
            "output_dir": MetadataValue.path(str(FIXML_DIR)),
            "files": MetadataValue.md(
                "| trade date | file | trades | of day |\n|-|-|-|-|\n"
                + "\n".join(
                    f"| {f['trade_date']} | {f['file']} | {f['trades_in_file']} | {f['trades_on_date']} |"
                    for f in files
                )
            ),
            "n_files": len(files),
            "n_trades": sum(f["trades_in_file"] for f in files),
            "cases_missing": ", ".join(manifest["cases_missing"]) or "none",
        }
    )


@asset_check(
    asset=fixml_sample_trade_reports,
    description=(
        "scripts/check_fixml.py: every file validates against the FIXML schema and every "
        "trade round-trips to mart_daily_trade_report exactly; TotMsg = message count."
    ),
)
def fixml_valid_and_round_trips():
    proc = _run("check_fixml.py", "--fixml-dir", str(FIXML_DIR))
    tail = "\n".join(proc.stdout.strip().splitlines()[-15:])
    meta = {
        "exit_code": proc.returncode,
        "schema_validation": "skipped (PM_FIXML_SKIP_SCHEMA=1)"
        if os.environ.get("PM_FIXML_SKIP_SCHEMA", "") == "1" else "run",
        "output_tail": MetadataValue.text(tail),
    }
    if proc.returncode == 2:  # schema not loadable: well-formedness only
        return AssetCheckResult(passed=False, severity=AssetCheckSeverity.WARN, metadata=meta)
    return AssetCheckResult(passed=proc.returncode == 0, severity=AssetCheckSeverity.ERROR, metadata=meta)
