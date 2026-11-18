"""Dagster code location for the pm_reporting pipeline.

    dagster dev -m orchestration.definitions                         # UI on http://127.0.0.1:3000
    dagster job execute -m orchestration.definitions -j routine

See docs/ORCHESTRATION.md.
"""
from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    in_process_executor,
)

from .alerts import alert_on_run_failure
from .dbt_assets import dbt_heavy, dbt_monitoring, dbt_resource, dbt_routine
from .extract_assets import extract_specs
from .fixml_assets import fixml_sample_trade_reports, fixml_valid_and_round_trips

# DuckDB allows one writer per file, so every job runs its steps one at a
# time in a single process (no two steps ever open the database together).
routine_job = define_asset_job(
    name="routine",
    description=(
        "The routine dbt build (`dbt build --selector routine`: everything not tagged heavy, "
        "with its tests), then the FIXML sample trade reports and their validation."
    ),
    selection=AssetSelection.assets(dbt_routine, fixml_sample_trade_reports),
    executor_def=in_process_executor,
)

monitoring_job = define_asset_job(
    name="monitoring",
    description=(
        "`dbt build --selector monitoring`: the three heavy monitoring / completeness models "
        "and their tests. Run after editing one of them or rebuilding fct_rfq_lifecycle or a "
        "heavy staging model."
    ),
    selection=AssetSelection.assets(dbt_monitoring),
    executor_def=in_process_executor,
)

full_rebuild_job = define_asset_job(
    name="full_rebuild",
    description=(
        "Every model and every test: routine, then heavy, then monitoring. About a minute on "
        "the synthetic data. On the original data it re-parsed every source log, was slow, and "
        "was guarded behind an explicit opt-in."
    ),
    selection=AssetSelection.assets(dbt_routine, dbt_heavy, dbt_monitoring, fixml_sample_trade_reports),
    executor_def=in_process_executor,
)

routine_daily = ScheduleDefinition(
    name="routine_daily",
    job=routine_job,
    cron_schedule="0 6 * * *",
    execution_timezone="Etc/GMT+5",      # the reporting clock: fixed UTC-5
    default_status=DefaultScheduleStatus.STOPPED,
    description=(
        "06:00 on the reporting clock, daily. OFF by default: the extract is closed, so a daily "
        "run would change nothing. Turn it on in the UI if data starts flowing."
    ),
)

defs = Definitions(
    assets=[*extract_specs, dbt_routine, dbt_heavy, dbt_monitoring, fixml_sample_trade_reports],
    asset_checks=[fixml_valid_and_round_trips],
    jobs=[routine_job, monitoring_job, full_rebuild_job],
    schedules=[routine_daily],
    sensors=[alert_on_run_failure],
    resources={"dbt": dbt_resource},
)
