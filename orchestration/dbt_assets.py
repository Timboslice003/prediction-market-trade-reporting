"""Every dbt node (model, seed, snapshot) as a Dagster asset; dbt tests as asset checks.

The project is split into three asset definitions that line up with the dbt
selectors in selectors.yml:

    dbt_routine     selector `routine`              (everything not tagged heavy)
    dbt_heavy       tag:heavy, minus tag:monitoring (heavy staging + fct_rfq_lifecycle)
    dbt_monitoring  selector `monitoring`           (the 3 heavy monitoring models)

Why three and not one: when a job runs an asset definition WHOLE, dagster-dbt
passes that definition's own selection to dbt unchanged (`dbt build --selector
routine`), so dbt picks the tests exactly as the selector does, singular tests
included. When a job runs only PART of a definition, dagster-dbt lists the
models one by one and turns indirect test selection off, so singular tests
would not run. Each job therefore runs whole definitions only.

The graph only flows one way between them (routine -> heavy -> monitoring;
heavy staging has no upstream models), so the split creates no cycle.
"""
import os
import sys
from pathlib import Path

from dagster import AssetExecutionContext, RetryPolicy
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, dbt_assets

from .project import dbt_project

# One retry, a minute apart: covers a transient failure such as the DuckDB
# file being locked by another process. A failing data test fails again on the
# retry; the run then fails and the failure sensor writes the alert.
DBT_RETRY = RetryPolicy(max_retries=1, delay=60)


class LayeredTranslator(DagsterDbtTranslator):
    """Group assets by project layer (staging, intermediate, marts, ...)."""

    def get_group_name(self, dbt_resource_props):
        if dbt_resource_props["package_name"] != "pm_reporting":
            return dbt_resource_props["package_name"]          # elementary
        rtype = dbt_resource_props["resource_type"]
        if rtype in ("seed", "snapshot"):
            return f"{rtype}s"
        fqn = dbt_resource_props["fqn"]                        # [project, layer, ..., name]
        return fqn[1] if len(fqn) > 2 else "models"


TRANSLATOR = LayeredTranslator()


def _build(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


@dbt_assets(
    manifest=dbt_project.manifest_path,
    project=dbt_project,
    selector="routine",
    name="dbt_routine",
    dagster_dbt_translator=TRANSLATOR,
    retry_policy=DBT_RETRY,
)
def dbt_routine(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from _build(context, dbt)


@dbt_assets(
    manifest=dbt_project.manifest_path,
    project=dbt_project,
    select="tag:heavy",
    exclude="tag:monitoring",
    name="dbt_heavy",
    dagster_dbt_translator=TRANSLATOR,
    # No retry: on big data this step is long; a blind rerun is not wanted.
)
def dbt_heavy(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from _build(context, dbt)


@dbt_assets(
    manifest=dbt_project.manifest_path,
    project=dbt_project,
    selector="monitoring",
    name="dbt_monitoring",
    dagster_dbt_translator=TRANSLATOR,
    retry_policy=DBT_RETRY,
)
def dbt_monitoring(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from _build(context, dbt)


# The venv's own dbt, so the right versions run even when the venv is not on PATH.
_DBT_EXE = Path(sys.executable).parent / ("dbt.exe" if os.name == "nt" else "dbt")
dbt_resource = DbtCliResource(project_dir=dbt_project, target="dev", dbt_executable=str(_DBT_EXE))
