# Orchestration (Dagster)

The whole pipeline as one dependency-aware graph: what runs, in what order,
and what happens when something fails. The code is in `orchestration/`. The
last section maps every piece to Airflow.

## The DAG in plain words

1. **Extract** (group `extract`; external assets, never run by Dagster).
   - The bot's logs (`raw/bot_logs`) and the exchange's market-results API
     (`raw/exchange_markets_api`) feed the Parquet / text tables under
     `DBT_DATA_ROOT`. On this public copy `scripts/generate_synthetic.py`
     writes them.
   - These are the dbt sources, one asset each, keyed `[source, table]`.
2. **dbt** (groups `staging`, `intermediate`, `marts`, `reconciliation`,
   `seeds`, `snapshots`, `elementary`). Every model, seed and snapshot is an
   asset, with lineage read from the dbt manifest, and every dbt test (237)
   is an asset check on the model it tests. The project is three asset
   definitions that match the dbt selectors:

   | Definition | dbt selection | Assets | Checks |
   | - | - | - | - |
   | `dbt_routine` | `--selector routine` (everything not tagged heavy) | 57 | 177 |
   | `dbt_heavy` | `--select tag:heavy --exclude tag:monitoring` (heavy staging + fct_rfq_lifecycle) | 6 | 43 |
   | `dbt_monitoring` | `--selector monitoring` (3 monitoring / completeness models) | 3 | 17 |

   Data flows routine → heavy → monitoring, and never backwards:
   - `stg_fills` feeds `fct_rfq_lifecycle`;
   - `fct_rfq_lifecycle`, `fct_fills` and `mart_daily_trade_report` feed the
     monitoring marts;
   - the heavy staging models have no upstream models.
3. **FIXML** (group `regulatory_output`). The asset
   `fixml_sample_trade_reports` runs `scripts/render_fixml.py` after
   `mart_daily_trade_report` and `fct_fills`. Its check
   `fixml_valid_and_round_trips` runs `scripts/check_fixml.py`: schema
   validation plus the exact round trip against the mart (round trip only
   when `PM_FIXML_SKIP_SCHEMA=1`, as in CI; see docs/FIXML_MAPPING.md).

Why the three dbt definitions must each run whole: when a job runs a
definition in full, dagster-dbt hands dbt that definition's own selection
unchanged (for example `dbt build --selector routine`), so dbt picks the tests
exactly as the selector does. If a job ran only part of a definition,
dagster-dbt would list the models one by one and turn dbt's indirect test
selection off, and multi-parent singular tests would be skipped. Each job
therefore runs whole definitions only.

The singular tests that read several models carry
`config(meta={'dagster': {'ref': {'name': ...}}})`, which tells dagster-dbt
which asset to show them on. Each is attached to a model in the definition
whose selector already runs it.

## Jobs

| Job | Runs | Time on the synthetic data |
| - | - | - |
| `routine` | `dbt build --selector routine`, then the FIXML asset and its check | about a minute |
| `monitoring` | `dbt build --selector monitoring` | seconds |
| `full_rebuild` | routine, then heavy, then monitoring, then FIXML | about two minutes |

- Every job uses the in-process executor: one step at a time, in one
  process. DuckDB allows one writer per file, so no two steps may open it at
  once.
- On the original data the heavy models were large, so `routine` read their
  Parquet through `ref()` and `full_rebuild` (slow) refused to start without
  an explicit opt-in. Here everything is small, and the
  default `dbt build` builds it all.

**Schedule:** `routine_daily` runs the routine job at 06:00 on the reporting
clock (fixed UTC-5). It is **off by default**: the extract is closed, so a
daily run would change nothing. Turn it on in the UI (Automation →
`routine_daily`) if data starts flowing.

## Failure and retries

- **Retries.** The routine and monitoring dbt steps retry once, 60 s later
  (`RetryPolicy(max_retries=1, delay=60)`). The retry is for transient
  failures, such as the DuckDB file being locked by another process. A
  failing data test fails again on the retry; the run then fails. The heavy
  step has no retry: on big data a blind rerun of a long step is not wanted.
- **What a failure does.** A dbt error or an error-severity test failure
  fails the dbt step. Assets downstream of that step, such as the FIXML
  asset, are skipped. Warn-severity tests show as checks that failed with
  severity WARN, and the run goes on.
- **Alert.** The sensor `alert_on_run_failure` (on by default) fires on any
  failed run in this Dagster instance: launched from the UI, by the schedule,
  or with `dagster job execute`.
  - It appends a plain-text block to `$DAGSTER_HOME/alerts/run_failures.log`
    (default `work/dagster/alerts/`): time, job, run id, the run's error,
    each failed step, the root cause behind it, and what to do next.
  - The root cause matters because, after retries run out, the step's own
    error only says "Exceeded max_retries of 1".
  - It uses `monitor_all_code_locations=True`: CLI-executed runs carry no
    code-location origin, and a location-scoped sensor silently skips them.
    The instance holds only this project.
  - Sensors are evaluated by the Dagster daemon, which `dagster dev` starts.
    A run that fails while no daemon is running is alerted the next time one
    starts. No webhooks, no e-mail.
- **Re-running.** In the UI, open the failed run and click *Re-execute → From
  failure*. That reruns the failed step and everything after it.

## Running it

From the repo root, after `python scripts/generate_synthetic.py` and
`dbt deps && dbt build`:

```bash
export DAGSTER_HOME=$PWD/work/dagster && mkdir -p $DAGSTER_HOME
cp orchestration/dagster.yaml $DAGSTER_HOME/
dagster definitions validate -m orchestration.definitions
dagster job execute -m orchestration.definitions -j routine
dagster dev -m orchestration.definitions        # UI: http://127.0.0.1:3000
```

(PowerShell: `$env:DAGSTER_HOME = "$PWD\work\dagster"`.)

- **Paths.** `orchestration/project.py` turns every path default
  (`data/synthetic`, `work/...`) into an absolute path, so dbt resolves the
  same files whatever its working directory. Each can be overridden by its
  environment variable.
- **Where Dagster's state goes.** Runs, event logs, compute logs, schedule
  and sensor state, and alerts live in `DAGSTER_HOME`, which is git-ignored.
  Telemetry is off (`dagster.yaml`).
- **The asset graph comes from `target/manifest.json`.** `dagster dev`
  re-parses the dbt project on start. The CLI and the daemon use the
  manifest as it is, so run `dbt parse` after changing models before using
  the CLI.

**What you'll see in the UI:**
- **Assets → Lineage.** The whole graph, from `raw/bot_logs` through the
  Parquet sources, staging, intermediate and marts, to
  `fixml_sample_trade_reports`. External assets (the extract) show no
  materialize button.
- **An asset's page.** Its dbt description, columns, the SQL, its checks
  (the dbt tests) with their latest results, and its materialization history.
- **Jobs.** `routine`, `monitoring` and `full_rebuild`.
- **Runs.** Each run's step timeline, dbt's log lines, check results.
- **Automation.** The schedule `routine_daily` (stopped) and the sensor
  `alert_on_run_failure` (running).

## The same pipeline in Airflow

Airflow has no assets. It schedules **tasks** in a **DAG**. The mapping:

| Dagster here | Airflow |
| - | - |
| Code location (`orchestration.definitions`) | A DAG file in `dags/` (one per job, or one DAG with branches) |
| External assets for the extract | Nothing to run. Documented in the DAG's `doc_md`, or a `Dataset` / `Asset` marking the Parquet root as an upstream input |
| dbt asset definitions (`dbt_routine`, ...) | One `BashOperator` per selector running `dbt build --selector ...`. Or, at model grain, astronomer-cosmos's `DbtTaskGroup`, which renders one task per model plus its tests |
| Asset checks (dbt tests) | dbt test tasks. `dbt build` runs them inside the same task; cosmos renders them as separate test tasks after each model |
| FIXML asset + check | Two `BashOperator`s, `render_fixml` → `check_fixml`, downstream of the dbt task |
| Jobs `routine` / `monitoring` / `full_rebuild` | Three DAGs, or one DAG plus a `Param` that picks the selector. `full_rebuild` gets `schedule=None` (manual only) |
| Asset lineage between groups | Task dependencies (`>>`) inside a DAG; across DAGs, `Dataset`-triggered scheduling or `ExternalTaskSensor` |
| Schedule `routine_daily`, stopped | `schedule="0 6 * * *"` with `is_paused_upon_creation=True` |
| `RetryPolicy(max_retries=1, delay=60)` | `retries=1, retry_delay=timedelta(seconds=60)` |
| Run-failure sensor writing the alert log | `on_failure_callback` on the DAG (once per failed run) or a task (once per failed try) |
| In-process executor (one writer for DuckDB) | `max_active_tasks=1` on the DAG, or a one-slot pool shared by every task that opens the DuckDB file |
| `dagster dev` UI | Airflow webserver: Grid / Graph views and task logs |

Illustrative only, not installed or run here:

```python
# dags/pm_reporting_routine.py  (illustrative; Airflow 2.x syntax)
from datetime import datetime, timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

REPO = "/opt/pm_reporting"
ALERT_LOG = Path("/opt/airflow/alerts/run_failures.log")


def alert_on_failure(context):
    """DAG-level on_failure_callback: the Airflow twin of alert_on_run_failure."""
    ti = context["task_instance"]
    ALERT_LOG.parent.mkdir(parents=True, exist_ok=True)
    with ALERT_LOG.open("a", encoding="utf-8") as fh:
        fh.write(
            f"{'=' * 78}\nAIRFLOW DAG RUN FAILED  {datetime.utcnow():%Y-%m-%dT%H:%M:%SZ}\n"
            f"dag:     {ti.dag_id}\nrun_id:  {context['run_id']}\n"
            f"task:    {ti.task_id} (try {ti.try_number})\n"
            f"error:   {context.get('exception')}\n"
            f"log:     {ti.log_url}\n\n"
        )


with DAG(
    dag_id="pm_reporting_routine",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2025, 3, 1, tz="Etc/GMT+5"),
    catchup=False,
    is_paused_upon_creation=True,          # data is static: off by default
    max_active_tasks=1,                    # DuckDB: one writer at a time
    on_failure_callback=alert_on_failure,
    doc_md="The extract is a closed, one-off input; not re-run.",
) as dag:
    dbt_build_routine = BashOperator(
        task_id="dbt_build_routine",       # models + their tests (= Dagster asset checks)
        bash_command="dbt build --selector routine",
        cwd=REPO, retries=1, retry_delay=timedelta(seconds=60),
    )
    render_fixml = BashOperator(task_id="render_fixml", bash_command="python scripts/render_fixml.py", cwd=REPO)
    check_fixml = BashOperator(task_id="check_fixml", bash_command="python scripts/check_fixml.py", cwd=REPO)

    dbt_build_routine >> render_fixml >> check_fixml
```

What Airflow gives up compared with this setup:
- **Per-model visibility.** It sees one `dbt build` task unless you add cosmos.
- **Per-test results in the UI.** With a single `dbt build` task, test
  results live in the task log, not as checks on each table.
- **Lineage from the dbt manifest.** Airflow knows only the task order you write.
