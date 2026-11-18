"""Run-failure alerting: a sensor that appends a plain-text alert to a log file.

No webhooks, no e-mail: the alert goes to <alerts dir>/run_failures.log
(default work/dagster/alerts/, see orchestration/project.py). Sensors are evaluated by
the Dagster daemon, which `dagster dev` starts; a run launched while no daemon
is running is picked up the next time one starts (the sensor keeps a cursor).
"""
import datetime as dt

from dagster import DagsterEventType, DefaultSensorStatus, RunFailureSensorContext, run_failure_sensor

from .project import ALERT_LOG


def _line(err):
    msg = err.message.strip().splitlines() if err else []
    return msg[0][:500] if msg else ""


def _failed_steps(context: RunFailureSensorContext):
    """(step, error, root cause) per failed step.

    After retries run out, a step's error only says "Exceeded max_retries of 1";
    the exception that actually failed it is at the bottom of the error's
    cause chain, so the alert shows that too.
    """
    out = []
    records = context.instance.get_records_for_run(
        run_id=context.dagster_run.run_id, of_type=DagsterEventType.STEP_FAILURE
    ).records
    for rec in records:
        ev = rec.event_log_entry.dagster_event
        err = getattr(ev.event_specific_data, "error", None)
        root = err
        while root is not None and (root.cause or root.context):
            root = root.cause or root.context
        top, bottom = _line(err) or (ev.message or ""), _line(root)
        out.append((ev.step_key, top, bottom if bottom != top else None))
    return out


@run_failure_sensor(
    name="alert_on_run_failure",
    description=f"Appends an alert to {ALERT_LOG} whenever any run in this Dagster instance fails (UI, schedule or CLI).",
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=30,
    # Runs started with `dagster job execute` carry no code-location origin, and
    # a location-scoped run-status sensor silently skips them (found by
    # deliberately failing a run). This instance holds one code
    # location, so monitoring all of them alerts on exactly this project's runs,
    # however they were started.
    monitor_all_code_locations=True,
)
def alert_on_run_failure(context: RunFailureSensorContext):
    run = context.dagster_run
    steps = _failed_steps(context)
    now = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    lines = [
        "=" * 78,
        f"DAGSTER RUN FAILED  {now}",
        f"job:     {run.job_name}",
        f"run_id:  {run.run_id}",
        f"error:   {context.failure_event.message}",
    ]
    for key, msg, cause in steps:
        lines.append(f"step:    {key}: {msg}")
        if cause:
            lines.append(f"cause:   {cause}")
    if not steps:
        lines.append("step:    (no step failure recorded: run-level failure)")
    lines += [
        "what to do: open the run in the Dagster UI (dagster dev -> Runs -> this run_id),",
        "            read the failed step's logs, fix, then re-execute from failure.",
        "",
    ]
    ALERT_LOG.parent.mkdir(parents=True, exist_ok=True)
    with ALERT_LOG.open("a", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    context.log.info(f"alert written to {ALERT_LOG} for run {run.run_id}")
