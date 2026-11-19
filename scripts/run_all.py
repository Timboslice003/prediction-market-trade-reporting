"""Run the whole pipeline end to end on synthetic data, timing each step.

    python scripts/run_all.py [--skip-edr] [--skip-dagster] [--no-fixml-schema] [--site work/site]

Steps (each must succeed; the script stops at the first failure):
  1. generate      scripts/generate_synthetic.py -> data/synthetic
  2. dbt deps      packages (dbt_utils, elementary)
  3. dbt build     every model, seed, snapshot and test (selector `everything`)
  4. fixml schema  scripts/fetch_fixml_schema.py (skipped if already present)
  5. fixml render  scripts/render_fixml.py -> work/fixml
  6. fixml check   scripts/check_fixml.py (schema validation + round trip)
--no-fixml-schema (what CI uses) skips step 4 and the schema half of step 6
(also inside the Dagster asset check): the round trip still runs. The schema
is a third-party download, so validation against it is a local step.
  7. dagster       definitions validate, then the `routine` job
  8. dbt docs      dbt docs generate --static
  9. elementary    edr report (from the separate .venv-edr; see requirements-edr.txt)
 10. runbook       scripts/runbook_tests.py check
Then it collects the docs site and the Elementary report into --site (for
GitHub Pages) and writes the timings to work/timings.json.

Run from the repo root. Uses the dbt / dagster next to the running Python, so
run it with the project venv's python.
"""
import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BIN = Path(sys.executable).parent
EXE = ".exe" if os.name == "nt" else ""


def tool(name, venv_bin=BIN):
    return str(venv_bin / f"{name}{EXE}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--skip-edr", action="store_true")
    ap.add_argument("--skip-dagster", action="store_true")
    ap.add_argument("--no-fixml-schema", action="store_true",
                    help="skip the FIXML schema download and validation (round trip still runs)")
    ap.add_argument("--site", default="work/site")
    ap.add_argument("--edr-venv", default=os.environ.get("PM_EDR_VENV", ".venv-edr"))
    a = ap.parse_args()
    os.chdir(ROOT)
    work = ROOT / "work"
    work.mkdir(exist_ok=True)
    env = {**os.environ, "PYTHONIOENCODING": "utf-8"}
    if a.no_fixml_schema:
        env["PM_FIXML_SKIP_SCHEMA"] = "1"     # read by check_fixml.py, also inside the Dagster check
    # absolute paths, so every tool (edr runs from its own folder) sees the same files
    for var, default in {"DBT_DATA_ROOT": "data/synthetic", "DBT_STAGING_PARQUET_ROOT": "work/staging_parquet",
                         "PM_DUCKDB_PATH": "work/pm_reporting.duckdb", "PM_FIXML_DIR": "work/fixml",
                         "PM_REPO_ROOT": "."}.items():
        env.setdefault(var, str((ROOT / default).resolve()).replace("\\", "/"))
    dagster_home = work / "dagster"
    dagster_home.mkdir(parents=True, exist_ok=True)
    shutil.copy(ROOT / "orchestration" / "dagster.yaml", dagster_home / "dagster.yaml")
    env.setdefault("DAGSTER_HOME", str(dagster_home))
    edr_bin = (ROOT / a.edr_venv / ("Scripts" if os.name == "nt" else "bin")).resolve()
    edr_dir = work / "elementary"
    (edr_dir / "edr_config").mkdir(parents=True, exist_ok=True)
    (edr_dir / "edr_config" / "config.yml").write_text(
        "anonymous_usage_tracking: false\ndisable_elementary_version_check: true\n", encoding="utf-8")

    py = sys.executable
    steps = [
        ("generate", [py, "scripts/generate_synthetic.py"]),
        ("dbt deps", [tool("dbt"), "deps"]),
        ("dbt build", [tool("dbt"), "build"]),
        ("fixml schema", [py, "scripts/fetch_fixml_schema.py"]),
        ("fixml render", [py, "scripts/render_fixml.py"]),
        ("fixml check", [py, "scripts/check_fixml.py"]),
        ("dagster validate", [tool("dagster"), "definitions", "validate", "-m", "orchestration.definitions"]),
        ("dagster routine job", [tool("dagster"), "job", "execute", "-m", "orchestration.definitions", "-j", "routine"]),
        ("dbt docs", [tool("dbt"), "docs", "generate", "--static"]),
        ("elementary report", [tool("edr", edr_bin), "report", "-c", str(edr_dir / "edr_config"),
                               "-p", str(ROOT), "--project-dir", str(ROOT),
                               "--target-path", str(edr_dir / "edr_target"),
                               "--file-path", str(edr_dir / "elementary_report.html"),
                               "--open-browser", "false"]),
        ("runbook check", [py, "scripts/runbook_tests.py", "check"]),
    ]
    schema_ok = (work / "fixml_schema" / "latest_ep309_patched" / "fixml-main-Latest.xsd").exists()
    timings = []
    for name, cmd in steps:
        if name == "fixml schema" and a.no_fixml_schema:
            timings.append({"step": name, "seconds": 0.0, "note": "skipped (--no-fixml-schema)"})
            continue
        if name == "fixml schema" and schema_ok:
            timings.append({"step": name, "seconds": 0.0, "note": "already present"})
            continue
        if name.startswith("dagster") and a.skip_dagster:
            continue
        if name == "elementary report" and a.skip_edr:
            continue
        t0 = time.perf_counter()
        print(f"\n=== {name}: {' '.join(cmd)}", flush=True)
        log = work / f"run_{name.replace(' ', '_')}.log"
        with open(log, "w", encoding="utf-8") as fh:
            proc = subprocess.run(cmd, cwd=edr_dir if name == "elementary report" else ROOT, env=env,
                                  stdout=fh, stderr=subprocess.STDOUT, text=True, encoding="utf-8")
        secs = round(time.perf_counter() - t0, 1)
        tail = log.read_text(encoding="utf-8", errors="replace").strip().splitlines()[-3:]
        print("\n".join(tail))
        timings.append({"step": name, "seconds": secs, "exit_code": proc.returncode})
        if proc.returncode != 0:
            print(f"FAILED: {name} (exit {proc.returncode}); full log: {log}")
            (work / "timings.json").write_text(json.dumps(timings, indent=2), encoding="utf-8")
            sys.exit(1)

    # the static site: dbt docs at /, the Elementary report at /elementary/
    site = Path(a.site)
    if site.exists():
        shutil.rmtree(site)
    site.mkdir(parents=True)
    shutil.copy(ROOT / "target" / "static_index.html", site / "index.html")
    report = edr_dir / "elementary_report.html"
    if report.exists():
        (site / "elementary").mkdir()
        shutil.copy(report, site / "elementary" / "index.html")
    (work / "timings.json").write_text(json.dumps(timings, indent=2), encoding="utf-8")
    total = sum(t["seconds"] for t in timings)
    print("\nstep                    seconds")
    for t in timings:
        print(f"{t['step']:<22} {t['seconds']:>8}")
    print(f"{'total':<22} {round(total, 1):>8}")
    print(f"site: {site}")


if __name__ == "__main__":
    main()
