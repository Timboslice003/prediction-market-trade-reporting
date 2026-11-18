"""dbt-duckdb plugin: the directory swap for the external_parquet materialization.

SQL cannot rename or delete directories, so the materialization hands that
step to this plugin through dbt-duckdb's store hook
(adapter.store_relation('parquet_dir_swap', ..., path=<live dir>, ...)).

store() expects the freshly written, already row-count-checked files in
<live dir>__dbt_tmp and then:
  1. moves the current <live dir> (if any) aside to <live dir>__dbt_old,
  2. moves <live dir>__dbt_tmp into place as <live dir>,
  3. deletes <live dir>__dbt_old.
If step 1 or 2 fails (e.g. Windows refuses because another process holds a
file open), nothing has been deleted; the error stops the dbt model.

Wired up in profiles.yml (plugins + module_paths). On load it also creates
the Parquet root ($DBT_STAGING_PARQUET_ROOT, default work/staging_parquet).
"""
import os
import shutil

from dbt.adapters.duckdb.plugins import BasePlugin


def swap_in(live_dir: str) -> None:
    new_dir = live_dir + "__dbt_tmp"
    old_dir = live_dir + "__dbt_old"
    if not os.path.isdir(new_dir):
        raise RuntimeError(f"parquet_dir_swap: no new files at {new_dir}")
    if os.path.exists(old_dir):
        # Left over from an interrupted earlier swap; live_dir is the newer copy.
        shutil.rmtree(old_dir)
    if os.path.exists(live_dir):
        os.replace(live_dir, old_dir)
    os.replace(new_dir, live_dir)
    if os.path.exists(old_dir):
        shutil.rmtree(old_dir)


class Plugin(BasePlugin):
    def initialize(self, plugin_config):
        # DuckDB's COPY creates the model's directory but not its parents.
        os.makedirs(os.environ.get("DBT_STAGING_PARQUET_ROOT", "work/staging_parquet"), exist_ok=True)

    def store(self, target_config):
        swap_in(target_config.location.path)
