"""The extract as external (source) assets.

On the original project the extract was a one-off historical run: it read the
bot's JSONL logs, parsed them, and wrote Parquet that is read-only from then
on. Here scripts/generate_synthetic.py writes the same layout. Either way the
extract is outside Dagster: these assets start the lineage, and no job
materializes them.

Each dbt source table gets one external asset with the same key dagster-dbt
uses for it ([source_name, table_name]), so the dbt staging models hang off it.
"""
import json

from dagster import AssetKey, AssetSpec, MetadataValue

from .project import dbt_project

GROUP = "extract"

bot_logs = AssetSpec(
    key=AssetKey(["raw", "bot_logs"]),
    group_name=GROUP,
    kinds={"json"},
    description=(
        "The market-making bot's JSONL logs (pricing decisions, quote submissions, fills, "
        "settlements, loss budget, WebSocket frames) and the taker script's ledgers. "
        "On this public copy, written synthetically by scripts/generate_synthetic.py. "
        "Upstream of every Parquet table below."
    ),
    metadata={"produced_by": "scripts/generate_synthetic.py (stands in for the one-off extract)"},
)

exchange_markets_api = AssetSpec(
    key=AssetKey(["raw", "exchange_markets_api"]),
    group_name=GROUP,
    kinds={"api"},
    description=(
        "The exchange's market-results endpoint: the result of every leg market a fill "
        "touched. Re-fetching would call for `dbt snapshot` afterwards (snap_market_results)."
    ),
)

# Where each source's files sit, relative to DBT_DATA_ROOT.
_LOCATION = {
    "market_results": "market_results/market_settlements.parquet",
    "manifest": "_manifest/manifest.parquet",
    "expectations": "_manifest/expectations.parquet",
    "taker_plays_files": "taker/*/plays.csv",
    "taker_log_files": "taker/*/log.jsonl",
}


def _source_specs():
    manifest = json.loads(dbt_project.manifest_path.read_text(encoding="utf-8"))
    specs = []
    for src in manifest["sources"].values():
        if src["package_name"] != "pm_reporting":
            continue
        table, source = src["name"], src["source_name"]
        upstream = exchange_markets_api.key if table == "market_results" else bot_logs.key
        location = _LOCATION.get(table, f"{table}/month=YYYY-MM/*.parquet")
        kind = "csv" if location.endswith(".csv") else ("json" if location.endswith(".jsonl") else "parquet")
        specs.append(
            AssetSpec(
                key=AssetKey([source, table]),
                deps=[upstream],
                group_name=GROUP,
                kinds={kind},
                description=(
                    f"Extract output (read-only): {src.get('description', '').strip()} "
                    f"Source group: {src.get('source_description', '').strip()}"
                ).strip(),
                metadata={
                    "location": MetadataValue.path(f"$DBT_DATA_ROOT/{location}"),
                    "row_counts": "the extract manifest: $DBT_DATA_ROOT/_manifest/manifest.parquet",
                },
            )
        )
    return specs


extract_specs = [bot_logs, exchange_markets_api, *_source_specs()]
