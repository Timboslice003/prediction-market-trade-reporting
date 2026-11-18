"""RUNBOOK.md test inventory: generate it, and check it covers every test.

    python scripts/runbook_tests.py generate   # rewrite the inventory section
    python scripts/runbook_tests.py check      # exit 1 if any test is not covered

Both read target/manifest.json, so run `dbt parse` (or any dbt command) first.

The inventory sits between the BEGIN/END markers in RUNBOOK.md; everything
outside them is hand-written and left alone. Generic tests (unique, not_null,
accepted_values, relationships, ...) get their entry from a template filled
with the model and column. Singular tests need a hand-written entry in
SINGULAR below: `generate` refuses to run if one is missing, and `check`
fails if RUNBOOK.md lacks an entry for any test in the project, so a new test
cannot ship without its runbook entry.
"""

from __future__ import annotations

import json
import re
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MANIFEST = ROOT / "target" / "manifest.json"
RUNBOOK = ROOT / "RUNBOOK.md"
BEGIN = "<!-- BEGIN GENERATED TEST INVENTORY (scripts/runbook_tests.py generate) -->"
END = "<!-- END GENERATED TEST INVENTORY -->"
PROJECT = "pm_reporting"

# ---------------------------------------------------------------- singular
# name -> (checks, a failure means, [first three things to check])
SINGULAR: dict[str, tuple[str, str, list[str]]] = {
    # rowcounts
    **{
        f"assert_rowcount_{t}": (
            f"Per source file, staged rows of `{t}` + rows excluded for `_parse_error` equal the "
            "extract manifest's `rows_written` (`manifest_rowcount_reconciliation`).",
            "Staging dropped, duplicated or invented rows, or the extract under DBT_DATA_ROOT "
            "changed (a file added, replaced or truncated).",
            [
                "Run the test's compiled SQL: it lists each file that does not reconcile, with both counts.",
                "Compare that file's manifest row (`_manifest/manifest.parquet`) with the Parquet on disk. "
                "On synthetic data, regenerate with the same seed and rebuild.",
                "If the files are unchanged, diff the staging model's WHERE / QUALIFY / joins "
                "(`git log -p` on the model): staging must never drop real rows.",
            ],
        )
        for t in [
            "events_other", "events_rfq_created", "fills_raw", "fills_settled",
            "exchange_settlements", "loss_budget", "market_results", "quote_submissions",
            "rfq_outcomes", "rfq_outcomes_slim",
        ]
    },
    "assert_rowcount_taker": (
        "The taker ledgers stage exactly the rows of their files (plays: data rows; log: lines), per strategy.",
        "A taker file changed, or the text split in stg_taker_plays / stg_taker_log changed.",
        [
            "Count the lines of each taker/<strategy>/plays.csv (minus the header) and log.jsonl.",
            "Check `is_field_count_ok` in stg_taker_plays (a quoted comma would break the plain split).",
            "Check the filename regex that derives `strategy` and `_source_key`.",
        ],
    ),
    "assert_rfq_legs_match_outcome_leg_counts": (
        "Cheap stand-in for unique(rfq_outcome_id, leg_index) on stg_rfq_legs: every outcome's "
        "legs are exactly 1..len(legs) (count, min, max, sum agree).",
        "stg_rfq_legs and stg_rfq_outcomes no longer have the same leg set: one was rebuilt "
        "without the other, or the unnest changed.",
        [
            "Check when each external_parquet model's files were last written (work/staging_parquet/); "
            "a change to stg_rfq_outcomes needs stg_rfq_legs rebuilt after it.",
            "Run the compiled SQL and look at one failing outcome's legs in both models.",
            "Run the exact `full_rebuild_only` unique test on stg_rfq_legs to confirm.",
        ],
    ),
    "assert_rfq_outcomes_repeats_only_in_dual_instance_window": (
        "rfq_id repeats only inside the dual-instance window (vars dual_instance_*), twice each, and "
        "as many as the generator planted.",
        "A repeated rfq_id outside the window: a real duplicate in the decision log, or staging fanned "
        "rows out.",
        [
            "List the repeated rfq_ids with their _source_key / _line_no and received_at_utc.",
            "Compare the two rows' raw payloads: identical means a true duplicate; different means a "
            "real second decision (a new dual-instance episode).",
            "Do not change the grain (stg_rfq_outcomes is keyed on the source line) without a design review.",
        ],
    ),
    # fills / grading
    "assert_int_fills_graded_rowcount_expected": (
        "int_fills_graded has exactly the fills of the settled file (the generator's count).",
        "Grading dropped or duplicated fills (a join fan-out or an inner join losing fills).",
        [
            "Compare with `select count(*) from staging.stg_fills`.",
            "Check for duplicate fill_id in int_fills_graded and for fills with no legs.",
            "Diff int_fills_graded / int_fill_legs_resolved joins (`git log -p`).",
        ],
    ),
    "assert_python_totals_match_expected": (
        "The legacy Python script's baseline totals (cost, revenue, realized P&L, fees) are the known figures.",
        "The fills_settled extract or stg_python_graded changed, so the comparison is against a "
        "different baseline.",
        [
            "Recompute the four sums from stg_python_graded joined to stg_fills.",
            "Check the fills_settled row-count test (assert_rowcount_fills_settled).",
            "Check stg_python_graded's casts (to_money) and filters for a recent change.",
        ],
    ),
    "assert_recon_no_unexplained_mismatches": (
        "Every fill where dbt and the legacy script disagree falls in a named mismatch_category.",
        "A new kind of grading difference. Never loosen the test: give it a rule in "
        "recon_fills_python_vs_dbt.",
        [
            "Select the 'unexplained' rows from recon_fills_python_vs_dbt (fill_id, both results, diffs).",
            "Trace one fill's legs: fct_fill_legs / int_fill_legs_resolved vs stg_python_graded.legs_settled.",
            "Decide whose answer is right, write the rule, and record the decision.",
        ],
    ),
    "assert_recon_totals_fully_attributed": (
        "The dbt-minus-script totals sit entirely on categorized mismatch rows (agreeing fills sum to < half a cent).",
        "Differences are leaking through the 'agree' threshold: many sub-$0.000001 differences or a threshold change.",
        [
            "Sum revenue_diff / pnl_diff where not is_mismatch.",
            "Check money precision (to_money, floor_to_cent) for a change.",
            "Check the is_mismatch thresholds in recon_fills_python_vs_dbt.",
        ],
    ),
    "assert_taker_fills_identified": (
        "int_fills_origin finds exactly the planted taker-script fills and the one self-trade lookalike.",
        "The taker match rule (combo ticker + contract count) or the taker ledgers changed.",
        [
            "Count int_fills_origin by fill_origin / is_on_taker_ledger / is_self_trade_candidate.",
            "Check stg_taker_plays takes and their contract counts.",
            "Re-read int_fills_origin's header for the rule.",
        ],
    ),
    "assert_fills_raw_repeats_are_channel_duplicates": (
        "A fill_id repeats in the raw fill log only as one REST-poll record plus one websocket record "
        "of the same trade (same ticker, side, count).",
        "A real duplicate (same channel twice) or two conflicting records for one trade id.",
        [
            "Select the failing fill_ids from stg_fills_raw with fill_source, reverify_path and lineage.",
            "Compare the records' payloads (count, side, ticker, cost).",
            "If the exchange re-used a trade id, escalate: fill_id is the dedupe key everywhere downstream.",
        ],
    ),
    "assert_fills_raw_channel_duplicates": (
        "Stores (warn) every trade the raw fill log recorded twice (REST poll, then websocket).",
        "Expected to warn: these are planted channel duplicates, handled downstream (the settled file "
        "dedupes; settlement attribution counts raw-log fills once). A jump in their number points at "
        "a channel problem.",
        [
            "Read the stored rows (channels, reverify_paths) in the dbt_test__audit schema.",
            "Confirm assert_fills_raw_repeats_are_channel_duplicates passes (each is a clean pair).",
            "Compare their count with earlier runs.",
        ],
    ),
    # settlement / bridge
    "assert_exchange_settlements_unexplained": (
        "Stores (warn) every exchange settlement row no attribution rule explains.",
        "A settlement row that is not bot / taker / manual / known artifact: new account activity "
        "or a rule gap. Rows are in dbt_test__audit for review.",
        [
            "Read the stored rows (match_kind, count_diff, cost_diff_dollars).",
            "Look up the ticker in stg_fills, stg_fills_raw and stg_taker_plays.",
            "Add a rule to int_exchange_settlements_attributed and record it.",
        ],
    ),
    "assert_loss_budget_window_sums_to_cumulative": (
        "Inside the bridge window the ledger's cumulative equals the running sum of pnl, and the bridged "
        "entry's cumulative is the known target.",
        "A new restart / replay inside the window, or the bridged entry moved (var loss_budget_bridge_end_before_utc).",
        [
            "List int_loss_budget_ledger rows with is_cumulative_break.",
            "Check var loss_budget_bridge_end_before_utc in dbt_project.yml.",
            "Check stg_loss_budget's ordering columns (recorded_at_utc, _source_key, _line_no).",
        ],
    ),
    "assert_loss_budget_bridge_ties": (
        "The waterfall's start + items (remainder included) equals the ledger figure, to the cent.",
        "The bridge arithmetic is broken (an item double-counted or missing), not that the remainder grew.",
        [
            "Sum recon_loss_budget_positions item columns and compare with ledger_pnl_in_window.",
            "Check every bridge_class is assigned to exactly one item in recon_loss_budget_bridge.",
            "Re-run recon_loss_budget_positions and recon_loss_budget_bridge together.",
        ],
    ),
    "assert_loss_budget_remainder_small": (
        "Warns when the bridge's unexplained remainder exceeds $1 (on the synthetic data it is +$0.37, one position).",
        "Something new is unexplained in the ledger vs bot P&L. The remainder is still reported, not forced.",
        [
            "Find positions with item_residual_dollars <> 0 in recon_loss_budget_positions.",
            "Check whether they have a settlement-log row (has_settlement_row) and their cost_source.",
            "Classify the cause and add a bridge item.",
        ],
    ),
    "assert_bot_positions_never_reconciled": (
        "Stores (warn) every bot position the exchange settled but the reconciler never booked.",
        "Expected to warn: the pagination bug's footprint (one page read per poll). The bridge accounts "
        "for all of them; after a fix, this list should go to zero for new data.",
        [
            "Read the stored rows; group by first_leg_settled_at_utc hour to see the busy settlement batches.",
            "Compare with recon_loss_budget_bridge item 'never booked'.",
            "Check the reconciler's page handling (it must follow the cursor until the page is short).",
        ],
    ),
    # marts
    "assert_marts_tie_to_graded_totals": (
        "fct_fills and mart_daily_pnl both total the generator's independently computed gross P&L, "
        "cost, fees and net P&L (fee-known fills).",
        "A mart dropped / duplicated fills, or grading upstream changed the totals.",
        [
            "Compare each total in fct_fills, mart_daily_pnl and int_fills_graded.",
            "Check fct_fills joins (graded, origin, combos) for fan-out: count(*) vs count(distinct fill_id).",
            "If grading changed on purpose, the generator's grading must change with it.",
        ],
    ),
    "assert_daily_pnl_identity": (
        "On every mart_daily_pnl row gross = net + fees outside cost + gross of unknown-fee fills; fills sum to fct_fills.",
        "A metric was redefined or a fill fell out of the grain.",
        [
            "Find the failing rows and recompute them from fct_fills for that day / group.",
            "Check fee_outside_cost / net_pnl definitions in int_fills_graded.",
            "Check sport_group / n_legs nulls in fct_fills (dim_combos join).",
        ],
    ),
    "assert_daily_trade_report_rowcount_matches_fills": (
        "mart_daily_trade_report has exactly one row per fct_fills fill.",
        "The trade report lost or duplicated trades.",
        [
            "Compare with fct_fills' count.",
            "Check for duplicate trade_id.",
            "Check the model contract (dbt build -s mart_daily_trade_report) for a type error.",
        ],
    ),
    "assert_trade_report_notional_is_quantity_x_price": (
        "On every trade notional = quantity x price (to the cent) and notional + any folded fee = fill_cost.",
        "Price or notional derivation changed, or fill_cost / fee semantics changed upstream.",
        [
            "Inspect failing trades' price_source, fill_cost, fee, is_fee_in_fill_cost.",
            "Check int_fills_graded.is_fee_in_fill_cost and stg_fills.fill_price_dollars.",
            "Re-read mart_daily_trade_report's header (price / notional rules).",
        ],
    ),
    "assert_fills_for_matches_direct_query": (
        "The fills_for macro returns exactly what a hand-written Continental Cup query returns, at both "
        "grains, and the question is not empty.",
        "The macro's filters or grain logic changed, so ad-hoc extracts would be wrong.",
        [
            "Compile a fills_for call (analyses/continental_cup_fills.sql) and read the SQL.",
            "Check macros/fills_for.sql's condition building (_fills_for_in, date_basis).",
            "Check fct_fill_legs' competition / accepted_date_local columns.",
        ],
    ),
    "assert_leg_ticker_prefixes_in_seed": (
        "Stores (warn) every leg ticker prefix that is not in seeds/ticker_prefixes.csv.",
        "The exchange listed a new series; its markets are unclassified (sport NULL), never silently "
        "'other'.",
        [
            "Read the stored rows (prefix, n_markets, example_ticker) in dbt_test__audit.",
            "Look up the series' market titles (dim_markets.title) to classify it.",
            "Add a seed row (and any new sport / market_type to _marts__models.yml accepted values), then dbt seed + build.",
        ],
    ),
    "assert_every_mart_column_documented": (
        "Every column of every non-heavy mart has a YAML description.",
        "A mart gained a column without docs (or a mart was not built).",
        [
            "Run the test's SQL: it lists model and column.",
            "Add the description in models/marts/_marts*__models.yml.",
            "If the row says '<relation not built>', build that mart first.",
        ],
    ),
    "assert_every_heavy_mart_column_documented": (
        "Every column of fct_rfq_lifecycle and the two monitoring marts has a YAML description.",
        "A heavy mart gained a column without docs.",
        [
            "Run the test's SQL: it lists model and column.",
            "Add the description in models/marts/_marts_rfq__models.yml or _marts_monitoring__models.yml.",
            "Rebuild with --selector monitoring and re-run.",
        ],
    ),
    "assert_same_game_flag_agrees": (
        "Stores (warn) combos where the bot's same-game flag disagrees with the ticker-based one.",
        "Expected to warn: planted combos whose same-game leg the bot logged with no game slug. The bot "
        "would not have applied same-game limits to them.",
        [
            "Read the stored rows; look at the combos' legs in fct_fill_legs (bot_game_slug in dim_markets).",
            "Check dim_markets.game_key for the legs (same competition + date + matchup).",
            "If the ticker parse is wrong instead, fix dim_markets and its selection tests.",
        ],
    ),
    "assert_rfq_lifecycle_rowcount_matches_outcomes": (
        "fct_rfq_lifecycle has exactly as many rows as stg_rfq_outcomes. Heavy.",
        "The lifecycle build dropped or fanned out decisions (a join on attempts / fills duplicated rows).",
        [
            "Compare counts; find duplicate rfq_outcome_id (on big data group on hash(rfq_outcome_id)).",
            "Check old_era_outcome is one row per rfq_outcome_id and fills_by_quote one per quote_id.",
            "Check the external_parquet log line ('N rows -> location') of the last build.",
        ],
    ),
    "assert_rfq_lifecycle_no_unclassified_submits": (
        "Every quoting decision (submitted / priced_presubmit) gets a submit_bucket; none is 'unclassified'. Heavy.",
        "A new exit_reason / error_kind / submission shape upstream that the split has no rule for.",
        [
            "Group the failing rows by record_schema, exit_reason, error_kind (the test returns that).",
            "Find what the new value means in the bot's logging code / docs.",
            "Add a rule to fct_rfq_lifecycle's submit_bucket (a design change: review it).",
        ],
    ),
    "assert_rfq_lifecycle_one_counted_decision_per_rfq": (
        "Each doubled RFQ has exactly one counted decision, and it is the filled one when one filled. Heavy.",
        "The dedupe rule broke, so RFQ counts in the monitoring marts double-count or drop RFQs.",
        [
            "List failing rfq_ids with both decisions (received_at, is_filled, quote_id).",
            "Check the old-era attempt linkage (ASOF on received_at) for those RFQs.",
            "Re-read fct_rfq_lifecycle's header (is_counted_decision).",
        ],
    ),
    "assert_monitoring_marts_tie_to_facts": (
        "mart_market_maker_monitoring and mart_requester_activity carry every bot maker fill in fct_fills "
        "(incl. those with no RFQ link), every bot maker fill is either matched to a counted live quote or "
        "counted as unmatched, and both marts carry the same number of counted RFQ decisions. Heavy.",
        "One of the monitoring marts dropped or double-counted rows (a join or filter changed), so its "
        "rates are off.",
        [
            "Compare the fill counts the test returns; find which mart differs.",
            "Group that mart's source CTE by creator / day and diff against fct_fills (fill_origin, rfq_creator_id null).",
            "`git log -p` the two mart SQL files for a new filter or join.",
        ],
    ),
    "assert_requester_empty_creator_id_labelled": (
        "creator_id e3b0c442...b855 (var empty_creator_id_sha256) really is SHA-256 of the empty "
        "string; every mart_requester_activity row with it is labelled 'no_creator_id' and never "
        "flagged as a price checker; no other row has that label; and the label exists.",
        "The label or the price-checker exclusion broke, so the RFQs with no creator id would read as "
        "one huge requester (or vanish from the label).",
        [
            "Read the `problem` column: bad label / flag, wrong var, or no labelled row.",
            "Check the requester_label case and the is_heavy_price_checker guard in mart_requester_activity.sql.",
            "If 'no row carries the label': check fct_rfq_lifecycle.creator_id still holds the hash.",
        ],
    ),
    # reconciliation: completeness
    "assert_rfq_completeness_gaps_for_review": (
        "Stores (warn) every completeness-window hour where the bot recorded fewer RFQs than the exchange "
        "sent, with its likely_cause.",
        "Expected to warn: the planted end-of-hour losses (decision_log_tail_loss) and the planted pricer "
        "outage (bot_stopped, bot_off, bot_off_partial). Any other likely_cause, above all 'unexplained', is "
        "new and gets an investigation.",
        [
            "Read the stored rows; filter likely_cause <> 'decision_log_tail_loss'.",
            "For that hour, count sent vs recorded per minute to see where the gap sits; compare "
            "recording_stopped_sec_before_hour_end with events_stopped_sec_before_hour_end.",
            "Check deploy / restart records for that day and record the cause.",
        ],
    ),
    "assert_rfq_completeness_hours_complete": (
        "recon_rfq_completeness_hourly has every hour of the window and head + mid + tail = missing on each.",
        "An hour vanished (window vars changed) or the gap split double-counts.",
        [
            "Count rows and find missing hours against generate_series.",
            "Check vars completeness_window_start_utc / _end_utc.",
            "Check recording_bounds is one row per hour.",
        ],
    ),
    # snapshots
    "assert_snapshot_one_current_row_per_market": (
        "snap_market_results has exactly one current version per market and covers every staged market.",
        "The snapshot's unique key broke, or markets were added without a snapshot run.",
        [
            "Run `dbt snapshot` and re-test.",
            "List markets with 0 or 2+ current rows.",
            "Check the snapshot config (unique_key, check_cols) in snapshots/snap_market_results.yml.",
        ],
    ),
}

# ---------------------------------------------------------------- generic
GENERIC: dict[str, tuple[str, str, list[str]]] = {
    "not_null": (
        "`{col}` is never NULL in `{model}`.",
        "Rows reached `{model}` without `{col}`: the field went missing upstream, a parse/cast changed, or a "
        "join lost its match.",
        [
            "`select * from {model} where {col} is null limit 20` (or `dbt test -s {test} --store-failures`).",
            "Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no).",
            "`git log -p` the model and its parents for a recent change to that column.",
        ],
    ),
    "unique": (
        "`{col}` is unique in `{model}`.",
        "`{model}` has duplicate `{col}` values: a fan-out join, a source re-delivery, or a grain change.",
        [
            "Find the duplicated keys (group by {col} having count(*) > 1; on 100M+ rows group on hash({col})).",
            "Compare the duplicate rows' payloads: identical = true duplicate, different = a real repeat (e.g. the dual-instance window).",
            "Check the model's joins for a many-to-one that became many-to-many.",
        ],
    ),
    "accepted_values": (
        "`{col}` in `{model}` takes only the listed values.",
        "A new value appeared: a new bot schema / exchange value / seed label. It must get a rule, never a silent bucket.",
        [
            "`select {col}, count(*) from {model} group by 1` to find the new value.",
            "Trace where it comes from (source, seed, or a CASE in the model).",
            "Add a deliberate rule or accepted value (and the docs) and record the decision.",
        ],
    ),
    "relationships": (
        "Every `{col}` in `{model}` exists in its parent ({to}).",
        "Orphan keys: the parent was built from different data than the child, or the key format changed.",
        [
            "List the orphan keys (anti-join child to parent).",
            "Check both models were built in the same run (a partial build can leave one stale).",
            "Check the key's format / casing in both models.",
        ],
    ),
    "equal_rowcount": (
        "`{model}` has the same row count as {to}.",
        "Rows were dropped or duplicated between the two models.",
        [
            "Compare the two counts.",
            "Look for duplicate keys (fan-out) or an inner join that drops rows.",
            "Check whether one of the two was rebuilt without the other.",
        ],
    ),
    "unique_combination_of_columns": (
        "The column combination {cols} is unique in `{model}`.",
        "Duplicate rows at the model's grain.",
        [
            "Group by the columns (on huge tables, by a hash of them) to find duplicates.",
            "Compare duplicate rows' lineage (_source_key, _line_no).",
            "Check the unnest / join that builds the grain.",
        ],
    ),
    "expression_is_true": (
        "The expression `{expr}` holds on every row of `{model}`.",
        "A row breaks the model's invariant (see the column description).",
        [
            "Select the rows where the expression is false.",
            "Recompute the columns involved from their inputs.",
            "Check the macro / formula that builds them for a change.",
        ],
    ),
}


def load_tests() -> list[dict]:
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    nodes = manifest["nodes"]
    out = []
    for n in nodes.values():
        if n["resource_type"] != "test" or n["package_name"] != PROJECT:
            continue
        meta = n.get("test_metadata") or {}
        attached = n.get("attached_node") or next(
            (d for d in n["depends_on"]["nodes"] if d.split(".")[0] in ("model", "seed", "snapshot")), None
        )
        parent = nodes.get(attached) if attached else None
        layer = "other"
        if parent is not None:
            if parent["resource_type"] in ("seed", "snapshot"):
                layer = parent["resource_type"] + "s"
            else:
                layer = parent["fqn"][1] if len(parent["fqn"]) > 2 else "other"
        elif "rowcounts" in n["original_file_path"]:
            layer = "staging"
        if "rowcounts" in n["original_file_path"]:
            layer = "staging"
        out.append({
            "name": n["name"],
            "generic": meta.get("name"),
            "kwargs": meta.get("kwargs") or {},
            "model": parent["name"] if parent else None,
            "layer": layer,
            "severity": str(n["config"].get("severity", "error")).lower(),
            "tags": n["config"].get("tags") or [],
            "path": n["original_file_path"],
            "column": n.get("column_name"),
        })
    return out


def entry(t: dict) -> str:
    if t["generic"]:
        tmpl = GENERIC.get(t["generic"])
        if tmpl is None:
            raise SystemExit(f"no runbook template for generic test type {t['generic']!r} ({t['name']})")
        kw = t["kwargs"]
        fields = {
            "model": t["model"], "col": t["column"] or kw.get("column_name"), "test": t["name"],
            "to": re.sub(r"ref\('([^']+)'\)", r"\1", str(kw.get("to") or kw.get("compare_model") or "")),
            "cols": ", ".join(kw.get("combination_of_columns") or []), "expr": kw.get("expression", ""),
        }
        checks = tmpl[0].format(**fields)
        means = tmpl[1].format(**fields)
        first = [s.format(**fields) for s in tmpl[2]]
    else:
        if t["name"] not in SINGULAR:
            raise SystemExit(f"singular test {t['name']} ({t['path']}) has no runbook entry in "
                             "scripts/runbook_tests.py SINGULAR; write one")
        checks, means, first = SINGULAR[t["name"]]
    tags = [x for x in t["tags"] if x in ("full_rebuild_only",)]
    sev = t["severity"] + (" (tag full_rebuild_only: excluded from the heavy selector; runs in the default build)" if tags else "")
    lines = [
        f"- **`{t['name']}`**" + (f" (`{t['path']}`)" if not t["generic"] else ""),
        f"  - Checks: {checks}",
        f"  - Severity: {sev}",
        f"  - A failure means: {means}",
        "  - First checks: " + " ".join(f"({i}) {s}" for i, s in enumerate(first, 1)),
    ]
    return "\n".join(lines)


LAYER_ORDER = ["staging", "intermediate", "reconciliation", "marts", "seeds", "snapshots", "other"]


def generate() -> None:
    tests = load_tests()
    by_layer: dict[str, dict[str, list[dict]]] = defaultdict(lambda: defaultdict(list))
    for t in tests:
        by_layer[t["layer"]][t["model"] or "(project-level)"].append(t)
    parts = [BEGIN, "", f"{len(tests)} tests. Generated from target/manifest.json; do not edit by hand.", ""]
    for layer in LAYER_ORDER:
        if layer not in by_layer:
            continue
        n = sum(len(v) for v in by_layer[layer].values())
        parts += [f"### {layer.capitalize()} ({n} tests)", ""]
        for model in sorted(by_layer[layer]):
            parts += [f"#### {model}", ""]
            for t in sorted(by_layer[layer][model], key=lambda x: (x["generic"] is None, x["name"])):
                parts += [entry(t), ""]
    parts.append(END)
    text = RUNBOOK.read_text(encoding="utf-8")
    if BEGIN not in text or END not in text:
        raise SystemExit("RUNBOOK.md has no inventory markers")
    head, rest = text.split(BEGIN, 1)
    _, tail = rest.split(END, 1)
    RUNBOOK.write_text(head + "\n".join(parts) + tail, encoding="utf-8", newline="\n")
    print(f"RUNBOOK.md: {len(tests)} tests written")


def check() -> int:
    tests = load_tests()
    text = RUNBOOK.read_text(encoding="utf-8")
    missing = [t["name"] for t in tests if f"**`{t['name']}`**" not in text]
    documented = set(re.findall(r"\*\*`([a-z0-9_]+)`\*\*", text))
    stale = sorted(documented - {t["name"] for t in tests})
    for name in missing:
        print(f"NOT IN RUNBOOK: {name}")
    for name in stale:
        print(f"stale runbook entry (no such test): {name}")
    if missing or stale:
        print(f"FAIL: {len(missing)} tests missing, {len(stale)} stale. "
              "Run `python scripts/runbook_tests.py generate` (and add SINGULAR notes for new singular tests).")
        return 1
    print(f"OK: all {len(tests)} tests are covered by RUNBOOK.md")
    return 0


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "check"
    if cmd == "generate":
        generate()
    elif cmd == "check":
        sys.exit(check())
    else:
        raise SystemExit(__doc__)
