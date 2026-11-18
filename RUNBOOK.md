# Runbook

What every test in this project checks, how bad a failure is, and what to do
first. The on-call section explains how a break is handled; the test
inventory below it covers every test, grouped by layer and model.

- **Commands** (from the repo root)
  - Everything, tests included: `dbt build` (the default selector here is
    `everything`; about a minute on the synthetic data).
  - The everyday selection on big data: `dbt build --selector routine`
    (everything not tagged `heavy`).
  - Heavy models and their tests: `--selector heavy`; the three monitoring
    models (`recon_rfq_completeness_hourly`, `mart_market_maker_monitoring`,
    `mart_requester_activity`): `--selector monitoring`. After editing one of
    the heavy models, rebuild it explicitly, then run the routine build.
- **Stored failures:** tests configured with `store_failures` write their
  failing rows to the `dbt_test__audit` schema of the DuckDB file
  (`work/pm_reporting.duckdb`). The table is named after the test. The
  Elementary report shows them too.
- **Coverage check:** `python scripts/runbook_tests.py check` fails if any
  test in the project has no entry here. After you add a test, run `dbt
  parse`, then `python scripts/runbook_tests.py generate`. A new singular test
  needs a hand-written entry in that script's `SINGULAR` dict first;
  `generate` refuses without it.

## On-call

### Severity levels and expected response

| level | what it means | expected response |
|---|---|---|
| **error** | A data contract is broken: a key is duplicated, totals no longer tie, a row count is off, an unexplained grading or attribution difference appeared. `dbt build` fails, and models downstream of the failing test are skipped. | **Stop and fix before anyone uses the numbers.** Investigate the same day. Nothing built on the affected models is reported until the test passes again. Never loosen the test to get green: add a rule, or fix the data path. |
| **warn** | A known, reviewed condition that should stay visible: channel duplicates, completeness gaps, positions the reconciler never booked, a same-game flag the bot missed, unknown ticker prefixes, unexplained settlement rows, a bridge remainder over $1. The build passes. Most warn tests store their rows for review. | **Review within a week.** Read the stored rows, decide whether each one is known history or new, and record the decision. A warn that reports something NEW gets an investigation like an error. |
| **full_rebuild_only** (tag) | An exact but slow check on big data, e.g. unique `(rfq_outcome_id, leg_index)` on every RFQ leg. A cheap stand-in covers it in the heavy selection. | Runs in the default `everything` build. If a cheap stand-in fails, run the exact test to confirm before changing anything. |

On the synthetic data four warn tests fire on every build, by design: each
shows a planted break (README, "Planted breaks"). An error test failing on
the synthetic data is a bug in the project or the generator.

### How a break is investigated and closed

1. **Reproduce.** `dbt build -s <test>` or `dbt test -s <test> --store-failures`.
   Read the failing rows; every row keeps `_source_key` / `_line_no`, so you
   can trace it to the source line.
2. **Scope.** Find which models and which rows are affected, and whether the
   break is in the data (the extract changed), in code (`git log -p` on the
   models in the test's lineage) or in known history.
3. **Explain, don't filter.** Split the difference into named parts, each
   with evidence, and leave what can't be explained as an explicit
   remainder. Never force a tie, and never add a filter that hides rows.
4. **Fix with a rule.** Change the model, or add a category or bridge item
   with a written rule. Changes to an agreed design go through review.
5. **Verify.** Re-run the failing test and its neighbours (tie-outs,
   row counts) and the full build.
6. **Record and close.** Write what happened and why, commit, and only then
   call it closed.

### Worked example: the loss-budget bridge

**The break.** The bot keeps a loss-budget ledger: a running total of the P&L
it books when its positions settle. That total does not equal the bot's own
graded P&L, and the gap is large.

1. **Reproduce.** `int_loss_budget_ledger` has one row per ledger write.
   Checking cumulative = previous + pnl on every row finds exactly two breaks,
   each detected by rule:
   - a **replay**: in the dual-instance window a second bot process re-booked
     the same keys from the same base, so the cumulative counts them once;
   - a **restart from zero**: a deploy reset the running total.
2. **Scope.** `recon_loss_budget_positions` puts both sides at one grain, the
   position (combo x side). Every position is either in the bridge window or
   explained as outside it.
3. **Explain.** Each difference is a bridge item in
   `recon_loss_budget_bridge`, with its evidence: positions the reconciler
   never booked, history before the restart, scalar revenue logged as 0,
   fees folded into cost, estimated costs, manual and taker activity.
   The biggest item has a mechanism: the reconciler reads one page of
   settlements per poll and advances its cursor past the rest, so busy
   settlement batches lose everything after the first page
   (`assert_bot_positions_never_reconciled` lists them).
4. **Fix with a rule.** Nothing is forced. Each item is a rule on
   `bridge_class`, and what remains is reported as the remainder (on the
   synthetic data +$0.37 on one position: an estimate-cost fill booked while
   the settlement log was missing, so its cost could not be checked).
5. **Verify.**
   - `assert_loss_budget_bridge_ties` (error) checks the arithmetic;
   - `assert_loss_budget_window_sums_to_cumulative` (error) checks the window;
   - `assert_loss_budget_remainder_small` (warn) makes a remainder over $1
     loud.

If the bridge breaks again, start with the stored rows of the failing test.
Then work through `recon_loss_budget_positions`:

- `where item_residual_dollars <> 0` to see what is unexplained;
- `group by bridge_class` to see which item moved.

## Test inventory

<!-- BEGIN GENERATED TEST INVENTORY (scripts/runbook_tests.py generate) -->

237 tests. Generated from target/manifest.json; do not edit by hand.

### Staging (105 tests)

#### stg_events_other

- **`accepted_values_stg_events_other_accepted_side__yes__no`**
  - Checks: `accepted_side` in `stg_events_other` takes only the listed values.
  - Severity: error
  - A failure means: A new value appeared: a new bot schema / exchange value / seed label. It must get a rule, never a silent bucket.
  - First checks: (1) `select accepted_side, count(*) from stg_events_other group by 1` to find the new value. (2) Trace where it comes from (source, seed, or a CASE in the model). (3) Add a deliberate rule or accepted value (and the docs) and record the decision.

- **`accepted_values_stg_events_other_frame_type__quote_created__quote_accepted__quote_executed`**
  - Checks: `frame_type` in `stg_events_other` takes only the listed values.
  - Severity: error
  - A failure means: A new value appeared: a new bot schema / exchange value / seed label. It must get a rule, never a silent bucket.
  - First checks: (1) `select frame_type, count(*) from stg_events_other group by 1` to find the new value. (2) Trace where it comes from (source, seed, or a CASE in the model). (3) Add a deliberate rule or accepted value (and the docs) and record the decision.

- **`not_null_stg_events_other_event_id`**
  - Checks: `event_id` is never NULL in `stg_events_other`.
  - Severity: error
  - A failure means: Rows reached `stg_events_other` without `event_id`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from stg_events_other where event_id is null limit 20` (or `dbt test -s not_null_stg_events_other_event_id --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_stg_events_other_frame_type`**
  - Checks: `frame_type` is never NULL in `stg_events_other`.
  - Severity: error
  - A failure means: Rows reached `stg_events_other` without `frame_type`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from stg_events_other where frame_type is null limit 20` (or `dbt test -s not_null_stg_events_other_frame_type --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`unique_stg_events_other_event_id`**
  - Checks: `event_id` is unique in `stg_events_other`.
  - Severity: error
  - A failure means: `stg_events_other` has duplicate `event_id` values: a fan-out join, a source re-delivery, or a grain change.
  - First checks: (1) Find the duplicated keys (group by event_id having count(*) > 1; on very large tables group on hash(event_id)). (2) Compare the duplicate rows' payloads: identical = true duplicate, different = a real repeat (e.g. the dual-instance window). (3) Check the model's joins for a many-to-one that became many-to-many.

- **`assert_rowcount_events_other`** (`tests\rowcounts\assert_rowcount_events_other.sql`)
  - Checks: Per source file, staged rows of `events_other` + rows excluded for `_parse_error` equal the extract manifest's `rows_written` (`manifest_rowcount_reconciliation`).
  - Severity: error
  - A failure means: Staging dropped, duplicated or invented rows, or the extract under DBT_DATA_ROOT changed (a file added, replaced or truncated).
  - First checks: (1) Run the test's compiled SQL: it lists each file that does not reconcile, with both counts. (2) Compare that file's manifest row (`_manifest/manifest.parquet`) with the Parquet on disk. On synthetic data, regenerate with the same seed and rebuild. (3) If the files are unchanged, diff the staging model's WHERE / QUALIFY / joins (`git log -p` on the model): staging must never drop real rows.

#### stg_events_rfq_created

- **`not_null_stg_events_rfq_created_rfq_id`**
  - Checks: `rfq_id` is never NULL in `stg_events_rfq_created`.
  - Severity: error
  - A failure means: Rows reached `stg_events_rfq_created` without `rfq_id`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from stg_events_rfq_created where rfq_id is null limit 20` (or `dbt test -s not_null_stg_events_rfq_created_rfq_id --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`unique_stg_events_rfq_created_rfq_id`**
  - Checks: `rfq_id` is unique in `stg_events_rfq_created`.
  - Severity: error
  - A failure means: `stg_events_rfq_created` has duplicate `rfq_id` values: a fan-out join, a source re-delivery, or a grain change.
  - First checks: (1) Find the duplicated keys (group by rfq_id having count(*) > 1; on very large tables group on hash(rfq_id)). (2) Compare the duplicate rows' payloads: identical = true duplicate, different = a real repeat (e.g. the dual-instance window). (3) Check the model's joins for a many-to-one that became many-to-many.

- **`assert_rowcount_events_rfq_created`** (`tests\rowcounts\assert_rowcount_events_rfq_created.sql`)
  - Checks: Per source file, staged rows of `events_rfq_created` + rows excluded for `_parse_error` equal the extract manifest's `rows_written` (`manifest_rowcount_reconciliation`).
  - Severity: error
  - A failure means: Staging dropped, duplicated or invented rows, or the extract under DBT_DATA_ROOT changed (a file added, replaced or truncated).
  - First checks: (1) Run the test's compiled SQL: it lists each file that does not reconcile, with both counts. (2) Compare that file's manifest row (`_manifest/manifest.parquet`) with the Parquet on disk. On synthetic data, regenerate with the same seed and rebuild. (3) If the files are unchanged, diff the staging model's WHERE / QUALIFY / joins (`git log -p` on the model): staging must never drop real rows.

#### stg_exchange_settlements

- **`accepted_values_stg_exchange_settlements_settlement_result__yes__no__scalar`**
  - Checks: `settlement_result` in `stg_exchange_settlements` takes only the listed values.
  - Severity: error
  - A failure means: A new value appeared: a new bot schema / exchange value / seed label. It must get a rule, never a silent bucket.
  - First checks: (1) `select settlement_result, count(*) from stg_exchange_settlements group by 1` to find the new value. (2) Trace where it comes from (source, seed, or a CASE in the model). (3) Add a deliberate rule or accepted value (and the docs) and record the decision.

- **`accepted_values_stg_exchange_settlements_side__yes__no`**
  - Checks: `side` in `stg_exchange_settlements` takes only the listed values.
  - Severity: error
  - A failure means: A new value appeared: a new bot schema / exchange value / seed label. It must get a rule, never a silent bucket.
  - First checks: (1) `select side, count(*) from stg_exchange_settlements group by 1` to find the new value. (2) Trace where it comes from (source, seed, or a CASE in the model). (3) Add a deliberate rule or accepted value (and the docs) and record the decision.

- **`not_null_stg_exchange_settlements_settlement_id`**
  - Checks: `settlement_id` is never NULL in `stg_exchange_settlements`.
  - Severity: error
  - A failure means: Rows reached `stg_exchange_settlements` without `settlement_id`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from stg_exchange_settlements where settlement_id is null limit 20` (or `dbt test -s not_null_stg_exchange_settlements_settlement_id --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_stg_exchange_settlements_side`**
  - Checks: `side` is never NULL in `stg_exchange_settlements`.
  - Severity: error
  - A failure means: Rows reached `stg_exchange_settlements` without `side`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from stg_exchange_settlements where side is null limit 20` (or `dbt test -s not_null_stg_exchange_settlements_side --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`unique_stg_exchange_settlements_settlement_id`**
  - Checks: `settlement_id` is unique in `stg_exchange_settlements`.
  - Severity: error
  - A failure means: `stg_exchange_settlements` has duplicate `settlement_id` values: a fan-out join, a source re-delivery, or a grain change.
  - First checks: (1) Find the duplicated keys (group by settlement_id having count(*) > 1; on very large tables group on hash(settlement_id)). (2) Compare the duplicate rows' payloads: identical = true duplicate, different = a real repeat (e.g. the dual-instance window). (3) Check the model's joins for a many-to-one that became many-to-many.

- **`assert_rowcount_exchange_settlements`** (`tests\rowcounts\assert_rowcount_exchange_settlements.sql`)
  - Checks: Per source file, staged rows of `exchange_settlements` + rows excluded for `_parse_error` equal the extract manifest's `rows_written` (`manifest_rowcount_reconciliation`).
  - Severity: error
  - A failure means: Staging dropped, duplicated or invented rows, or the extract under DBT_DATA_ROOT changed (a file added, replaced or truncated).
  - First checks: (1) Run the test's compiled SQL: it lists each file that does not reconcile, with both counts. (2) Compare that file's manifest row (`_manifest/manifest.parquet`) with the Parquet on disk. On synthetic data, regenerate with the same seed and rebuild. (3) If the files are unchanged, diff the staging model's WHERE / QUALIFY / joins (`git log -p` on the model): staging must never drop real rows.

#### stg_fill_legs

- **`accepted_values_stg_fill_legs_side__yes__no`**
  - Checks: `side` in `stg_fill_legs` takes only the listed values.
  - Severity: error
  - A failure means: A new value appeared: a new bot schema / exchange value / seed label. It must get a rule, never a silent bucket.
  - First checks: (1) `select side, count(*) from stg_fill_legs group by 1` to find the new value. (2) Trace where it comes from (source, seed, or a CASE in the model). (3) Add a deliberate rule or accepted value (and the docs) and record the decision.

- **`dbt_utils_unique_combination_of_columns_stg_fill_legs_fill_id__leg_index`**
  - Checks: The column combination fill_id, leg_index is unique in `stg_fill_legs`.
  - Severity: error
  - A failure means: Duplicate rows at the model's grain.
  - First checks: (1) Group by the columns (on huge tables, by a hash of them) to find duplicates. (2) Compare duplicate rows' lineage (_source_key, _line_no). (3) Check the unnest / join that builds the grain.

- **`not_null_stg_fill_legs_fill_id`**
  - Checks: `fill_id` is never NULL in `stg_fill_legs`.
  - Severity: error
  - A failure means: Rows reached `stg_fill_legs` without `fill_id`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from stg_fill_legs where fill_id is null limit 20` (or `dbt test -s not_null_stg_fill_legs_fill_id --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_stg_fill_legs_fill_leg_id`**
  - Checks: `fill_leg_id` is never NULL in `stg_fill_legs`.
  - Severity: error
  - A failure means: Rows reached `stg_fill_legs` without `fill_leg_id`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from stg_fill_legs where fill_leg_id is null limit 20` (or `dbt test -s not_null_stg_fill_legs_fill_leg_id --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_stg_fill_legs_market_ticker`**
  - Checks: `market_ticker` is never NULL in `stg_fill_legs`.
  - Severity: error
  - A failure means: Rows reached `stg_fill_legs` without `market_ticker`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from stg_fill_legs where market_ticker is null limit 20` (or `dbt test -s not_null_stg_fill_legs_market_ticker --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_stg_fill_legs_side`**
  - Checks: `side` is never NULL in `stg_fill_legs`.
  - Severity: error
  - A failure means: Rows reached `stg_fill_legs` without `side`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from stg_fill_legs where side is null limit 20` (or `dbt test -s not_null_stg_fill_legs_side --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`relationships_stg_fill_legs_fill_id__fill_id__ref_stg_fills_`**
  - Checks: Every `fill_id` in `stg_fill_legs` exists in its parent (stg_fills).
  - Severity: error
  - A failure means: Orphan keys: the parent was built from different data than the child, or the key format changed.
  - First checks: (1) List the orphan keys (anti-join child to parent). (2) Check both models were built in the same run (a partial build can leave one stale). (3) Check the key's format / casing in both models.

- **`relationships_stg_fill_legs_market_ticker__market_ticker__ref_stg_market_results_`**
  - Checks: Every `market_ticker` in `stg_fill_legs` exists in its parent (stg_market_results).
  - Severity: error
  - A failure means: Orphan keys: the parent was built from different data than the child, or the key format changed.
  - First checks: (1) List the orphan keys (anti-join child to parent). (2) Check both models were built in the same run (a partial build can leave one stale). (3) Check the key's format / casing in both models.

- **`unique_stg_fill_legs_fill_leg_id`**
  - Checks: `fill_leg_id` is unique in `stg_fill_legs`.
  - Severity: error
  - A failure means: `stg_fill_legs` has duplicate `fill_leg_id` values: a fan-out join, a source re-delivery, or a grain change.
  - First checks: (1) Find the duplicated keys (group by fill_leg_id having count(*) > 1; on very large tables group on hash(fill_leg_id)). (2) Compare the duplicate rows' payloads: identical = true duplicate, different = a real repeat (e.g. the dual-instance window). (3) Check the model's joins for a many-to-one that became many-to-many.

- **`assert_leg_ticker_prefixes_in_seed`** (`tests\marts\assert_leg_ticker_prefixes_in_seed.sql`)
  - Checks: Stores (warn) every leg ticker prefix that is not in seeds/ticker_prefixes.csv.
  - Severity: warn
  - A failure means: The exchange listed a new series; its markets are unclassified (sport NULL), never silently 'other'.
  - First checks: (1) Read the stored rows (prefix, n_markets, example_ticker) in dbt_test__audit. (2) Look up the series' market titles (dim_markets.title) to classify it. (3) Add a seed row (and any new sport / market_type to _marts__models.yml accepted values), then dbt seed + build.

#### stg_fills

- **`accepted_values_stg_fills_fill_source__ws__rest_poll`**
  - Checks: `fill_source` in `stg_fills` takes only the listed values.
  - Severity: error
  - A failure means: A new value appeared: a new bot schema / exchange value / seed label. It must get a rule, never a silent bucket.
  - First checks: (1) `select fill_source, count(*) from stg_fills group by 1` to find the new value. (2) Trace where it comes from (source, seed, or a CASE in the model). (3) Add a deliberate rule or accepted value (and the docs) and record the decision.

- **`accepted_values_stg_fills_side__yes__no`**
  - Checks: `side` in `stg_fills` takes only the listed values.
  - Severity: error
  - A failure means: A new value appeared: a new bot schema / exchange value / seed label. It must get a rule, never a silent bucket.
  - First checks: (1) `select side, count(*) from stg_fills group by 1` to find the new value. (2) Trace where it comes from (source, seed, or a CASE in the model). (3) Add a deliberate rule or accepted value (and the docs) and record the decision.

- **`not_null_stg_fills_accepted_at_utc`**
  - Checks: `accepted_at_utc` is never NULL in `stg_fills`.
  - Severity: error
  - A failure means: Rows reached `stg_fills` without `accepted_at_utc`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from stg_fills where accepted_at_utc is null limit 20` (or `dbt test -s not_null_stg_fills_accepted_at_utc --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_stg_fills_combo_ticker`**
  - Checks: `combo_ticker` is never NULL in `stg_fills`.
  - Severity: error
  - A failure means: Rows reached `stg_fills` without `combo_ticker`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from stg_fills where combo_ticker is null limit 20` (or `dbt test -s not_null_stg_fills_combo_ticker --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_stg_fills_fill_cost_dollars`**
  - Checks: `fill_cost_dollars` is never NULL in `stg_fills`.
  - Severity: error
  - A failure means: Rows reached `stg_fills` without `fill_cost_dollars`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from stg_fills where fill_cost_dollars is null limit 20` (or `dbt test -s not_null_stg_fills_fill_cost_dollars --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_stg_fills_fill_id`**
  - Checks: `fill_id` is never NULL in `stg_fills`.
  - Severity: error
  - A failure means: Rows reached `stg_fills` without `fill_id`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from stg_fills where fill_id is null limit 20` (or `dbt test -s not_null_stg_fills_fill_id --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_stg_fills_side`**
  - Checks: `side` is never NULL in `stg_fills`.
  - Severity: error
  - A failure means: Rows reached `stg_fills` without `side`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from stg_fills where side is null limit 20` (or `dbt test -s not_null_stg_fills_side --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`unique_stg_fills_fill_id`**
  - Checks: `fill_id` is unique in `stg_fills`.
  - Severity: error
  - A failure means: `stg_fills` has duplicate `fill_id` values: a fan-out join, a source re-delivery, or a grain change.
  - First checks: (1) Find the duplicated keys (group by fill_id having count(*) > 1; on very large tables group on hash(fill_id)). (2) Compare the duplicate rows' payloads: identical = true duplicate, different = a real repeat (e.g. the dual-instance window). (3) Check the model's joins for a many-to-one that became many-to-many.

- **`assert_rowcount_fills_settled`** (`tests\rowcounts\assert_rowcount_fills_settled.sql`)
  - Checks: Per source file, staged rows of `fills_settled` + rows excluded for `_parse_error` equal the extract manifest's `rows_written` (`manifest_rowcount_reconciliation`).
  - Severity: error
  - A failure means: Staging dropped, duplicated or invented rows, or the extract under DBT_DATA_ROOT changed (a file added, replaced or truncated).
  - First checks: (1) Run the test's compiled SQL: it lists each file that does not reconcile, with both counts. (2) Compare that file's manifest row (`_manifest/manifest.parquet`) with the Parquet on disk. On synthetic data, regenerate with the same seed and rebuild. (3) If the files are unchanged, diff the staging model's WHERE / QUALIFY / joins (`git log -p` on the model): staging must never drop real rows.

#### stg_fills_raw

- **`accepted_values_stg_fills_raw_record_schema__fill_v2__fill_v3`**
  - Checks: `record_schema` in `stg_fills_raw` takes only the listed values.
  - Severity: error
  - A failure means: A new value appeared: a new bot schema / exchange value / seed label. It must get a rule, never a silent bucket.
  - First checks: (1) `select record_schema, count(*) from stg_fills_raw group by 1` to find the new value. (2) Trace where it comes from (source, seed, or a CASE in the model). (3) Add a deliberate rule or accepted value (and the docs) and record the decision.

- **`accepted_values_stg_fills_raw_side__yes__no`**
  - Checks: `side` in `stg_fills_raw` takes only the listed values.
  - Severity: error
  - A failure means: A new value appeared: a new bot schema / exchange value / seed label. It must get a rule, never a silent bucket.
  - First checks: (1) `select side, count(*) from stg_fills_raw group by 1` to find the new value. (2) Trace where it comes from (source, seed, or a CASE in the model). (3) Add a deliberate rule or accepted value (and the docs) and record the decision.

- **`not_null_stg_fills_raw_fill_id`**
  - Checks: `fill_id` is never NULL in `stg_fills_raw`.
  - Severity: error
  - A failure means: Rows reached `stg_fills_raw` without `fill_id`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from stg_fills_raw where fill_id is null limit 20` (or `dbt test -s not_null_stg_fills_raw_fill_id --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_stg_fills_raw_fill_record_id`**
  - Checks: `fill_record_id` is never NULL in `stg_fills_raw`.
  - Severity: error
  - A failure means: Rows reached `stg_fills_raw` without `fill_record_id`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from stg_fills_raw where fill_record_id is null limit 20` (or `dbt test -s not_null_stg_fills_raw_fill_record_id --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_stg_fills_raw_side`**
  - Checks: `side` is never NULL in `stg_fills_raw`.
  - Severity: error
  - A failure means: Rows reached `stg_fills_raw` without `side`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from stg_fills_raw where side is null limit 20` (or `dbt test -s not_null_stg_fills_raw_side --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`unique_stg_fills_raw_fill_record_id`**
  - Checks: `fill_record_id` is unique in `stg_fills_raw`.
  - Severity: error
  - A failure means: `stg_fills_raw` has duplicate `fill_record_id` values: a fan-out join, a source re-delivery, or a grain change.
  - First checks: (1) Find the duplicated keys (group by fill_record_id having count(*) > 1; on very large tables group on hash(fill_record_id)). (2) Compare the duplicate rows' payloads: identical = true duplicate, different = a real repeat (e.g. the dual-instance window). (3) Check the model's joins for a many-to-one that became many-to-many.

- **`assert_fills_raw_channel_duplicates`** (`tests\fills\assert_fills_raw_channel_duplicates.sql`)
  - Checks: Stores (warn) every trade the raw fill log recorded twice (REST poll, then websocket).
  - Severity: warn
  - A failure means: Expected to warn: these are planted channel duplicates, handled downstream (the settled file dedupes; settlement attribution counts raw-log fills once). A jump in their number points at a channel problem.
  - First checks: (1) Read the stored rows (channels, reverify_paths) in the dbt_test__audit schema. (2) Confirm assert_fills_raw_repeats_are_channel_duplicates passes (each is a clean pair). (3) Compare their count with earlier runs.

- **`assert_fills_raw_repeats_are_channel_duplicates`** (`tests\fills\assert_fills_raw_repeats_are_channel_duplicates.sql`)
  - Checks: A fill_id repeats in the raw fill log only as one REST-poll record plus one websocket record of the same trade (same ticker, side, count).
  - Severity: error
  - A failure means: A real duplicate (same channel twice) or two conflicting records for one trade id.
  - First checks: (1) Select the failing fill_ids from stg_fills_raw with fill_source, reverify_path and lineage. (2) Compare the records' payloads (count, side, ticker, cost). (3) If the exchange re-used a trade id, escalate: fill_id is the dedupe key everywhere downstream.

- **`assert_rowcount_fills_raw`** (`tests\rowcounts\assert_rowcount_fills_raw.sql`)
  - Checks: Per source file, staged rows of `fills_raw` + rows excluded for `_parse_error` equal the extract manifest's `rows_written` (`manifest_rowcount_reconciliation`).
  - Severity: error
  - A failure means: Staging dropped, duplicated or invented rows, or the extract under DBT_DATA_ROOT changed (a file added, replaced or truncated).
  - First checks: (1) Run the test's compiled SQL: it lists each file that does not reconcile, with both counts. (2) Compare that file's manifest row (`_manifest/manifest.parquet`) with the Parquet on disk. On synthetic data, regenerate with the same seed and rebuild. (3) If the files are unchanged, diff the staging model's WHERE / QUALIFY / joins (`git log -p` on the model): staging must never drop real rows.

#### stg_loss_budget

- **`accepted_values_stg_loss_budget_side__yes__no`**
  - Checks: `side` in `stg_loss_budget` takes only the listed values.
  - Severity: error
  - A failure means: A new value appeared: a new bot schema / exchange value / seed label. It must get a rule, never a silent bucket.
  - First checks: (1) `select side, count(*) from stg_loss_budget group by 1` to find the new value. (2) Trace where it comes from (source, seed, or a CASE in the model). (3) Add a deliberate rule or accepted value (and the docs) and record the decision.

- **`not_null_stg_loss_budget_ledger_entry_id`**
  - Checks: `ledger_entry_id` is never NULL in `stg_loss_budget`.
  - Severity: error
  - A failure means: Rows reached `stg_loss_budget` without `ledger_entry_id`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from stg_loss_budget where ledger_entry_id is null limit 20` (or `dbt test -s not_null_stg_loss_budget_ledger_entry_id --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_stg_loss_budget_settle_key`**
  - Checks: `settle_key` is never NULL in `stg_loss_budget`.
  - Severity: error
  - A failure means: Rows reached `stg_loss_budget` without `settle_key`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from stg_loss_budget where settle_key is null limit 20` (or `dbt test -s not_null_stg_loss_budget_settle_key --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_stg_loss_budget_side`**
  - Checks: `side` is never NULL in `stg_loss_budget`.
  - Severity: error
  - A failure means: Rows reached `stg_loss_budget` without `side`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from stg_loss_budget where side is null limit 20` (or `dbt test -s not_null_stg_loss_budget_side --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`unique_stg_loss_budget_ledger_entry_id`**
  - Checks: `ledger_entry_id` is unique in `stg_loss_budget`.
  - Severity: error
  - A failure means: `stg_loss_budget` has duplicate `ledger_entry_id` values: a fan-out join, a source re-delivery, or a grain change.
  - First checks: (1) Find the duplicated keys (group by ledger_entry_id having count(*) > 1; on very large tables group on hash(ledger_entry_id)). (2) Compare the duplicate rows' payloads: identical = true duplicate, different = a real repeat (e.g. the dual-instance window). (3) Check the model's joins for a many-to-one that became many-to-many.

- **`assert_rowcount_loss_budget`** (`tests\rowcounts\assert_rowcount_loss_budget.sql`)
  - Checks: Per source file, staged rows of `loss_budget` + rows excluded for `_parse_error` equal the extract manifest's `rows_written` (`manifest_rowcount_reconciliation`).
  - Severity: error
  - A failure means: Staging dropped, duplicated or invented rows, or the extract under DBT_DATA_ROOT changed (a file added, replaced or truncated).
  - First checks: (1) Run the test's compiled SQL: it lists each file that does not reconcile, with both counts. (2) Compare that file's manifest row (`_manifest/manifest.parquet`) with the Parquet on disk. On synthetic data, regenerate with the same seed and rebuild. (3) If the files are unchanged, diff the staging model's WHERE / QUALIFY / joins (`git log -p` on the model): staging must never drop real rows.

#### stg_market_results

- **`accepted_values_stg_market_results_market_result__yes__no__scalar`**
  - Checks: `market_result` in `stg_market_results` takes only the listed values.
  - Severity: error
  - A failure means: A new value appeared: a new bot schema / exchange value / seed label. It must get a rule, never a silent bucket.
  - First checks: (1) `select market_result, count(*) from stg_market_results group by 1` to find the new value. (2) Trace where it comes from (source, seed, or a CASE in the model). (3) Add a deliberate rule or accepted value (and the docs) and record the decision.

- **`accepted_values_stg_market_results_result_bucket__scored__void`**
  - Checks: `result_bucket` in `stg_market_results` takes only the listed values.
  - Severity: error
  - A failure means: A new value appeared: a new bot schema / exchange value / seed label. It must get a rule, never a silent bucket.
  - First checks: (1) `select result_bucket, count(*) from stg_market_results group by 1` to find the new value. (2) Trace where it comes from (source, seed, or a CASE in the model). (3) Add a deliberate rule or accepted value (and the docs) and record the decision.

- **`not_null_stg_market_results_market_result`**
  - Checks: `market_result` is never NULL in `stg_market_results`.
  - Severity: error
  - A failure means: Rows reached `stg_market_results` without `market_result`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from stg_market_results where market_result is null limit 20` (or `dbt test -s not_null_stg_market_results_market_result --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_stg_market_results_market_ticker`**
  - Checks: `market_ticker` is never NULL in `stg_market_results`.
  - Severity: error
  - A failure means: Rows reached `stg_market_results` without `market_ticker`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from stg_market_results where market_ticker is null limit 20` (or `dbt test -s not_null_stg_market_results_market_ticker --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`unique_stg_market_results_market_ticker`**
  - Checks: `market_ticker` is unique in `stg_market_results`.
  - Severity: error
  - A failure means: `stg_market_results` has duplicate `market_ticker` values: a fan-out join, a source re-delivery, or a grain change.
  - First checks: (1) Find the duplicated keys (group by market_ticker having count(*) > 1; on very large tables group on hash(market_ticker)). (2) Compare the duplicate rows' payloads: identical = true duplicate, different = a real repeat (e.g. the dual-instance window). (3) Check the model's joins for a many-to-one that became many-to-many.

- **`assert_rowcount_market_results`** (`tests\rowcounts\assert_rowcount_market_results.sql`)
  - Checks: Per source file, staged rows of `market_results` + rows excluded for `_parse_error` equal the extract manifest's `rows_written` (`manifest_rowcount_reconciliation`).
  - Severity: error
  - A failure means: Staging dropped, duplicated or invented rows, or the extract under DBT_DATA_ROOT changed (a file added, replaced or truncated).
  - First checks: (1) Run the test's compiled SQL: it lists each file that does not reconcile, with both counts. (2) Compare that file's manifest row (`_manifest/manifest.parquet`) with the Parquet on disk. On synthetic data, regenerate with the same seed and rebuild. (3) If the files are unchanged, diff the staging model's WHERE / QUALIFY / joins (`git log -p` on the model): staging must never drop real rows.

#### stg_python_graded

- **`accepted_values_stg_python_graded_parlay_result__yes__no__scalar`**
  - Checks: `parlay_result` in `stg_python_graded` takes only the listed values.
  - Severity: error
  - A failure means: A new value appeared: a new bot schema / exchange value / seed label. It must get a rule, never a silent bucket.
  - First checks: (1) `select parlay_result, count(*) from stg_python_graded group by 1` to find the new value. (2) Trace where it comes from (source, seed, or a CASE in the model). (3) Add a deliberate rule or accepted value (and the docs) and record the decision.

- **`dbt_utils_equal_rowcount_stg_python_graded_ref_stg_fills_`**
  - Checks: `stg_python_graded` has the same row count as stg_fills.
  - Severity: error
  - A failure means: Rows were dropped or duplicated between the two models.
  - First checks: (1) Compare the two counts. (2) Look for duplicate keys (fan-out) or an inner join that drops rows. (3) Check whether one of the two was rebuilt without the other.

- **`not_null_stg_python_graded_fill_id`**
  - Checks: `fill_id` is never NULL in `stg_python_graded`.
  - Severity: error
  - A failure means: Rows reached `stg_python_graded` without `fill_id`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from stg_python_graded where fill_id is null limit 20` (or `dbt test -s not_null_stg_python_graded_fill_id --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`relationships_stg_python_graded_fill_id__fill_id__ref_stg_fills_`**
  - Checks: Every `fill_id` in `stg_python_graded` exists in its parent (stg_fills).
  - Severity: error
  - A failure means: Orphan keys: the parent was built from different data than the child, or the key format changed.
  - First checks: (1) List the orphan keys (anti-join child to parent). (2) Check both models were built in the same run (a partial build can leave one stale). (3) Check the key's format / casing in both models.

- **`unique_stg_python_graded_fill_id`**
  - Checks: `fill_id` is unique in `stg_python_graded`.
  - Severity: error
  - A failure means: `stg_python_graded` has duplicate `fill_id` values: a fan-out join, a source re-delivery, or a grain change.
  - First checks: (1) Find the duplicated keys (group by fill_id having count(*) > 1; on very large tables group on hash(fill_id)). (2) Compare the duplicate rows' payloads: identical = true duplicate, different = a real repeat (e.g. the dual-instance window). (3) Check the model's joins for a many-to-one that became many-to-many.

- **`assert_python_totals_match_expected`** (`tests\fills\assert_python_totals_match_expected.sql`)
  - Checks: The legacy Python script's baseline totals (cost, revenue, realized P&L, fees) are the known figures.
  - Severity: error
  - A failure means: The fills_settled extract or stg_python_graded changed, so the comparison is against a different baseline.
  - First checks: (1) Recompute the four sums from stg_python_graded joined to stg_fills. (2) Check the fills_settled row-count test (assert_rowcount_fills_settled). (3) Check stg_python_graded's casts (to_money) and filters for a recent change.

#### stg_quote_submissions

- **`accepted_values_stg_quote_submissions_phase__pre__post__skipped`**
  - Checks: `phase` in `stg_quote_submissions` takes only the listed values.
  - Severity: error
  - A failure means: A new value appeared: a new bot schema / exchange value / seed label. It must get a rule, never a silent bucket.
  - First checks: (1) `select phase, count(*) from stg_quote_submissions group by 1` to find the new value. (2) Trace where it comes from (source, seed, or a CASE in the model). (3) Add a deliberate rule or accepted value (and the docs) and record the decision.

- **`accepted_values_stg_quote_submissions_record_schema__quote_submission_v1__quote_submission_v2`**
  - Checks: `record_schema` in `stg_quote_submissions` takes only the listed values.
  - Severity: error
  - A failure means: A new value appeared: a new bot schema / exchange value / seed label. It must get a rule, never a silent bucket.
  - First checks: (1) `select record_schema, count(*) from stg_quote_submissions group by 1` to find the new value. (2) Trace where it comes from (source, seed, or a CASE in the model). (3) Add a deliberate rule or accepted value (and the docs) and record the decision.

- **`accepted_values_stg_quote_submissions_skip_reason__both_sides_gated__subpenny_both_bids__drop_stale_age_exceeded__drop_stale_predicted`**
  - Checks: `skip_reason` in `stg_quote_submissions` takes only the listed values.
  - Severity: error
  - A failure means: A new value appeared: a new bot schema / exchange value / seed label. It must get a rule, never a silent bucket.
  - First checks: (1) `select skip_reason, count(*) from stg_quote_submissions group by 1` to find the new value. (2) Trace where it comes from (source, seed, or a CASE in the model). (3) Add a deliberate rule or accepted value (and the docs) and record the decision.

- **`not_null_stg_quote_submissions_attempt_id`**
  - Checks: `attempt_id` is never NULL in `stg_quote_submissions`.
  - Severity: error
  - A failure means: Rows reached `stg_quote_submissions` without `attempt_id`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from stg_quote_submissions where attempt_id is null limit 20` (or `dbt test -s not_null_stg_quote_submissions_attempt_id --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_stg_quote_submissions_phase`**
  - Checks: `phase` is never NULL in `stg_quote_submissions`.
  - Severity: error
  - A failure means: Rows reached `stg_quote_submissions` without `phase`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from stg_quote_submissions where phase is null limit 20` (or `dbt test -s not_null_stg_quote_submissions_phase --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_stg_quote_submissions_submission_id`**
  - Checks: `submission_id` is never NULL in `stg_quote_submissions`.
  - Severity: error
  - A failure means: Rows reached `stg_quote_submissions` without `submission_id`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from stg_quote_submissions where submission_id is null limit 20` (or `dbt test -s not_null_stg_quote_submissions_submission_id --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`unique_stg_quote_submissions_submission_id`**
  - Checks: `submission_id` is unique in `stg_quote_submissions`.
  - Severity: error
  - A failure means: `stg_quote_submissions` has duplicate `submission_id` values: a fan-out join, a source re-delivery, or a grain change.
  - First checks: (1) Find the duplicated keys (group by submission_id having count(*) > 1; on very large tables group on hash(submission_id)). (2) Compare the duplicate rows' payloads: identical = true duplicate, different = a real repeat (e.g. the dual-instance window). (3) Check the model's joins for a many-to-one that became many-to-many.

- **`assert_rowcount_quote_submissions`** (`tests\rowcounts\assert_rowcount_quote_submissions.sql`)
  - Checks: Per source file, staged rows of `quote_submissions` + rows excluded for `_parse_error` equal the extract manifest's `rows_written` (`manifest_rowcount_reconciliation`).
  - Severity: error
  - A failure means: Staging dropped, duplicated or invented rows, or the extract under DBT_DATA_ROOT changed (a file added, replaced or truncated).
  - First checks: (1) Run the test's compiled SQL: it lists each file that does not reconcile, with both counts. (2) Compare that file's manifest row (`_manifest/manifest.parquet`) with the Parquet on disk. On synthetic data, regenerate with the same seed and rebuild. (3) If the files are unchanged, diff the staging model's WHERE / QUALIFY / joins (`git log -p` on the model): staging must never drop real rows.

#### stg_rfq_legs

- **`accepted_values_stg_rfq_legs_side__yes__no`**
  - Checks: `side` in `stg_rfq_legs` takes only the listed values.
  - Severity: error
  - A failure means: A new value appeared: a new bot schema / exchange value / seed label. It must get a rule, never a silent bucket.
  - First checks: (1) `select side, count(*) from stg_rfq_legs group by 1` to find the new value. (2) Trace where it comes from (source, seed, or a CASE in the model). (3) Add a deliberate rule or accepted value (and the docs) and record the decision.

- **`dbt_utils_unique_combination_of_columns_stg_rfq_legs_rfq_outcome_id__leg_index`**
  - Checks: The column combination rfq_outcome_id, leg_index is unique in `stg_rfq_legs`.
  - Severity: error (tag full_rebuild_only: excluded from the heavy selector; runs in the default build)
  - A failure means: Duplicate rows at the model's grain.
  - First checks: (1) Group by the columns (on huge tables, by a hash of them) to find duplicates. (2) Compare duplicate rows' lineage (_source_key, _line_no). (3) Check the unnest / join that builds the grain.

- **`not_null_stg_rfq_legs_leg_index`**
  - Checks: `leg_index` is never NULL in `stg_rfq_legs`.
  - Severity: error
  - A failure means: Rows reached `stg_rfq_legs` without `leg_index`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from stg_rfq_legs where leg_index is null limit 20` (or `dbt test -s not_null_stg_rfq_legs_leg_index --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_stg_rfq_legs_market_ticker`**
  - Checks: `market_ticker` is never NULL in `stg_rfq_legs`.
  - Severity: error
  - A failure means: Rows reached `stg_rfq_legs` without `market_ticker`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from stg_rfq_legs where market_ticker is null limit 20` (or `dbt test -s not_null_stg_rfq_legs_market_ticker --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_stg_rfq_legs_rfq_id`**
  - Checks: `rfq_id` is never NULL in `stg_rfq_legs`.
  - Severity: error
  - A failure means: Rows reached `stg_rfq_legs` without `rfq_id`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from stg_rfq_legs where rfq_id is null limit 20` (or `dbt test -s not_null_stg_rfq_legs_rfq_id --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_stg_rfq_legs_rfq_outcome_id`**
  - Checks: `rfq_outcome_id` is never NULL in `stg_rfq_legs`.
  - Severity: error
  - A failure means: Rows reached `stg_rfq_legs` without `rfq_outcome_id`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from stg_rfq_legs where rfq_outcome_id is null limit 20` (or `dbt test -s not_null_stg_rfq_legs_rfq_outcome_id --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_stg_rfq_legs_side`**
  - Checks: `side` is never NULL in `stg_rfq_legs`.
  - Severity: error
  - A failure means: Rows reached `stg_rfq_legs` without `side`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from stg_rfq_legs where side is null limit 20` (or `dbt test -s not_null_stg_rfq_legs_side --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

#### stg_rfq_outcomes

- **`accepted_values_stg_rfq_outcomes_record_schema__rfq_outcome_v1__would_quote_v2__exposure_rejected_v2__breaker_skip_v2__balance_guard_skip_v1`**
  - Checks: `record_schema` in `stg_rfq_outcomes` takes only the listed values.
  - Severity: error
  - A failure means: A new value appeared: a new bot schema / exchange value / seed label. It must get a rule, never a silent bucket.
  - First checks: (1) `select record_schema, count(*) from stg_rfq_outcomes group by 1` to find the new value. (2) Trace where it comes from (source, seed, or a CASE in the model). (3) Add a deliberate rule or accepted value (and the docs) and record the decision.

- **`accepted_values_stg_rfq_outcomes_stage__submitted__priced_logged__priced_rejected__gated_skipped__balance_skipped__breaker_skipped__priced_presubmit`**
  - Checks: `stage` in `stg_rfq_outcomes` takes only the listed values.
  - Severity: error
  - A failure means: A new value appeared: a new bot schema / exchange value / seed label. It must get a rule, never a silent bucket.
  - First checks: (1) `select stage, count(*) from stg_rfq_outcomes group by 1` to find the new value. (2) Trace where it comes from (source, seed, or a CASE in the model). (3) Add a deliberate rule or accepted value (and the docs) and record the decision.

- **`not_null_stg_rfq_outcomes_received_at_utc`**
  - Checks: `received_at_utc` is never NULL in `stg_rfq_outcomes`.
  - Severity: error
  - A failure means: Rows reached `stg_rfq_outcomes` without `received_at_utc`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from stg_rfq_outcomes where received_at_utc is null limit 20` (or `dbt test -s not_null_stg_rfq_outcomes_received_at_utc --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_stg_rfq_outcomes_rfq_id`**
  - Checks: `rfq_id` is never NULL in `stg_rfq_outcomes`.
  - Severity: error
  - A failure means: Rows reached `stg_rfq_outcomes` without `rfq_id`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from stg_rfq_outcomes where rfq_id is null limit 20` (or `dbt test -s not_null_stg_rfq_outcomes_rfq_id --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_stg_rfq_outcomes_rfq_outcome_id`**
  - Checks: `rfq_outcome_id` is never NULL in `stg_rfq_outcomes`.
  - Severity: error
  - A failure means: Rows reached `stg_rfq_outcomes` without `rfq_outcome_id`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from stg_rfq_outcomes where rfq_outcome_id is null limit 20` (or `dbt test -s not_null_stg_rfq_outcomes_rfq_outcome_id --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_stg_rfq_outcomes_stage`**
  - Checks: `stage` is never NULL in `stg_rfq_outcomes`.
  - Severity: error
  - A failure means: Rows reached `stg_rfq_outcomes` without `stage`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from stg_rfq_outcomes where stage is null limit 20` (or `dbt test -s not_null_stg_rfq_outcomes_stage --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`unique_stg_rfq_outcomes_rfq_outcome_id`**
  - Checks: `rfq_outcome_id` is unique in `stg_rfq_outcomes`.
  - Severity: error
  - A failure means: `stg_rfq_outcomes` has duplicate `rfq_outcome_id` values: a fan-out join, a source re-delivery, or a grain change.
  - First checks: (1) Find the duplicated keys (group by rfq_outcome_id having count(*) > 1; on very large tables group on hash(rfq_outcome_id)). (2) Compare the duplicate rows' payloads: identical = true duplicate, different = a real repeat (e.g. the dual-instance window). (3) Check the model's joins for a many-to-one that became many-to-many.

- **`assert_rfq_legs_match_outcome_leg_counts`** (`tests\rfq\assert_rfq_legs_match_outcome_leg_counts.sql`)
  - Checks: Cheap stand-in for unique(rfq_outcome_id, leg_index) on stg_rfq_legs: every outcome's legs are exactly 1..len(legs) (count, min, max, sum agree).
  - Severity: error
  - A failure means: stg_rfq_legs and stg_rfq_outcomes no longer have the same leg set: one was rebuilt without the other, or the unnest changed.
  - First checks: (1) Check when each external_parquet model's files were last written (work/staging_parquet/); a change to stg_rfq_outcomes needs stg_rfq_legs rebuilt after it. (2) Run the compiled SQL and look at one failing outcome's legs in both models. (3) Run the exact `full_rebuild_only` unique test on stg_rfq_legs to confirm.

- **`assert_rfq_outcomes_repeats_only_in_dual_instance_window`** (`tests\rfq\assert_rfq_outcomes_repeats_only_in_dual_instance_window.sql`)
  - Checks: rfq_id repeats only inside the dual-instance window (vars dual_instance_*), twice each, and as many as the generator planted.
  - Severity: error
  - A failure means: A repeated rfq_id outside the window: a real duplicate in the decision log, or staging fanned rows out.
  - First checks: (1) List the repeated rfq_ids with their _source_key / _line_no and received_at_utc. (2) Compare the two rows' raw payloads: identical means a true duplicate; different means a real second decision (a new dual-instance episode). (3) Do not change the grain (stg_rfq_outcomes is keyed on the source line) without a design review.

- **`assert_rowcount_rfq_outcomes`** (`tests\rowcounts\assert_rowcount_rfq_outcomes.sql`)
  - Checks: Per source file, staged rows of `rfq_outcomes` + rows excluded for `_parse_error` equal the extract manifest's `rows_written` (`manifest_rowcount_reconciliation`).
  - Severity: error
  - A failure means: Staging dropped, duplicated or invented rows, or the extract under DBT_DATA_ROOT changed (a file added, replaced or truncated).
  - First checks: (1) Run the test's compiled SQL: it lists each file that does not reconcile, with both counts. (2) Compare that file's manifest row (`_manifest/manifest.parquet`) with the Parquet on disk. On synthetic data, regenerate with the same seed and rebuild. (3) If the files are unchanged, diff the staging model's WHERE / QUALIFY / joins (`git log -p` on the model): staging must never drop real rows.

#### stg_rfq_outcomes_slim

- **`accepted_values_stg_rfq_outcomes_slim_stage__filtered__submitted__priced_logged__priced_rejected__gated_skipped__balance_skipped__breaker_skipped`**
  - Checks: `stage` in `stg_rfq_outcomes_slim` takes only the listed values.
  - Severity: error
  - A failure means: A new value appeared: a new bot schema / exchange value / seed label. It must get a rule, never a silent bucket.
  - First checks: (1) `select stage, count(*) from stg_rfq_outcomes_slim group by 1` to find the new value. (2) Trace where it comes from (source, seed, or a CASE in the model). (3) Add a deliberate rule or accepted value (and the docs) and record the decision.

- **`not_null_stg_rfq_outcomes_slim_rfq_id`**
  - Checks: `rfq_id` is never NULL in `stg_rfq_outcomes_slim`.
  - Severity: error
  - A failure means: Rows reached `stg_rfq_outcomes_slim` without `rfq_id`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from stg_rfq_outcomes_slim where rfq_id is null limit 20` (or `dbt test -s not_null_stg_rfq_outcomes_slim_rfq_id --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_stg_rfq_outcomes_slim_stage`**
  - Checks: `stage` is never NULL in `stg_rfq_outcomes_slim`.
  - Severity: error
  - A failure means: Rows reached `stg_rfq_outcomes_slim` without `stage`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from stg_rfq_outcomes_slim where stage is null limit 20` (or `dbt test -s not_null_stg_rfq_outcomes_slim_stage --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`unique_stg_rfq_outcomes_slim_rfq_id`**
  - Checks: `rfq_id` is unique in `stg_rfq_outcomes_slim`.
  - Severity: error
  - A failure means: `stg_rfq_outcomes_slim` has duplicate `rfq_id` values: a fan-out join, a source re-delivery, or a grain change.
  - First checks: (1) Find the duplicated keys (group by rfq_id having count(*) > 1; on very large tables group on hash(rfq_id)). (2) Compare the duplicate rows' payloads: identical = true duplicate, different = a real repeat (e.g. the dual-instance window). (3) Check the model's joins for a many-to-one that became many-to-many.

- **`assert_rowcount_rfq_outcomes_slim`** (`tests\rowcounts\assert_rowcount_rfq_outcomes_slim.sql`)
  - Checks: Per source file, staged rows of `rfq_outcomes_slim` + rows excluded for `_parse_error` equal the extract manifest's `rows_written` (`manifest_rowcount_reconciliation`).
  - Severity: error
  - A failure means: Staging dropped, duplicated or invented rows, or the extract under DBT_DATA_ROOT changed (a file added, replaced or truncated).
  - First checks: (1) Run the test's compiled SQL: it lists each file that does not reconcile, with both counts. (2) Compare that file's manifest row (`_manifest/manifest.parquet`) with the Parquet on disk. On synthetic data, regenerate with the same seed and rebuild. (3) If the files are unchanged, diff the staging model's WHERE / QUALIFY / joins (`git log -p` on the model): staging must never drop real rows.

#### stg_taker_log

- **`accepted_values_stg_taker_log_log_kind__rfq__quote__accepted`**
  - Checks: `log_kind` in `stg_taker_log` takes only the listed values.
  - Severity: error
  - A failure means: A new value appeared: a new bot schema / exchange value / seed label. It must get a rule, never a silent bucket.
  - First checks: (1) `select log_kind, count(*) from stg_taker_log group by 1` to find the new value. (2) Trace where it comes from (source, seed, or a CASE in the model). (3) Add a deliberate rule or accepted value (and the docs) and record the decision.

- **`not_null_stg_taker_log_log_kind`**
  - Checks: `log_kind` is never NULL in `stg_taker_log`.
  - Severity: error
  - A failure means: Rows reached `stg_taker_log` without `log_kind`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from stg_taker_log where log_kind is null limit 20` (or `dbt test -s not_null_stg_taker_log_log_kind --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_stg_taker_log_logged_at_utc`**
  - Checks: `logged_at_utc` is never NULL in `stg_taker_log`.
  - Severity: error
  - A failure means: Rows reached `stg_taker_log` without `logged_at_utc`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from stg_taker_log where logged_at_utc is null limit 20` (or `dbt test -s not_null_stg_taker_log_logged_at_utc --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_stg_taker_log_taker_log_id`**
  - Checks: `taker_log_id` is never NULL in `stg_taker_log`.
  - Severity: error
  - A failure means: Rows reached `stg_taker_log` without `taker_log_id`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from stg_taker_log where taker_log_id is null limit 20` (or `dbt test -s not_null_stg_taker_log_taker_log_id --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`unique_stg_taker_log_taker_log_id`**
  - Checks: `taker_log_id` is unique in `stg_taker_log`.
  - Severity: error
  - A failure means: `stg_taker_log` has duplicate `taker_log_id` values: a fan-out join, a source re-delivery, or a grain change.
  - First checks: (1) Find the duplicated keys (group by taker_log_id having count(*) > 1; on very large tables group on hash(taker_log_id)). (2) Compare the duplicate rows' payloads: identical = true duplicate, different = a real repeat (e.g. the dual-instance window). (3) Check the model's joins for a many-to-one that became many-to-many.

#### stg_taker_plays

- **`accepted_values_stg_taker_plays_action__take__pass`**
  - Checks: `action` in `stg_taker_plays` takes only the listed values.
  - Severity: error
  - A failure means: A new value appeared: a new bot schema / exchange value / seed label. It must get a rule, never a silent bucket.
  - First checks: (1) `select action, count(*) from stg_taker_plays group by 1` to find the new value. (2) Trace where it comes from (source, seed, or a CASE in the model). (3) Add a deliberate rule or accepted value (and the docs) and record the decision.

- **`accepted_values_stg_taker_plays_is_field_count_ok__True`**
  - Checks: `is_field_count_ok` in `stg_taker_plays` takes only the listed values.
  - Severity: error
  - A failure means: A new value appeared: a new bot schema / exchange value / seed label. It must get a rule, never a silent bucket.
  - First checks: (1) `select is_field_count_ok, count(*) from stg_taker_plays group by 1` to find the new value. (2) Trace where it comes from (source, seed, or a CASE in the model). (3) Add a deliberate rule or accepted value (and the docs) and record the decision.

- **`accepted_values_stg_taker_plays_strategy__taker_a__taker_b`**
  - Checks: `strategy` in `stg_taker_plays` takes only the listed values.
  - Severity: error
  - A failure means: A new value appeared: a new bot schema / exchange value / seed label. It must get a rule, never a silent bucket.
  - First checks: (1) `select strategy, count(*) from stg_taker_plays group by 1` to find the new value. (2) Trace where it comes from (source, seed, or a CASE in the model). (3) Add a deliberate rule or accepted value (and the docs) and record the decision.

- **`not_null_stg_taker_plays_action`**
  - Checks: `action` is never NULL in `stg_taker_plays`.
  - Severity: error
  - A failure means: Rows reached `stg_taker_plays` without `action`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from stg_taker_plays where action is null limit 20` (or `dbt test -s not_null_stg_taker_plays_action --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_stg_taker_plays_combo_ticker`**
  - Checks: `combo_ticker` is never NULL in `stg_taker_plays`.
  - Severity: error
  - A failure means: Rows reached `stg_taker_plays` without `combo_ticker`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from stg_taker_plays where combo_ticker is null limit 20` (or `dbt test -s not_null_stg_taker_plays_combo_ticker --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_stg_taker_plays_strategy`**
  - Checks: `strategy` is never NULL in `stg_taker_plays`.
  - Severity: error
  - A failure means: Rows reached `stg_taker_plays` without `strategy`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from stg_taker_plays where strategy is null limit 20` (or `dbt test -s not_null_stg_taker_plays_strategy --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_stg_taker_plays_taker_play_id`**
  - Checks: `taker_play_id` is never NULL in `stg_taker_plays`.
  - Severity: error
  - A failure means: Rows reached `stg_taker_plays` without `taker_play_id`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from stg_taker_plays where taker_play_id is null limit 20` (or `dbt test -s not_null_stg_taker_plays_taker_play_id --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`unique_stg_taker_plays_taker_play_id`**
  - Checks: `taker_play_id` is unique in `stg_taker_plays`.
  - Severity: error
  - A failure means: `stg_taker_plays` has duplicate `taker_play_id` values: a fan-out join, a source re-delivery, or a grain change.
  - First checks: (1) Find the duplicated keys (group by taker_play_id having count(*) > 1; on very large tables group on hash(taker_play_id)). (2) Compare the duplicate rows' payloads: identical = true duplicate, different = a real repeat (e.g. the dual-instance window). (3) Check the model's joins for a many-to-one that became many-to-many.

- **`assert_rowcount_taker`** (`tests\rowcounts\assert_rowcount_taker.sql`)
  - Checks: The taker ledgers stage exactly the rows of their files (plays: data rows; log: lines), per strategy.
  - Severity: error
  - A failure means: A taker file changed, or the text split in stg_taker_plays / stg_taker_log changed.
  - First checks: (1) Count the lines of each taker/<strategy>/plays.csv (minus the header) and log.jsonl. (2) Check `is_field_count_ok` in stg_taker_plays (a quoted comma would break the plain split). (3) Check the filename regex that derives `strategy` and `_source_key`.

### Intermediate (39 tests)

#### int_exchange_settlements_attributed

- **`accepted_values_int_exchange_settlements_attributed_attribution_source__bot__bot_taker__manual_single_market__manual_combo__bot_plus_other_activity__known_artifact__bot_fill_log_only__unexplained`**
  - Checks: `attribution_source` in `int_exchange_settlements_attributed` takes only the listed values.
  - Severity: error
  - A failure means: A new value appeared: a new bot schema / exchange value / seed label. It must get a rule, never a silent bucket.
  - First checks: (1) `select attribution_source, count(*) from int_exchange_settlements_attributed group by 1` to find the new value. (2) Trace where it comes from (source, seed, or a CASE in the model). (3) Add a deliberate rule or accepted value (and the docs) and record the decision.

- **`accepted_values_int_exchange_settlements_attributed_match_kind__exact__exact_ex_fee__cost_estimated__count_differs__cost_differs__no_fills_this_side__no_fills_on_ticker`**
  - Checks: `match_kind` in `int_exchange_settlements_attributed` takes only the listed values.
  - Severity: error
  - A failure means: A new value appeared: a new bot schema / exchange value / seed label. It must get a rule, never a silent bucket.
  - First checks: (1) `select match_kind, count(*) from int_exchange_settlements_attributed group by 1` to find the new value. (2) Trace where it comes from (source, seed, or a CASE in the model). (3) Add a deliberate rule or accepted value (and the docs) and record the decision.

- **`dbt_utils_equal_rowcount_int_exchange_settlements_attributed_ref_stg_exchange_settlements_`**
  - Checks: `int_exchange_settlements_attributed` has the same row count as stg_exchange_settlements.
  - Severity: error
  - A failure means: Rows were dropped or duplicated between the two models.
  - First checks: (1) Compare the two counts. (2) Look for duplicate keys (fan-out) or an inner join that drops rows. (3) Check whether one of the two was rebuilt without the other.

- **`not_null_int_exchange_settlements_attributed_attribution_source`**
  - Checks: `attribution_source` is never NULL in `int_exchange_settlements_attributed`.
  - Severity: error
  - A failure means: Rows reached `int_exchange_settlements_attributed` without `attribution_source`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from int_exchange_settlements_attributed where attribution_source is null limit 20` (or `dbt test -s not_null_int_exchange_settlements_attributed_attribution_source --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_int_exchange_settlements_attributed_match_kind`**
  - Checks: `match_kind` is never NULL in `int_exchange_settlements_attributed`.
  - Severity: error
  - A failure means: Rows reached `int_exchange_settlements_attributed` without `match_kind`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from int_exchange_settlements_attributed where match_kind is null limit 20` (or `dbt test -s not_null_int_exchange_settlements_attributed_match_kind --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_int_exchange_settlements_attributed_settlement_id`**
  - Checks: `settlement_id` is never NULL in `int_exchange_settlements_attributed`.
  - Severity: error
  - A failure means: Rows reached `int_exchange_settlements_attributed` without `settlement_id`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from int_exchange_settlements_attributed where settlement_id is null limit 20` (or `dbt test -s not_null_int_exchange_settlements_attributed_settlement_id --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_int_exchange_settlements_attributed_underlying_source`**
  - Checks: `underlying_source` is never NULL in `int_exchange_settlements_attributed`.
  - Severity: error
  - A failure means: Rows reached `int_exchange_settlements_attributed` without `underlying_source`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from int_exchange_settlements_attributed where underlying_source is null limit 20` (or `dbt test -s not_null_int_exchange_settlements_attributed_underlying_source --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`unique_int_exchange_settlements_attributed_settlement_id`**
  - Checks: `settlement_id` is unique in `int_exchange_settlements_attributed`.
  - Severity: error
  - A failure means: `int_exchange_settlements_attributed` has duplicate `settlement_id` values: a fan-out join, a source re-delivery, or a grain change.
  - First checks: (1) Find the duplicated keys (group by settlement_id having count(*) > 1; on very large tables group on hash(settlement_id)). (2) Compare the duplicate rows' payloads: identical = true duplicate, different = a real repeat (e.g. the dual-instance window). (3) Check the model's joins for a many-to-one that became many-to-many.

- **`assert_exchange_settlements_unexplained`** (`tests\settlement\assert_exchange_settlements_unexplained.sql`)
  - Checks: Stores (warn) every exchange settlement row no attribution rule explains.
  - Severity: warn
  - A failure means: A settlement row that is not bot / taker / manual / known artifact: new account activity or a rule gap. Rows are in dbt_test__audit for review.
  - First checks: (1) Read the stored rows (match_kind, count_diff, cost_diff_dollars). (2) Look up the ticker in stg_fills, stg_fills_raw and stg_taker_plays. (3) Add a rule to int_exchange_settlements_attributed and record it.

#### int_fill_legs_resolved

- **`accepted_values_int_fill_legs_resolved_leg_outcome__won__lost__scalar__void`**
  - Checks: `leg_outcome` in `int_fill_legs_resolved` takes only the listed values.
  - Severity: error
  - A failure means: A new value appeared: a new bot schema / exchange value / seed label. It must get a rule, never a silent bucket.
  - First checks: (1) `select leg_outcome, count(*) from int_fill_legs_resolved group by 1` to find the new value. (2) Trace where it comes from (source, seed, or a CASE in the model). (3) Add a deliberate rule or accepted value (and the docs) and record the decision.

- **`not_null_int_fill_legs_resolved_fill_id`**
  - Checks: `fill_id` is never NULL in `int_fill_legs_resolved`.
  - Severity: error
  - A failure means: Rows reached `int_fill_legs_resolved` without `fill_id`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from int_fill_legs_resolved where fill_id is null limit 20` (or `dbt test -s not_null_int_fill_legs_resolved_fill_id --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_int_fill_legs_resolved_fill_leg_id`**
  - Checks: `fill_leg_id` is never NULL in `int_fill_legs_resolved`.
  - Severity: error
  - A failure means: Rows reached `int_fill_legs_resolved` without `fill_leg_id`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from int_fill_legs_resolved where fill_leg_id is null limit 20` (or `dbt test -s not_null_int_fill_legs_resolved_fill_leg_id --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_int_fill_legs_resolved_leg_outcome`**
  - Checks: `leg_outcome` is never NULL in `int_fill_legs_resolved`.
  - Severity: error
  - A failure means: Rows reached `int_fill_legs_resolved` without `leg_outcome`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from int_fill_legs_resolved where leg_outcome is null limit 20` (or `dbt test -s not_null_int_fill_legs_resolved_leg_outcome --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`relationships_int_fill_legs_resolved_fill_id__fill_id__ref_stg_fills_`**
  - Checks: Every `fill_id` in `int_fill_legs_resolved` exists in its parent (stg_fills).
  - Severity: error
  - A failure means: Orphan keys: the parent was built from different data than the child, or the key format changed.
  - First checks: (1) List the orphan keys (anti-join child to parent). (2) Check both models were built in the same run (a partial build can leave one stale). (3) Check the key's format / casing in both models.

- **`unique_int_fill_legs_resolved_fill_leg_id`**
  - Checks: `fill_leg_id` is unique in `int_fill_legs_resolved`.
  - Severity: error
  - A failure means: `int_fill_legs_resolved` has duplicate `fill_leg_id` values: a fan-out join, a source re-delivery, or a grain change.
  - First checks: (1) Find the duplicated keys (group by fill_leg_id having count(*) > 1; on very large tables group on hash(fill_leg_id)). (2) Compare the duplicate rows' payloads: identical = true duplicate, different = a real repeat (e.g. the dual-instance window). (3) Check the model's joins for a many-to-one that became many-to-many.

#### int_fill_positions

- **`accepted_values_int_fill_positions_position_origin__bot_maker__taker_script__mixed`**
  - Checks: `position_origin` in `int_fill_positions` takes only the listed values.
  - Severity: error
  - A failure means: A new value appeared: a new bot schema / exchange value / seed label. It must get a rule, never a silent bucket.
  - First checks: (1) `select position_origin, count(*) from int_fill_positions group by 1` to find the new value. (2) Trace where it comes from (source, seed, or a CASE in the model). (3) Add a deliberate rule or accepted value (and the docs) and record the decision.

- **`not_null_int_fill_positions_position_id`**
  - Checks: `position_id` is never NULL in `int_fill_positions`.
  - Severity: error
  - A failure means: Rows reached `int_fill_positions` without `position_id`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from int_fill_positions where position_id is null limit 20` (or `dbt test -s not_null_int_fill_positions_position_id --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_int_fill_positions_position_origin`**
  - Checks: `position_origin` is never NULL in `int_fill_positions`.
  - Severity: error
  - A failure means: Rows reached `int_fill_positions` without `position_origin`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from int_fill_positions where position_origin is null limit 20` (or `dbt test -s not_null_int_fill_positions_position_origin --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`unique_int_fill_positions_position_id`**
  - Checks: `position_id` is unique in `int_fill_positions`.
  - Severity: error
  - A failure means: `int_fill_positions` has duplicate `position_id` values: a fan-out join, a source re-delivery, or a grain change.
  - First checks: (1) Find the duplicated keys (group by position_id having count(*) > 1; on very large tables group on hash(position_id)). (2) Compare the duplicate rows' payloads: identical = true duplicate, different = a real repeat (e.g. the dual-instance window). (3) Check the model's joins for a many-to-one that became many-to-many.

#### int_fills_graded

- **`accepted_values_int_fills_graded_parlay_result__yes__no__scalar__result_anomalous`**
  - Checks: `parlay_result` in `int_fills_graded` takes only the listed values.
  - Severity: error
  - A failure means: A new value appeared: a new bot schema / exchange value / seed label. It must get a rule, never a silent bucket.
  - First checks: (1) `select parlay_result, count(*) from int_fills_graded group by 1` to find the new value. (2) Trace where it comes from (source, seed, or a CASE in the model). (3) Add a deliberate rule or accepted value (and the docs) and record the decision.

- **`dbt_utils_equal_rowcount_int_fills_graded_ref_stg_fills_`**
  - Checks: `int_fills_graded` has the same row count as stg_fills.
  - Severity: error
  - A failure means: Rows were dropped or duplicated between the two models.
  - First checks: (1) Compare the two counts. (2) Look for duplicate keys (fan-out) or an inner join that drops rows. (3) Check whether one of the two was rebuilt without the other.

- **`dbt_utils_expression_is_true_int_fills_graded_revenue_paid_dollars___revenue_exact_dollars_and_revenue_paid_dollars_revenue_exact_dollars_0_01`**
  - Checks: The expression `<= revenue_exact_dollars and revenue_paid_dollars > revenue_exact_dollars - 0.01` holds on every row of `int_fills_graded`.
  - Severity: error
  - A failure means: A row breaks the model's invariant (see the column description).
  - First checks: (1) Select the rows where the expression is false. (2) Recompute the columns involved from their inputs. (3) Check the macro / formula that builds them for a change.

- **`not_null_int_fills_graded_fill_id`**
  - Checks: `fill_id` is never NULL in `int_fills_graded`.
  - Severity: error
  - A failure means: Rows reached `int_fills_graded` without `fill_id`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from int_fills_graded where fill_id is null limit 20` (or `dbt test -s not_null_int_fills_graded_fill_id --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_int_fills_graded_parlay_result`**
  - Checks: `parlay_result` is never NULL in `int_fills_graded`.
  - Severity: error
  - A failure means: Rows reached `int_fills_graded` without `parlay_result`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from int_fills_graded where parlay_result is null limit 20` (or `dbt test -s not_null_int_fills_graded_parlay_result --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`unique_int_fills_graded_fill_id`**
  - Checks: `fill_id` is unique in `int_fills_graded`.
  - Severity: error
  - A failure means: `int_fills_graded` has duplicate `fill_id` values: a fan-out join, a source re-delivery, or a grain change.
  - First checks: (1) Find the duplicated keys (group by fill_id having count(*) > 1; on very large tables group on hash(fill_id)). (2) Compare the duplicate rows' payloads: identical = true duplicate, different = a real repeat (e.g. the dual-instance window). (3) Check the model's joins for a many-to-one that became many-to-many.

- **`assert_int_fills_graded_rowcount_expected`** (`tests\fills\assert_int_fills_graded_rowcount_expected.sql`)
  - Checks: int_fills_graded has exactly the fills of the settled file (the generator's count).
  - Severity: error
  - A failure means: Grading dropped or duplicated fills (a join fan-out or an inner join losing fills).
  - First checks: (1) Compare with `select count(*) from staging.stg_fills`. (2) Check for duplicate fill_id in int_fills_graded and for fills with no legs. (3) Diff int_fills_graded / int_fill_legs_resolved joins (`git log -p`).

#### int_fills_origin

- **`accepted_values_int_fills_origin_fill_origin__bot_maker__taker_script`**
  - Checks: `fill_origin` in `int_fills_origin` takes only the listed values.
  - Severity: error
  - A failure means: A new value appeared: a new bot schema / exchange value / seed label. It must get a rule, never a silent bucket.
  - First checks: (1) `select fill_origin, count(*) from int_fills_origin group by 1` to find the new value. (2) Trace where it comes from (source, seed, or a CASE in the model). (3) Add a deliberate rule or accepted value (and the docs) and record the decision.

- **`dbt_utils_equal_rowcount_int_fills_origin_ref_stg_fills_`**
  - Checks: `int_fills_origin` has the same row count as stg_fills.
  - Severity: error
  - A failure means: Rows were dropped or duplicated between the two models.
  - First checks: (1) Compare the two counts. (2) Look for duplicate keys (fan-out) or an inner join that drops rows. (3) Check whether one of the two was rebuilt without the other.

- **`not_null_int_fills_origin_fill_id`**
  - Checks: `fill_id` is never NULL in `int_fills_origin`.
  - Severity: error
  - A failure means: Rows reached `int_fills_origin` without `fill_id`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from int_fills_origin where fill_id is null limit 20` (or `dbt test -s not_null_int_fills_origin_fill_id --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_int_fills_origin_fill_origin`**
  - Checks: `fill_origin` is never NULL in `int_fills_origin`.
  - Severity: error
  - A failure means: Rows reached `int_fills_origin` without `fill_origin`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from int_fills_origin where fill_origin is null limit 20` (or `dbt test -s not_null_int_fills_origin_fill_origin --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`unique_int_fills_origin_fill_id`**
  - Checks: `fill_id` is unique in `int_fills_origin`.
  - Severity: error
  - A failure means: `int_fills_origin` has duplicate `fill_id` values: a fan-out join, a source re-delivery, or a grain change.
  - First checks: (1) Find the duplicated keys (group by fill_id having count(*) > 1; on very large tables group on hash(fill_id)). (2) Compare the duplicate rows' payloads: identical = true duplicate, different = a real repeat (e.g. the dual-instance window). (3) Check the model's joins for a many-to-one that became many-to-many.

- **`assert_taker_fills_identified`** (`tests\fills\assert_taker_fills_identified.sql`)
  - Checks: int_fills_origin finds exactly the planted taker-script fills and the one self-trade lookalike.
  - Severity: error
  - A failure means: The taker match rule (combo ticker + contract count) or the taker ledgers changed.
  - First checks: (1) Count int_fills_origin by fill_origin / is_on_taker_ledger / is_self_trade_candidate. (2) Check stg_taker_plays takes and their contract counts. (3) Re-read int_fills_origin's header for the rule.

#### int_loss_budget_ledger

- **`accepted_values_int_loss_budget_ledger_bridge_window__before_window__in_window__after_window`**
  - Checks: `bridge_window` in `int_loss_budget_ledger` takes only the listed values.
  - Severity: error
  - A failure means: A new value appeared: a new bot schema / exchange value / seed label. It must get a rule, never a silent bucket.
  - First checks: (1) `select bridge_window, count(*) from int_loss_budget_ledger group by 1` to find the new value. (2) Trace where it comes from (source, seed, or a CASE in the model). (3) Add a deliberate rule or accepted value (and the docs) and record the decision.

- **`not_null_int_loss_budget_ledger_bridge_window`**
  - Checks: `bridge_window` is never NULL in `int_loss_budget_ledger`.
  - Severity: error
  - A failure means: Rows reached `int_loss_budget_ledger` without `bridge_window`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from int_loss_budget_ledger where bridge_window is null limit 20` (or `dbt test -s not_null_int_loss_budget_ledger_bridge_window --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_int_loss_budget_ledger_ledger_entry_id`**
  - Checks: `ledger_entry_id` is never NULL in `int_loss_budget_ledger`.
  - Severity: error
  - A failure means: Rows reached `int_loss_budget_ledger` without `ledger_entry_id`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from int_loss_budget_ledger where ledger_entry_id is null limit 20` (or `dbt test -s not_null_int_loss_budget_ledger_ledger_entry_id --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_int_loss_budget_ledger_ledger_seq`**
  - Checks: `ledger_seq` is never NULL in `int_loss_budget_ledger`.
  - Severity: error
  - A failure means: Rows reached `int_loss_budget_ledger` without `ledger_seq`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from int_loss_budget_ledger where ledger_seq is null limit 20` (or `dbt test -s not_null_int_loss_budget_ledger_ledger_seq --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`unique_int_loss_budget_ledger_ledger_entry_id`**
  - Checks: `ledger_entry_id` is unique in `int_loss_budget_ledger`.
  - Severity: error
  - A failure means: `int_loss_budget_ledger` has duplicate `ledger_entry_id` values: a fan-out join, a source re-delivery, or a grain change.
  - First checks: (1) Find the duplicated keys (group by ledger_entry_id having count(*) > 1; on very large tables group on hash(ledger_entry_id)). (2) Compare the duplicate rows' payloads: identical = true duplicate, different = a real repeat (e.g. the dual-instance window). (3) Check the model's joins for a many-to-one that became many-to-many.

- **`unique_int_loss_budget_ledger_ledger_seq`**
  - Checks: `ledger_seq` is unique in `int_loss_budget_ledger`.
  - Severity: error
  - A failure means: `int_loss_budget_ledger` has duplicate `ledger_seq` values: a fan-out join, a source re-delivery, or a grain change.
  - First checks: (1) Find the duplicated keys (group by ledger_seq having count(*) > 1; on very large tables group on hash(ledger_seq)). (2) Compare the duplicate rows' payloads: identical = true duplicate, different = a real repeat (e.g. the dual-instance window). (3) Check the model's joins for a many-to-one that became many-to-many.

- **`assert_loss_budget_window_sums_to_cumulative`** (`tests\settlement\assert_loss_budget_window_sums_to_cumulative.sql`)
  - Checks: Inside the bridge window the ledger's cumulative equals the running sum of pnl, and the bridged entry's cumulative is the known target.
  - Severity: error
  - A failure means: A new restart / replay inside the window, or the bridged entry moved (var loss_budget_bridge_end_before_utc).
  - First checks: (1) List int_loss_budget_ledger rows with is_cumulative_break. (2) Check var loss_budget_bridge_end_before_utc in dbt_project.yml. (3) Check stg_loss_budget's ordering columns (recorded_at_utc, _source_key, _line_no).

### Reconciliation (22 tests)

#### recon_fills_python_vs_dbt

- **`accepted_values_recon_fills_python_vs_dbt_mismatch_category__python_scalar_patch_ignored_fill_side__python_scalar_patch_assumed_half__python_books_exact_not_paid_cent__python_pnl_rounded_4dp__unexplained`**
  - Checks: `mismatch_category` in `recon_fills_python_vs_dbt` takes only the listed values.
  - Severity: error
  - A failure means: A new value appeared: a new bot schema / exchange value / seed label. It must get a rule, never a silent bucket.
  - First checks: (1) `select mismatch_category, count(*) from recon_fills_python_vs_dbt group by 1` to find the new value. (2) Trace where it comes from (source, seed, or a CASE in the model). (3) Add a deliberate rule or accepted value (and the docs) and record the decision.

- **`not_null_recon_fills_python_vs_dbt_fill_id`**
  - Checks: `fill_id` is never NULL in `recon_fills_python_vs_dbt`.
  - Severity: error
  - A failure means: Rows reached `recon_fills_python_vs_dbt` without `fill_id`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from recon_fills_python_vs_dbt where fill_id is null limit 20` (or `dbt test -s not_null_recon_fills_python_vs_dbt_fill_id --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_recon_fills_python_vs_dbt_is_mismatch`**
  - Checks: `is_mismatch` is never NULL in `recon_fills_python_vs_dbt`.
  - Severity: error
  - A failure means: Rows reached `recon_fills_python_vs_dbt` without `is_mismatch`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from recon_fills_python_vs_dbt where is_mismatch is null limit 20` (or `dbt test -s not_null_recon_fills_python_vs_dbt_is_mismatch --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`unique_recon_fills_python_vs_dbt_fill_id`**
  - Checks: `fill_id` is unique in `recon_fills_python_vs_dbt`.
  - Severity: error
  - A failure means: `recon_fills_python_vs_dbt` has duplicate `fill_id` values: a fan-out join, a source re-delivery, or a grain change.
  - First checks: (1) Find the duplicated keys (group by fill_id having count(*) > 1; on very large tables group on hash(fill_id)). (2) Compare the duplicate rows' payloads: identical = true duplicate, different = a real repeat (e.g. the dual-instance window). (3) Check the model's joins for a many-to-one that became many-to-many.

- **`assert_recon_no_unexplained_mismatches`** (`tests\fills\assert_recon_no_unexplained_mismatches.sql`)
  - Checks: Every fill where dbt and the legacy script disagree falls in a named mismatch_category.
  - Severity: error
  - A failure means: A new kind of grading difference. Never loosen the test: give it a rule in recon_fills_python_vs_dbt.
  - First checks: (1) Select the 'unexplained' rows from recon_fills_python_vs_dbt (fill_id, both results, diffs). (2) Trace one fill's legs: fct_fill_legs / int_fill_legs_resolved vs stg_python_graded.legs_settled. (3) Decide whose answer is right, write the rule, and record the decision.

- **`assert_recon_totals_fully_attributed`** (`tests\fills\assert_recon_totals_fully_attributed.sql`)
  - Checks: The dbt-minus-script totals sit entirely on categorized mismatch rows (agreeing fills sum to < half a cent).
  - Severity: error
  - A failure means: Differences are leaking through the 'agree' threshold: many sub-$0.000001 differences or a threshold change.
  - First checks: (1) Sum revenue_diff / pnl_diff where not is_mismatch. (2) Check money precision (to_money, floor_to_cent) for a change. (3) Check the is_mismatch thresholds in recon_fills_python_vs_dbt.

#### recon_loss_budget_bridge

- **`not_null_recon_loss_budget_bridge_amount_dollars`**
  - Checks: `amount_dollars` is never NULL in `recon_loss_budget_bridge`.
  - Severity: error
  - A failure means: Rows reached `recon_loss_budget_bridge` without `amount_dollars`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from recon_loss_budget_bridge where amount_dollars is null limit 20` (or `dbt test -s not_null_recon_loss_budget_bridge_amount_dollars --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_recon_loss_budget_bridge_step_order`**
  - Checks: `step_order` is never NULL in `recon_loss_budget_bridge`.
  - Severity: error
  - A failure means: Rows reached `recon_loss_budget_bridge` without `step_order`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from recon_loss_budget_bridge where step_order is null limit 20` (or `dbt test -s not_null_recon_loss_budget_bridge_step_order --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`unique_recon_loss_budget_bridge_step_order`**
  - Checks: `step_order` is unique in `recon_loss_budget_bridge`.
  - Severity: error
  - A failure means: `recon_loss_budget_bridge` has duplicate `step_order` values: a fan-out join, a source re-delivery, or a grain change.
  - First checks: (1) Find the duplicated keys (group by step_order having count(*) > 1; on very large tables group on hash(step_order)). (2) Compare the duplicate rows' payloads: identical = true duplicate, different = a real repeat (e.g. the dual-instance window). (3) Check the model's joins for a many-to-one that became many-to-many.

- **`assert_loss_budget_bridge_ties`** (`tests\settlement\assert_loss_budget_bridge_ties.sql`)
  - Checks: The waterfall's start + items (remainder included) equals the ledger figure, to the cent.
  - Severity: error
  - A failure means: The bridge arithmetic is broken (an item double-counted or missing), not that the remainder grew.
  - First checks: (1) Sum recon_loss_budget_positions item columns and compare with ledger_pnl_in_window. (2) Check every bridge_class is assigned to exactly one item in recon_loss_budget_bridge. (3) Re-run recon_loss_budget_positions and recon_loss_budget_bridge together.

- **`assert_loss_budget_remainder_small`** (`tests\settlement\assert_loss_budget_remainder_small.sql`)
  - Checks: Warns when the bridge's unexplained remainder exceeds $1 (on the synthetic data it is +$0.37, one position).
  - Severity: warn
  - A failure means: Something new is unexplained in the ledger vs bot P&L. The remainder is still reported, not forced.
  - First checks: (1) Find positions with item_residual_dollars <> 0 in recon_loss_budget_positions. (2) Check whether they have a settlement-log row (has_settlement_row) and their cost_source. (3) Classify the cause and add a bridge item.

#### recon_loss_budget_positions

- **`accepted_values_recon_loss_budget_positions_bridge_class__bot_in_window__bot_before_window__bot_after_window__bot_ledger_write_lost__bot_settled_after_ledger_end__bot_never_reconciled__other_outside_window__manual_single_market__other_activity_opposite_side__bot_fill_log_only__manual_combo`**
  - Checks: `bridge_class` in `recon_loss_budget_positions` takes only the listed values.
  - Severity: error
  - A failure means: A new value appeared: a new bot schema / exchange value / seed label. It must get a rule, never a silent bucket.
  - First checks: (1) `select bridge_class, count(*) from recon_loss_budget_positions group by 1` to find the new value. (2) Trace where it comes from (source, seed, or a CASE in the model). (3) Add a deliberate rule or accepted value (and the docs) and record the decision.

- **`not_null_recon_loss_budget_positions_bridge_class`**
  - Checks: `bridge_class` is never NULL in `recon_loss_budget_positions`.
  - Severity: error
  - A failure means: Rows reached `recon_loss_budget_positions` without `bridge_class`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from recon_loss_budget_positions where bridge_class is null limit 20` (or `dbt test -s not_null_recon_loss_budget_positions_bridge_class --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_recon_loss_budget_positions_position_id`**
  - Checks: `position_id` is never NULL in `recon_loss_budget_positions`.
  - Severity: error
  - A failure means: Rows reached `recon_loss_budget_positions` without `position_id`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from recon_loss_budget_positions where position_id is null limit 20` (or `dbt test -s not_null_recon_loss_budget_positions_position_id --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`unique_recon_loss_budget_positions_position_id`**
  - Checks: `position_id` is unique in `recon_loss_budget_positions`.
  - Severity: error
  - A failure means: `recon_loss_budget_positions` has duplicate `position_id` values: a fan-out join, a source re-delivery, or a grain change.
  - First checks: (1) Find the duplicated keys (group by position_id having count(*) > 1; on very large tables group on hash(position_id)). (2) Compare the duplicate rows' payloads: identical = true duplicate, different = a real repeat (e.g. the dual-instance window). (3) Check the model's joins for a many-to-one that became many-to-many.

- **`assert_bot_positions_never_reconciled`** (`tests\settlement\assert_bot_positions_never_reconciled.sql`)
  - Checks: Stores (warn) every bot position the exchange settled but the reconciler never booked.
  - Severity: warn
  - A failure means: Expected to warn: the pagination bug's footprint (one page read per poll). The bridge accounts for all of them; after a fix, this list should go to zero for new data.
  - First checks: (1) Read the stored rows; group by first_leg_settled_at_utc hour to see the busy settlement batches. (2) Compare with recon_loss_budget_bridge item 'never booked'. (3) Check the reconciler's page handling (it must follow the cursor until the page is short).

#### recon_rfq_completeness_hourly

- **`accepted_values_recon_rfq_completeness_hourly_likely_cause__bot_off_no_data__no_gap__no_gap_within_tolerance__bot_off__bot_off_partial__decision_log_tail_loss__tail_loss_after_fix__bot_stopped__unexplained`**
  - Checks: `likely_cause` in `recon_rfq_completeness_hourly` takes only the listed values.
  - Severity: error
  - A failure means: A new value appeared: a new bot schema / exchange value / seed label. It must get a rule, never a silent bucket.
  - First checks: (1) `select likely_cause, count(*) from recon_rfq_completeness_hourly group by 1` to find the new value. (2) Trace where it comes from (source, seed, or a CASE in the model). (3) Add a deliberate rule or accepted value (and the docs) and record the decision.

- **`not_null_recon_rfq_completeness_hourly_hour_utc`**
  - Checks: `hour_utc` is never NULL in `recon_rfq_completeness_hourly`.
  - Severity: error
  - A failure means: Rows reached `recon_rfq_completeness_hourly` without `hour_utc`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from recon_rfq_completeness_hourly where hour_utc is null limit 20` (or `dbt test -s not_null_recon_rfq_completeness_hourly_hour_utc --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_recon_rfq_completeness_hourly_likely_cause`**
  - Checks: `likely_cause` is never NULL in `recon_rfq_completeness_hourly`.
  - Severity: error
  - A failure means: Rows reached `recon_rfq_completeness_hourly` without `likely_cause`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from recon_rfq_completeness_hourly where likely_cause is null limit 20` (or `dbt test -s not_null_recon_rfq_completeness_hourly_likely_cause --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`unique_recon_rfq_completeness_hourly_hour_utc`**
  - Checks: `hour_utc` is unique in `recon_rfq_completeness_hourly`.
  - Severity: error
  - A failure means: `recon_rfq_completeness_hourly` has duplicate `hour_utc` values: a fan-out join, a source re-delivery, or a grain change.
  - First checks: (1) Find the duplicated keys (group by hour_utc having count(*) > 1; on very large tables group on hash(hour_utc)). (2) Compare the duplicate rows' payloads: identical = true duplicate, different = a real repeat (e.g. the dual-instance window). (3) Check the model's joins for a many-to-one that became many-to-many.

- **`assert_rfq_completeness_gaps_for_review`** (`tests\reconciliation\assert_rfq_completeness_gaps_for_review.sql`)
  - Checks: Stores (warn) every completeness-window hour where the bot recorded fewer RFQs than the exchange sent, with its likely_cause.
  - Severity: warn
  - A failure means: Expected to warn: the planted end-of-hour losses (decision_log_tail_loss) and the planted pricer outage (bot_stopped, bot_off, bot_off_partial). Any other likely_cause, above all 'unexplained', is new and gets an investigation.
  - First checks: (1) Read the stored rows; filter likely_cause <> 'decision_log_tail_loss'. (2) For that hour, count sent vs recorded per minute to see where the gap sits; compare recording_stopped_sec_before_hour_end with events_stopped_sec_before_hour_end. (3) Check deploy / restart records for that day and record the cause.

- **`assert_rfq_completeness_hours_complete`** (`tests\reconciliation\assert_rfq_completeness_hours_complete.sql`)
  - Checks: recon_rfq_completeness_hourly has every hour of the window and head + mid + tail = missing on each.
  - Severity: error
  - A failure means: An hour vanished (window vars changed) or the gap split double-counts.
  - First checks: (1) Count rows and find missing hours against generate_series. (2) Check vars completeness_window_start_utc / _end_utc. (3) Check recording_bounds is one row per hour.

### Marts (65 tests)

#### dim_combos

- **`accepted_values_dim_combos_sport_group__baseball__basketball__hockey__football__soccer__tennis__multi_sport`**
  - Checks: `sport_group` in `dim_combos` takes only the listed values.
  - Severity: error
  - A failure means: A new value appeared: a new bot schema / exchange value / seed label. It must get a rule, never a silent bucket.
  - First checks: (1) `select sport_group, count(*) from dim_combos group by 1` to find the new value. (2) Trace where it comes from (source, seed, or a CASE in the model). (3) Add a deliberate rule or accepted value (and the docs) and record the decision.

- **`not_null_dim_combos_combo_ticker`**
  - Checks: `combo_ticker` is never NULL in `dim_combos`.
  - Severity: error
  - A failure means: Rows reached `dim_combos` without `combo_ticker`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from dim_combos where combo_ticker is null limit 20` (or `dbt test -s not_null_dim_combos_combo_ticker --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_dim_combos_n_legs`**
  - Checks: `n_legs` is never NULL in `dim_combos`.
  - Severity: error
  - A failure means: Rows reached `dim_combos` without `n_legs`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from dim_combos where n_legs is null limit 20` (or `dbt test -s not_null_dim_combos_n_legs --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`unique_dim_combos_combo_ticker`**
  - Checks: `combo_ticker` is unique in `dim_combos`.
  - Severity: error
  - A failure means: `dim_combos` has duplicate `combo_ticker` values: a fan-out join, a source re-delivery, or a grain change.
  - First checks: (1) Find the duplicated keys (group by combo_ticker having count(*) > 1; on very large tables group on hash(combo_ticker)). (2) Compare the duplicate rows' payloads: identical = true duplicate, different = a real repeat (e.g. the dual-instance window). (3) Check the model's joins for a many-to-one that became many-to-many.

- **`assert_same_game_flag_agrees`** (`tests\marts\assert_same_game_flag_agrees.sql`)
  - Checks: Stores (warn) combos where the bot's same-game flag disagrees with the ticker-based one.
  - Severity: warn
  - A failure means: Expected to warn: planted combos whose same-game leg the bot logged with no game slug. The bot would not have applied same-game limits to them.
  - First checks: (1) Read the stored rows; look at the combos' legs in fct_fill_legs (bot_game_slug in dim_markets). (2) Check dim_markets.game_key for the legs (same competition + date + matchup). (3) If the ticker parse is wrong instead, fix dim_markets and its selection tests.

#### dim_markets

- **`accepted_values_dim_markets_market_type__moneyline__total__spread__btts__advance__prop_hr__prop_first_inning_run__prop_pts__prop_td`**
  - Checks: `market_type` in `dim_markets` takes only the listed values.
  - Severity: error
  - A failure means: A new value appeared: a new bot schema / exchange value / seed label. It must get a rule, never a silent bucket.
  - First checks: (1) `select market_type, count(*) from dim_markets group by 1` to find the new value. (2) Trace where it comes from (source, seed, or a CASE in the model). (3) Add a deliberate rule or accepted value (and the docs) and record the decision.

- **`accepted_values_dim_markets_sport__baseball__basketball__hockey__football__soccer__tennis`**
  - Checks: `sport` in `dim_markets` takes only the listed values.
  - Severity: error
  - A failure means: A new value appeared: a new bot schema / exchange value / seed label. It must get a rule, never a silent bucket.
  - First checks: (1) `select sport, count(*) from dim_markets group by 1` to find the new value. (2) Trace where it comes from (source, seed, or a CASE in the model). (3) Add a deliberate rule or accepted value (and the docs) and record the decision.

- **`not_null_dim_markets_market_ticker`**
  - Checks: `market_ticker` is never NULL in `dim_markets`.
  - Severity: error
  - A failure means: Rows reached `dim_markets` without `market_ticker`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from dim_markets where market_ticker is null limit 20` (or `dbt test -s not_null_dim_markets_market_ticker --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_dim_markets_ticker_prefix`**
  - Checks: `ticker_prefix` is never NULL in `dim_markets`.
  - Severity: error
  - A failure means: Rows reached `dim_markets` without `ticker_prefix`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from dim_markets where ticker_prefix is null limit 20` (or `dbt test -s not_null_dim_markets_ticker_prefix --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`relationships_dim_markets_ticker_prefix__ticker_prefix__ref_ticker_prefixes_`**
  - Checks: Every `ticker_prefix` in `dim_markets` exists in its parent (ticker_prefixes).
  - Severity: warn
  - A failure means: Orphan keys: the parent was built from different data than the child, or the key format changed.
  - First checks: (1) List the orphan keys (anti-join child to parent). (2) Check both models were built in the same run (a partial build can leave one stale). (3) Check the key's format / casing in both models.

- **`unique_dim_markets_market_ticker`**
  - Checks: `market_ticker` is unique in `dim_markets`.
  - Severity: error
  - A failure means: `dim_markets` has duplicate `market_ticker` values: a fan-out join, a source re-delivery, or a grain change.
  - First checks: (1) Find the duplicated keys (group by market_ticker having count(*) > 1; on very large tables group on hash(market_ticker)). (2) Compare the duplicate rows' payloads: identical = true duplicate, different = a real repeat (e.g. the dual-instance window). (3) Check the model's joins for a many-to-one that became many-to-many.

- **`assert_every_mart_column_documented`** (`tests\marts\assert_every_mart_column_documented.sql`)
  - Checks: Every column of every non-heavy mart has a YAML description.
  - Severity: error
  - A failure means: A mart gained a column without docs (or a mart was not built).
  - First checks: (1) Run the test's SQL: it lists model and column. (2) Add the description in models/marts/_marts*__models.yml. (3) If the row says '<relation not built>', build that mart first.

#### fct_fill_legs

- **`accepted_values_fct_fill_legs_market_type__moneyline__total__spread__btts__advance__prop_hr__prop_first_inning_run__prop_pts__prop_td`**
  - Checks: `market_type` in `fct_fill_legs` takes only the listed values.
  - Severity: error
  - A failure means: A new value appeared: a new bot schema / exchange value / seed label. It must get a rule, never a silent bucket.
  - First checks: (1) `select market_type, count(*) from fct_fill_legs group by 1` to find the new value. (2) Trace where it comes from (source, seed, or a CASE in the model). (3) Add a deliberate rule or accepted value (and the docs) and record the decision.

- **`accepted_values_fct_fill_legs_sport__baseball__basketball__hockey__football__soccer__tennis`**
  - Checks: `sport` in `fct_fill_legs` takes only the listed values.
  - Severity: error
  - A failure means: A new value appeared: a new bot schema / exchange value / seed label. It must get a rule, never a silent bucket.
  - First checks: (1) `select sport, count(*) from fct_fill_legs group by 1` to find the new value. (2) Trace where it comes from (source, seed, or a CASE in the model). (3) Add a deliberate rule or accepted value (and the docs) and record the decision.

- **`not_null_fct_fill_legs_fill_id`**
  - Checks: `fill_id` is never NULL in `fct_fill_legs`.
  - Severity: error
  - A failure means: Rows reached `fct_fill_legs` without `fill_id`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from fct_fill_legs where fill_id is null limit 20` (or `dbt test -s not_null_fct_fill_legs_fill_id --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_fct_fill_legs_fill_leg_id`**
  - Checks: `fill_leg_id` is never NULL in `fct_fill_legs`.
  - Severity: error
  - A failure means: Rows reached `fct_fill_legs` without `fill_leg_id`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from fct_fill_legs where fill_leg_id is null limit 20` (or `dbt test -s not_null_fct_fill_legs_fill_leg_id --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_fct_fill_legs_market_ticker`**
  - Checks: `market_ticker` is never NULL in `fct_fill_legs`.
  - Severity: error
  - A failure means: Rows reached `fct_fill_legs` without `market_ticker`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from fct_fill_legs where market_ticker is null limit 20` (or `dbt test -s not_null_fct_fill_legs_market_ticker --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`relationships_fct_fill_legs_fill_id__fill_id__ref_fct_fills_`**
  - Checks: Every `fill_id` in `fct_fill_legs` exists in its parent (fct_fills).
  - Severity: error
  - A failure means: Orphan keys: the parent was built from different data than the child, or the key format changed.
  - First checks: (1) List the orphan keys (anti-join child to parent). (2) Check both models were built in the same run (a partial build can leave one stale). (3) Check the key's format / casing in both models.

- **`relationships_fct_fill_legs_market_ticker__market_ticker__ref_dim_markets_`**
  - Checks: Every `market_ticker` in `fct_fill_legs` exists in its parent (dim_markets).
  - Severity: error
  - A failure means: Orphan keys: the parent was built from different data than the child, or the key format changed.
  - First checks: (1) List the orphan keys (anti-join child to parent). (2) Check both models were built in the same run (a partial build can leave one stale). (3) Check the key's format / casing in both models.

- **`unique_fct_fill_legs_fill_leg_id`**
  - Checks: `fill_leg_id` is unique in `fct_fill_legs`.
  - Severity: error
  - A failure means: `fct_fill_legs` has duplicate `fill_leg_id` values: a fan-out join, a source re-delivery, or a grain change.
  - First checks: (1) Find the duplicated keys (group by fill_leg_id having count(*) > 1; on very large tables group on hash(fill_leg_id)). (2) Compare the duplicate rows' payloads: identical = true duplicate, different = a real repeat (e.g. the dual-instance window). (3) Check the model's joins for a many-to-one that became many-to-many.

#### fct_fills

- **`accepted_values_fct_fills_fill_origin__bot_maker__taker_script`**
  - Checks: `fill_origin` in `fct_fills` takes only the listed values.
  - Severity: error
  - A failure means: A new value appeared: a new bot schema / exchange value / seed label. It must get a rule, never a silent bucket.
  - First checks: (1) `select fill_origin, count(*) from fct_fills group by 1` to find the new value. (2) Trace where it comes from (source, seed, or a CASE in the model). (3) Add a deliberate rule or accepted value (and the docs) and record the decision.

- **`accepted_values_fct_fills_side__yes__no`**
  - Checks: `side` in `fct_fills` takes only the listed values.
  - Severity: error
  - A failure means: A new value appeared: a new bot schema / exchange value / seed label. It must get a rule, never a silent bucket.
  - First checks: (1) `select side, count(*) from fct_fills group by 1` to find the new value. (2) Trace where it comes from (source, seed, or a CASE in the model). (3) Add a deliberate rule or accepted value (and the docs) and record the decision.

- **`not_null_fct_fills_accepted_at_utc`**
  - Checks: `accepted_at_utc` is never NULL in `fct_fills`.
  - Severity: error
  - A failure means: Rows reached `fct_fills` without `accepted_at_utc`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from fct_fills where accepted_at_utc is null limit 20` (or `dbt test -s not_null_fct_fills_accepted_at_utc --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_fct_fills_accepted_date_local`**
  - Checks: `accepted_date_local` is never NULL in `fct_fills`.
  - Severity: error
  - A failure means: Rows reached `fct_fills` without `accepted_date_local`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from fct_fills where accepted_date_local is null limit 20` (or `dbt test -s not_null_fct_fills_accepted_date_local --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_fct_fills_combo_ticker`**
  - Checks: `combo_ticker` is never NULL in `fct_fills`.
  - Severity: error
  - A failure means: Rows reached `fct_fills` without `combo_ticker`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from fct_fills where combo_ticker is null limit 20` (or `dbt test -s not_null_fct_fills_combo_ticker --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_fct_fills_fill_id`**
  - Checks: `fill_id` is never NULL in `fct_fills`.
  - Severity: error
  - A failure means: Rows reached `fct_fills` without `fill_id`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from fct_fills where fill_id is null limit 20` (or `dbt test -s not_null_fct_fills_fill_id --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_fct_fills_fill_origin`**
  - Checks: `fill_origin` is never NULL in `fct_fills`.
  - Severity: error
  - A failure means: Rows reached `fct_fills` without `fill_origin`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from fct_fills where fill_origin is null limit 20` (or `dbt test -s not_null_fct_fills_fill_origin --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_fct_fills_side`**
  - Checks: `side` is never NULL in `fct_fills`.
  - Severity: error
  - A failure means: Rows reached `fct_fills` without `side`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from fct_fills where side is null limit 20` (or `dbt test -s not_null_fct_fills_side --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`relationships_fct_fills_combo_ticker__combo_ticker__ref_dim_combos_`**
  - Checks: Every `combo_ticker` in `fct_fills` exists in its parent (dim_combos).
  - Severity: error
  - A failure means: Orphan keys: the parent was built from different data than the child, or the key format changed.
  - First checks: (1) List the orphan keys (anti-join child to parent). (2) Check both models were built in the same run (a partial build can leave one stale). (3) Check the key's format / casing in both models.

- **`unique_fct_fills_fill_id`**
  - Checks: `fill_id` is unique in `fct_fills`.
  - Severity: error
  - A failure means: `fct_fills` has duplicate `fill_id` values: a fan-out join, a source re-delivery, or a grain change.
  - First checks: (1) Find the duplicated keys (group by fill_id having count(*) > 1; on very large tables group on hash(fill_id)). (2) Compare the duplicate rows' payloads: identical = true duplicate, different = a real repeat (e.g. the dual-instance window). (3) Check the model's joins for a many-to-one that became many-to-many.

- **`assert_fills_for_matches_direct_query`** (`tests\marts\assert_fills_for_matches_direct_query.sql`)
  - Checks: The fills_for macro returns exactly what a hand-written Continental Cup query returns, at both grains, and the question is not empty.
  - Severity: error
  - A failure means: The macro's filters or grain logic changed, so ad-hoc extracts would be wrong.
  - First checks: (1) Compile a fills_for call (analyses/continental_cup_fills.sql) and read the SQL. (2) Check macros/fills_for.sql's condition building (_fills_for_in, date_basis). (3) Check fct_fill_legs' competition / accepted_date_local columns.

- **`assert_marts_tie_to_graded_totals`** (`tests\marts\assert_marts_tie_to_graded_totals.sql`)
  - Checks: fct_fills and mart_daily_pnl both total the generator's independently computed gross P&L, cost, fees and net P&L (fee-known fills).
  - Severity: error
  - A failure means: A mart dropped / duplicated fills, or grading upstream changed the totals.
  - First checks: (1) Compare each total in fct_fills, mart_daily_pnl and int_fills_graded. (2) Check fct_fills joins (graded, origin, combos) for fan-out: count(*) vs count(distinct fill_id). (3) If grading changed on purpose, the generator's grading must change with it.

- **`assert_monitoring_marts_tie_to_facts`** (`tests\marts\assert_monitoring_marts_tie_to_facts.sql`)
  - Checks: mart_market_maker_monitoring and mart_requester_activity carry every bot maker fill in fct_fills (incl. those with no RFQ link), every bot maker fill is either matched to a counted live quote or counted as unmatched, and both marts carry the same number of counted RFQ decisions. Heavy.
  - Severity: error
  - A failure means: One of the monitoring marts dropped or double-counted rows (a join or filter changed), so its rates are off.
  - First checks: (1) Compare the fill counts the test returns; find which mart differs. (2) Group that mart's source CTE by creator / day and diff against fct_fills (fill_origin, rfq_creator_id null). (3) `git log -p` the two mart SQL files for a new filter or join.

#### fct_rfq_lifecycle

- **`accepted_values_fct_rfq_lifecycle_submit_bucket__live_quote__dropped_before_sending__sent_but_rejected__send_outcome_unknown__no_submission_record`**
  - Checks: `submit_bucket` in `fct_rfq_lifecycle` takes only the listed values.
  - Severity: error
  - A failure means: A new value appeared: a new bot schema / exchange value / seed label. It must get a rule, never a silent bucket.
  - First checks: (1) `select submit_bucket, count(*) from fct_rfq_lifecycle group by 1` to find the new value. (2) Trace where it comes from (source, seed, or a CASE in the model). (3) Add a deliberate rule or accepted value (and the docs) and record the decision.

- **`not_null_fct_rfq_lifecycle_is_counted_decision`**
  - Checks: `is_counted_decision` is never NULL in `fct_rfq_lifecycle`.
  - Severity: error
  - A failure means: Rows reached `fct_rfq_lifecycle` without `is_counted_decision`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from fct_rfq_lifecycle where is_counted_decision is null limit 20` (or `dbt test -s not_null_fct_rfq_lifecycle_is_counted_decision --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_fct_rfq_lifecycle_is_filled`**
  - Checks: `is_filled` is never NULL in `fct_rfq_lifecycle`.
  - Severity: error
  - A failure means: Rows reached `fct_rfq_lifecycle` without `is_filled`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from fct_rfq_lifecycle where is_filled is null limit 20` (or `dbt test -s not_null_fct_rfq_lifecycle_is_filled --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_fct_rfq_lifecycle_rfq_id`**
  - Checks: `rfq_id` is never NULL in `fct_rfq_lifecycle`.
  - Severity: error
  - A failure means: Rows reached `fct_rfq_lifecycle` without `rfq_id`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from fct_rfq_lifecycle where rfq_id is null limit 20` (or `dbt test -s not_null_fct_rfq_lifecycle_rfq_id --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_fct_rfq_lifecycle_rfq_outcome_id`**
  - Checks: `rfq_outcome_id` is never NULL in `fct_rfq_lifecycle`.
  - Severity: error
  - A failure means: Rows reached `fct_rfq_lifecycle` without `rfq_outcome_id`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from fct_rfq_lifecycle where rfq_outcome_id is null limit 20` (or `dbt test -s not_null_fct_rfq_lifecycle_rfq_outcome_id --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_fct_rfq_lifecycle_stage`**
  - Checks: `stage` is never NULL in `fct_rfq_lifecycle`.
  - Severity: error
  - A failure means: Rows reached `fct_rfq_lifecycle` without `stage`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from fct_rfq_lifecycle where stage is null limit 20` (or `dbt test -s not_null_fct_rfq_lifecycle_stage --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`unique_fct_rfq_lifecycle_rfq_outcome_id`**
  - Checks: `rfq_outcome_id` is unique in `fct_rfq_lifecycle`.
  - Severity: error (tag full_rebuild_only: excluded from the heavy selector; runs in the default build)
  - A failure means: `fct_rfq_lifecycle` has duplicate `rfq_outcome_id` values: a fan-out join, a source re-delivery, or a grain change.
  - First checks: (1) Find the duplicated keys (group by rfq_outcome_id having count(*) > 1; on very large tables group on hash(rfq_outcome_id)). (2) Compare the duplicate rows' payloads: identical = true duplicate, different = a real repeat (e.g. the dual-instance window). (3) Check the model's joins for a many-to-one that became many-to-many.

- **`assert_every_heavy_mart_column_documented`** (`tests\marts\assert_every_heavy_mart_column_documented.sql`)
  - Checks: Every column of fct_rfq_lifecycle and the two monitoring marts has a YAML description.
  - Severity: error
  - A failure means: A heavy mart gained a column without docs.
  - First checks: (1) Run the test's SQL: it lists model and column. (2) Add the description in models/marts/_marts_rfq__models.yml or _marts_monitoring__models.yml. (3) Rebuild with --selector monitoring and re-run.

- **`assert_rfq_lifecycle_no_unclassified_submits`** (`tests\marts\assert_rfq_lifecycle_no_unclassified_submits.sql`)
  - Checks: Every quoting decision (submitted / priced_presubmit) gets a submit_bucket; none is 'unclassified'. Heavy.
  - Severity: error
  - A failure means: A new exit_reason / error_kind / submission shape upstream that the split has no rule for.
  - First checks: (1) Group the failing rows by record_schema, exit_reason, error_kind (the test returns that). (2) Find what the new value means in the bot's logging code / docs. (3) Add a rule to fct_rfq_lifecycle's submit_bucket (a design change: review it).

- **`assert_rfq_lifecycle_one_counted_decision_per_rfq`** (`tests\marts\assert_rfq_lifecycle_one_counted_decision_per_rfq.sql`)
  - Checks: Each doubled RFQ has exactly one counted decision, and it is the filled one when one filled. Heavy.
  - Severity: error
  - A failure means: The dedupe rule broke, so RFQ counts in the monitoring marts double-count or drop RFQs.
  - First checks: (1) List failing rfq_ids with both decisions (received_at, is_filled, quote_id). (2) Check the old-era attempt linkage (ASOF on received_at) for those RFQs. (3) Re-read fct_rfq_lifecycle's header (is_counted_decision).

- **`assert_rfq_lifecycle_rowcount_matches_outcomes`** (`tests\marts\assert_rfq_lifecycle_rowcount_matches_outcomes.sql`)
  - Checks: fct_rfq_lifecycle has exactly as many rows as stg_rfq_outcomes. Heavy.
  - Severity: error
  - A failure means: The lifecycle build dropped or fanned out decisions (a join on attempts / fills duplicated rows).
  - First checks: (1) Compare counts; find duplicate rfq_outcome_id (on big data group on hash(rfq_outcome_id)). (2) Check old_era_outcome is one row per rfq_outcome_id and fills_by_quote one per quote_id. (3) Check the external_parquet log line ('N rows -> location') of the last build.

#### mart_daily_pnl

- **`accepted_values_mart_daily_pnl_sport_group__baseball__basketball__hockey__football__soccer__tennis__multi_sport`**
  - Checks: `sport_group` in `mart_daily_pnl` takes only the listed values.
  - Severity: error
  - A failure means: A new value appeared: a new bot schema / exchange value / seed label. It must get a rule, never a silent bucket.
  - First checks: (1) `select sport_group, count(*) from mart_daily_pnl group by 1` to find the new value. (2) Trace where it comes from (source, seed, or a CASE in the model). (3) Add a deliberate rule or accepted value (and the docs) and record the decision.

- **`not_null_mart_daily_pnl_daily_pnl_id`**
  - Checks: `daily_pnl_id` is never NULL in `mart_daily_pnl`.
  - Severity: error
  - A failure means: Rows reached `mart_daily_pnl` without `daily_pnl_id`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from mart_daily_pnl where daily_pnl_id is null limit 20` (or `dbt test -s not_null_mart_daily_pnl_daily_pnl_id --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_mart_daily_pnl_n_legs`**
  - Checks: `n_legs` is never NULL in `mart_daily_pnl`.
  - Severity: error
  - A failure means: Rows reached `mart_daily_pnl` without `n_legs`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from mart_daily_pnl where n_legs is null limit 20` (or `dbt test -s not_null_mart_daily_pnl_n_legs --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_mart_daily_pnl_side`**
  - Checks: `side` is never NULL in `mart_daily_pnl`.
  - Severity: error
  - A failure means: Rows reached `mart_daily_pnl` without `side`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from mart_daily_pnl where side is null limit 20` (or `dbt test -s not_null_mart_daily_pnl_side --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_mart_daily_pnl_sport_group`**
  - Checks: `sport_group` is never NULL in `mart_daily_pnl`.
  - Severity: error
  - A failure means: Rows reached `mart_daily_pnl` without `sport_group`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from mart_daily_pnl where sport_group is null limit 20` (or `dbt test -s not_null_mart_daily_pnl_sport_group --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_mart_daily_pnl_trade_date_local`**
  - Checks: `trade_date_local` is never NULL in `mart_daily_pnl`.
  - Severity: error
  - A failure means: Rows reached `mart_daily_pnl` without `trade_date_local`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from mart_daily_pnl where trade_date_local is null limit 20` (or `dbt test -s not_null_mart_daily_pnl_trade_date_local --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`unique_mart_daily_pnl_daily_pnl_id`**
  - Checks: `daily_pnl_id` is unique in `mart_daily_pnl`.
  - Severity: error
  - A failure means: `mart_daily_pnl` has duplicate `daily_pnl_id` values: a fan-out join, a source re-delivery, or a grain change.
  - First checks: (1) Find the duplicated keys (group by daily_pnl_id having count(*) > 1; on very large tables group on hash(daily_pnl_id)). (2) Compare the duplicate rows' payloads: identical = true duplicate, different = a real repeat (e.g. the dual-instance window). (3) Check the model's joins for a many-to-one that became many-to-many.

- **`assert_daily_pnl_identity`** (`tests\marts\assert_daily_pnl_identity.sql`)
  - Checks: On every mart_daily_pnl row gross = net + fees outside cost + gross of unknown-fee fills; fills sum to fct_fills.
  - Severity: error
  - A failure means: A metric was redefined or a fill fell out of the grain.
  - First checks: (1) Find the failing rows and recompute them from fct_fills for that day / group. (2) Check fee_outside_cost / net_pnl definitions in int_fills_graded. (3) Check sport_group / n_legs nulls in fct_fills (dim_combos join).

#### mart_daily_trade_report

- **`accepted_values_mart_daily_trade_report_role__maker__taker`**
  - Checks: `role` in `mart_daily_trade_report` takes only the listed values.
  - Severity: error
  - A failure means: A new value appeared: a new bot schema / exchange value / seed label. It must get a rule, never a silent bucket.
  - First checks: (1) `select role, count(*) from mart_daily_trade_report group by 1` to find the new value. (2) Trace where it comes from (source, seed, or a CASE in the model). (3) Add a deliberate rule or accepted value (and the docs) and record the decision.

- **`unique_mart_daily_trade_report_trade_id`**
  - Checks: `trade_id` is unique in `mart_daily_trade_report`.
  - Severity: error
  - A failure means: `mart_daily_trade_report` has duplicate `trade_id` values: a fan-out join, a source re-delivery, or a grain change.
  - First checks: (1) Find the duplicated keys (group by trade_id having count(*) > 1; on very large tables group on hash(trade_id)). (2) Compare the duplicate rows' payloads: identical = true duplicate, different = a real repeat (e.g. the dual-instance window). (3) Check the model's joins for a many-to-one that became many-to-many.

- **`assert_daily_trade_report_rowcount_matches_fills`** (`tests\marts\assert_daily_trade_report_rowcount_matches_fills.sql`)
  - Checks: mart_daily_trade_report has exactly one row per fct_fills fill.
  - Severity: error
  - A failure means: The trade report lost or duplicated trades.
  - First checks: (1) Compare with fct_fills' count. (2) Check for duplicate trade_id. (3) Check the model contract (dbt build -s mart_daily_trade_report) for a type error.

- **`assert_trade_report_notional_is_quantity_x_price`** (`tests\marts\assert_trade_report_notional_is_quantity_x_price.sql`)
  - Checks: On every trade notional = quantity x price (to the cent) and notional + any folded fee = fill_cost.
  - Severity: error
  - A failure means: Price or notional derivation changed, or fill_cost / fee semantics changed upstream.
  - First checks: (1) Inspect failing trades' price_source, fill_cost, fee, is_fee_in_fill_cost. (2) Check int_fills_graded.is_fee_in_fill_cost and stg_fills.fill_price_dollars. (3) Re-read mart_daily_trade_report's header (price / notional rules).

#### mart_market_maker_monitoring

- **`accepted_values_mart_market_maker_monitoring_data_period__submission_log_undercounted__decision_log_tail_loss__clean`**
  - Checks: `data_period` in `mart_market_maker_monitoring` takes only the listed values.
  - Severity: error
  - A failure means: A new value appeared: a new bot schema / exchange value / seed label. It must get a rule, never a silent bucket.
  - First checks: (1) `select data_period, count(*) from mart_market_maker_monitoring group by 1` to find the new value. (2) Trace where it comes from (source, seed, or a CASE in the model). (3) Add a deliberate rule or accepted value (and the docs) and record the decision.

- **`not_null_mart_market_maker_monitoring_data_period`**
  - Checks: `data_period` is never NULL in `mart_market_maker_monitoring`.
  - Severity: error
  - A failure means: Rows reached `mart_market_maker_monitoring` without `data_period`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from mart_market_maker_monitoring where data_period is null limit 20` (or `dbt test -s not_null_mart_market_maker_monitoring_data_period --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_mart_market_maker_monitoring_date_local`**
  - Checks: `date_local` is never NULL in `mart_market_maker_monitoring`.
  - Severity: error
  - A failure means: Rows reached `mart_market_maker_monitoring` without `date_local`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from mart_market_maker_monitoring where date_local is null limit 20` (or `dbt test -s not_null_mart_market_maker_monitoring_date_local --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`unique_mart_market_maker_monitoring_date_local`**
  - Checks: `date_local` is unique in `mart_market_maker_monitoring`.
  - Severity: error
  - A failure means: `mart_market_maker_monitoring` has duplicate `date_local` values: a fan-out join, a source re-delivery, or a grain change.
  - First checks: (1) Find the duplicated keys (group by date_local having count(*) > 1; on very large tables group on hash(date_local)). (2) Compare the duplicate rows' payloads: identical = true duplicate, different = a real repeat (e.g. the dual-instance window). (3) Check the model's joins for a many-to-one that became many-to-many.

#### mart_requester_activity

- **`not_null_mart_requester_activity_requester_label`**
  - Checks: `requester_label` is never NULL in `mart_requester_activity`.
  - Severity: error
  - A failure means: Rows reached `mart_requester_activity` without `requester_label`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from mart_requester_activity where requester_label is null limit 20` (or `dbt test -s not_null_mart_requester_activity_requester_label --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_mart_requester_activity_requester_week_id`**
  - Checks: `requester_week_id` is never NULL in `mart_requester_activity`.
  - Severity: error
  - A failure means: Rows reached `mart_requester_activity` without `requester_week_id`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from mart_requester_activity where requester_week_id is null limit 20` (or `dbt test -s not_null_mart_requester_activity_requester_week_id --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_mart_requester_activity_week_start_local`**
  - Checks: `week_start_local` is never NULL in `mart_requester_activity`.
  - Severity: error
  - A failure means: Rows reached `mart_requester_activity` without `week_start_local`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from mart_requester_activity where week_start_local is null limit 20` (or `dbt test -s not_null_mart_requester_activity_week_start_local --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`unique_mart_requester_activity_requester_week_id`**
  - Checks: `requester_week_id` is unique in `mart_requester_activity`.
  - Severity: error
  - A failure means: `mart_requester_activity` has duplicate `requester_week_id` values: a fan-out join, a source re-delivery, or a grain change.
  - First checks: (1) Find the duplicated keys (group by requester_week_id having count(*) > 1; on very large tables group on hash(requester_week_id)). (2) Compare the duplicate rows' payloads: identical = true duplicate, different = a real repeat (e.g. the dual-instance window). (3) Check the model's joins for a many-to-one that became many-to-many.

- **`assert_requester_empty_creator_id_labelled`** (`tests\marts\assert_requester_empty_creator_id_labelled.sql`)
  - Checks: creator_id e3b0c442...b855 (var empty_creator_id_sha256) really is SHA-256 of the empty string; every mart_requester_activity row with it is labelled 'no_creator_id' and never flagged as a price checker; no other row has that label; and the label exists.
  - Severity: error
  - A failure means: The label or the price-checker exclusion broke, so the RFQs with no creator id would read as one huge requester (or vanish from the label).
  - First checks: (1) Read the `problem` column: bad label / flag, wrong var, or no labelled row. (2) Check the requester_label case and the is_heavy_price_checker guard in mart_requester_activity.sql. (3) If 'no row carries the label': check fct_rfq_lifecycle.creator_id still holds the hash.

### Seeds (5 tests)

#### ticker_prefixes

- **`not_null_ticker_prefixes_competition`**
  - Checks: `competition` is never NULL in `ticker_prefixes`.
  - Severity: error
  - A failure means: Rows reached `ticker_prefixes` without `competition`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from ticker_prefixes where competition is null limit 20` (or `dbt test -s not_null_ticker_prefixes_competition --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_ticker_prefixes_market_type`**
  - Checks: `market_type` is never NULL in `ticker_prefixes`.
  - Severity: error
  - A failure means: Rows reached `ticker_prefixes` without `market_type`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from ticker_prefixes where market_type is null limit 20` (or `dbt test -s not_null_ticker_prefixes_market_type --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_ticker_prefixes_sport`**
  - Checks: `sport` is never NULL in `ticker_prefixes`.
  - Severity: error
  - A failure means: Rows reached `ticker_prefixes` without `sport`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from ticker_prefixes where sport is null limit 20` (or `dbt test -s not_null_ticker_prefixes_sport --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`not_null_ticker_prefixes_ticker_prefix`**
  - Checks: `ticker_prefix` is never NULL in `ticker_prefixes`.
  - Severity: error
  - A failure means: Rows reached `ticker_prefixes` without `ticker_prefix`: the field went missing upstream, a parse/cast changed, or a join lost its match.
  - First checks: (1) `select * from ticker_prefixes where ticker_prefix is null limit 20` (or `dbt test -s not_null_ticker_prefixes_ticker_prefix --store-failures`). (2) Check the same column in the upstream model / source for the same rows (lineage: _source_key, _line_no). (3) `git log -p` the model and its parents for a recent change to that column.

- **`unique_ticker_prefixes_ticker_prefix`**
  - Checks: `ticker_prefix` is unique in `ticker_prefixes`.
  - Severity: error
  - A failure means: `ticker_prefixes` has duplicate `ticker_prefix` values: a fan-out join, a source re-delivery, or a grain change.
  - First checks: (1) Find the duplicated keys (group by ticker_prefix having count(*) > 1; on very large tables group on hash(ticker_prefix)). (2) Compare the duplicate rows' payloads: identical = true duplicate, different = a real repeat (e.g. the dual-instance window). (3) Check the model's joins for a many-to-one that became many-to-many.

### Snapshots (1 tests)

#### snap_market_results

- **`assert_snapshot_one_current_row_per_market`** (`tests\snapshots\assert_snapshot_one_current_row_per_market.sql`)
  - Checks: snap_market_results has exactly one current version per market and covers every staged market.
  - Severity: error
  - A failure means: The snapshot's unique key broke, or markets were added without a snapshot run.
  - First checks: (1) Run `dbt snapshot` and re-test. (2) List markets with 0 or 2+ current rows. (3) Check the snapshot config (unique_key, check_cols) in snapshots/snap_market_results.yml.

<!-- END GENERATED TEST INVENTORY -->
