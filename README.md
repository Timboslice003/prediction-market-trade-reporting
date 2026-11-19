# Prediction-market trade reporting on dbt + DuckDB

[![build](https://github.com/Timboslice003/prediction-market-trade-reporting/actions/workflows/build.yml/badge.svg)](https://github.com/Timboslice003/prediction-market-trade-reporting/actions/workflows/build.yml)

## Start here

A regulatory-reporting pipeline that turns a trading bot's messy logs into
graded fills, P&L that reconciles to the cent, and a contract-enforced daily
trade report, with a test for every way the logs break. Browse the
[dbt docs site](https://timboslice003.github.io/prediction-market-trade-reporting/)
and the [Elementary data-quality report](https://timboslice003.github.io/prediction-market-trade-reporting/elementary/);
then read the [loss-budget bridge model](models/reconciliation/recon_loss_budget_bridge.sql),
the [planted-breaks table](#planted-breaks-and-what-catches-each), the
[FIXML mapping](docs/FIXML_MAPPING.md), and a
[sample FIXML trade capture report](docs/examples/sample_trade_capture_report.xml)
([what each trade in it shows](docs/examples/README.md)).

## Overview

A regulatory-style reporting pipeline for an automated market maker that
answers RFQs (requests for quote) on multi-leg parlays at **an RFQ-based
prediction market exchange**. It takes the bot's raw logs and turns them into
graded fills, P&L, a reconciled loss-budget bridge, a daily trade report with
an enforced contract, FIX trade-capture samples, and market-maker / requester
surveillance marts, with tests built to catch the ways those logs actually
break.

> **About the data.** This project was built on a private, real dataset (the
> logs of a market-making bot that traded real money). **This repository runs
> on synthetic data only.** `scripts/generate_synthetic.py` invents every row:
> prices, sizes, fees, teams, ids and volumes are drawn from simple made-up
> distributions, not from the real data. No real rows, ids, totals or findings
> are included.
>
> **How it was built.** The project was built with AI coding agents (Claude),
> working under Timmy Hernandez's design, decisions and review.

## Run it

Python 3.13. From the repo root:

```bash
pip install -r requirements.txt
python scripts/generate_synthetic.py      # writes data/synthetic (about 30 s)
dbt deps && dbt build                     # every model and test (about a minute)
```

That is the whole core. `dbt build` should end with `PASS=301 WARN=4
ERROR=0`: the four warnings are planted breaks reporting themselves (see
below). Everything it writes goes to `work/` (git-ignored); every path can be
overridden by an environment variable (`DBT_DATA_ROOT`, `PM_DUCKDB_PATH`,
`DBT_STAGING_PARQUET_ROOT`).

The full pipeline, as CI runs it (FIXML samples, Dagster, docs site,
Elementary report; the Elementary CLI lives in its own venv):

```bash
python -m venv .venv-edr && .venv-edr/bin/pip install -r requirements-edr.txt
python scripts/run_all.py                 # about 4 minutes; timings in work/timings.json
```

On Windows the second venv's pip is `.venv-edr\Scripts\pip install -r requirements-edr.txt`.

CI runs `python scripts/run_all.py --no-fixml-schema`: it renders the FIXML
samples and round-trips them against the mart, but skips validation against
the FIX schema, a third-party download this repo does not redistribute.
Schema validation is a local step: plain `run_all.py` (above) fetches the
schema once and validates; see [docs/FIXML_MAPPING.md](docs/FIXML_MAPPING.md#schema-and-validation).

| step | seconds (local run) |
|---|---:|
| generate synthetic data | 28 |
| dbt deps | 9 |
| dbt build (305 nodes) | 59 |
| FIXML schema fetch (local only) + render + validate | 4 |
| Dagster: validate definitions + routine job | 72 |
| dbt docs generate | 8 |
| Elementary report | 23–61 |

## Lineage at a glance

```
bot logs (synthetic)  ─┐
exchange market API ───┤  data/synthetic: one Parquet file per source log file + manifest
taker-script ledgers ──┘
        │
   staging (15)        rename, cast, cents→dollars, UTC + reporting clock, schema-era
        │              unification, parse-error exclusion; every row keeps _source_key/_line_no
        │
   intermediate        int_fill_legs_resolved → int_fills_graded   (dbt's own grading)
        │              int_fills_origin → int_fill_positions       (bot vs taker script)
        │              int_exchange_settlements_attributed         (every settlement row → one source)
        │              int_loss_budget_ledger                      (replays and restarts, by rule)
        │
   reconciliation      recon_fills_python_vs_dbt      dbt vs the legacy Python script, every diff categorized
        │              recon_loss_budget_positions → recon_loss_budget_bridge   (waterfall that ties)
        │              recon_rfq_completeness_hourly  exchange RFQs vs the bot's decision log
        │
   marts               dim_markets, dim_combos, fct_fills, fct_fill_legs, mart_daily_pnl,
        │              mart_daily_trade_report (contract-enforced), fct_rfq_lifecycle,
        │              mart_market_maker_monitoring, mart_requester_activity
        │
   outputs             FIXML TrdCaptRpt samples (schema-validated, round-tripped) · dbt docs ·
                       Elementary report · Dagster asset graph with every test as an asset check
```

## What each layer demonstrates

- **Sources and staging** (`models/staging/`). Parquet read through
  `env_var` paths, per-file row-count reconciliation against the extract
  manifest (`assert_rowcount_*`), two decision-log schema eras unified into
  one shape, `_parse_error` rows excluded but still counted, money as exact
  decimals, cents converted, a fixed-offset reporting clock. Staging never
  joins, grades or drops real rows. The three largest models use a custom
  materialization (`macros/external_parquet.sql` + a dbt-duckdb plugin) that
  streams them to Parquet outside the DuckDB file: on the original data they
  were large, and DuckDB never shrinks its file.
- **Grading** (`int_fill_legs_resolved`, `int_fills_graded`). dbt grades
  every fill on its own, from fill legs and the exchange's market results,
  never from the legacy script's answer. Scalar (fractional) settlements are
  graded natively; payouts are floored to the cent as the exchange pays them;
  a fee folded into cost is never subtracted twice; an unknown fee makes net
  P&L NULL, never zero.
- **Reconciliation** (`models/reconciliation/`). The legacy Python script's
  P&L next to dbt's, every difference in a named, rule-based category, and an
  error test that fails on any `unexplained` one. A loss-budget bridge from
  the bot's gross P&L to its ledger, position by position, that ties to the
  cent with the remainder reported, never forced. An hourly completeness
  check that tells logging loss from bot downtime by where the gap sits in
  the hour.
- **Marts** (`models/marts/`). Market and combo dimensions parsed from
  tickers and classified only from a seed; a fill fact and a fill-leg fact
  for one-query questions (`fills_for` macro); daily P&L with an identity
  test; a daily trade report modeled on an exchange's report with an
  enforced contract; the RFQ lifecycle (every quoting decision in exactly one
  submit bucket); market-maker monitoring with alert thresholds as vars; and
  requester surveillance with a price-checker flag.
- **Tests** (237). Generic tests on every key and enum, 42 singular tests
  for the business rules, warn tests that store their rows for review, a
  test that every mart column is documented, and a runbook entry for every
  test (`RUNBOOK.md`; `scripts/runbook_tests.py check` fails on any gap).
  On real data, tests pinned hand-verified figures; here they compare
  against the generator's ground truth (`_manifest/expectations.parquet`,
  read only by tests through the `expected()` macro, never by a model).
- **FIXML** (`scripts/render_fixml.py`, `scripts/check_fixml.py`,
  `docs/FIXML_MAPPING.md`). Sample FIX TradeCaptureReport batches, validated
  against the FIX Trading Community's schema (fetched and checksum-verified
  by `scripts/fetch_fixml_schema.py`, not redistributed; a local step, not
  in CI) and round-tripped field by field against the mart (locally and in CI).
- **Orchestration** (`orchestration/`, `docs/ORCHESTRATION.md`). Dagster:
  every dbt model an asset, every dbt test an asset check, the FIXML render
  an asset with its validation as a check, jobs that match the dbt
  selectors, retries, and a run-failure alert sensor. Includes the mapping to
  Airflow.
- **Observability.** Elementary (dbt package + `edr` report) and the dbt
  docs site, both published to GitHub Pages by CI
  (`.github/workflows/build.yml`).

## Planted breaks, and what catches each

The generator plants every break below on purpose. Error tests pass (each
break is detected and categorized, not a crash); warn tests and
reconciliation categories show them. Counts are for the default seed.

| # | Planted break | What catches it | What you see |
|---|---|---|---|
| 1 | **Duplicate fills over two channels**: 12 trades logged by the REST poll and again by the websocket | `assert_fills_raw_channel_duplicates` (warn, stored rows); `assert_fills_raw_repeats_are_channel_duplicates` (error: each is a clean two-channel pair); settlement attribution counts raw-log fills once | WARN 12 |
| 2 | **Scalar settlements, incl. NO-side fills**: leg markets voided at a fractional value | `int_fills_graded` grades them natively; `recon_fills_python_vs_dbt` categorizes every disagreement with the legacy script: `python_scalar_patch_ignored_fill_side` (NO side, 3), `python_scalar_patch_assumed_half` (2), `python_books_exact_not_paid_cent` (4), `python_pnl_rounded_4dp` (1); `assert_recon_no_unexplained_mismatches` (error) | 10 categorized fills, 0 unexplained |
| 3 | **Fee folded into cost, and unknown fees**: settle-time-enriched fills with fill_cost = count × price + fee (211); estimate-cost fills with no fee (113) | `int_fills_graded.is_fee_in_fill_cost` / `net_pnl` NULL when the fee is unknown; `assert_daily_pnl_identity`, `assert_trade_report_notional_is_quantity_x_price` (error); bridge items 9–11 | net P&L never double-counts a fee or treats unknown as 0 |
| 4 | **Settlements dropped by a pagination bug**: the reconciler reads one page per poll and skips the rest | bridge item "never booked" (`recon_loss_budget_bridge`); `assert_bot_positions_never_reconciled` (warn, stored rows) | WARN 94 positions |
| 5 | **Dual-instance window**: two bot processes decide the same RFQs for 30 minutes (8 RFQs decided twice; the filled quote belongs to the later decision) and re-book 3 ledger entries | `assert_rfq_outcomes_repeats_only_in_dual_instance_window`, `assert_rfq_lifecycle_one_counted_decision_per_rfq` (error); `int_loss_budget_ledger` detects the replay by rule | each RFQ counted once, the filled decision kept |
| 6 | **Decision rows lost at the end of the hour** (a logging bug), plus a pricing-loop outage | `recon_rfq_completeness_hourly.likely_cause`; `assert_rfq_completeness_gaps_for_review` (warn, stored rows) | WARN 30: 27 tail-loss hours, 1 bot_stopped, 1 bot_off, 1 bot_off_partial |
| 7 | **Missing creator id**: the exchange sends SHA-256 of an empty string | `mart_requester_activity.requester_label = 'no_creator_id'`, never a price checker; `assert_requester_empty_creator_id_labelled` (error) | 488 RFQs labelled, not one giant requester |
| 8 | **A same-game combo the bot's flag missed**: a leg logged with no game slug | `dim_combos.is_same_game` (from tickers) vs the bot's flag; `assert_same_game_flag_agrees` (warn, stored rows) | WARN 2 |
| 9 | **Manual trades and opposite-side hedges** in the account's settlement feed | `int_exchange_settlements_attributed`: `manual_single_market`, `manual_combo`, `bot_plus_other_activity` (hedges and extra contracts); `assert_exchange_settlements_unexplained` stays empty | every settlement row has exactly one source |

Also planted, and caught: taker-script fills hidden in the bot's fill log
and a bot fill that looks like a self-trade (`int_fills_origin`,
`assert_taker_fills_identified`); the ledger restarting at $0 and a lost
ledger write (`int_loss_budget_ledger`, bridge items 3–4); an archive copy of
the submission log missing rows (`no_submission_record` bucket and
`data_period` flags in `mart_market_maker_monitoring`); a rate-limit burst, a
latency spike and a price-check flood (monitoring alert flags on exactly
those days); truncated log lines (`_parse_error`, counted by the row-count
tests).

**The loss-budget bridge ties** (`assert_loss_budget_bridge_ties`, error)
from the bot's gross P&L to the ledger's cumulative through 16 named items,
with a **+$0.37 remainder on one position**: an estimate-cost fill booked
while the settlement log was missing, so the exchange's cost could not be
checked. It is reported, not forced
(`recon_loss_budget_positions where item_residual_dollars <> 0`).

## Layout

```
scripts/generate_synthetic.py     the synthetic extract (deterministic from --seed)
models/staging|intermediate|reconciliation|marts
tests/                            singular tests (generic ones live in the model YAML)
macros/                           external_parquet materialization, manifest reconciliation, fills_for, expected()
seeds/ticker_prefixes.csv         series prefix -> sport / competition / market type
snapshots/                        type-2 history of market results
orchestration/                    Dagster code location
scripts/render_fixml.py, check_fixml.py, fetch_fixml_schema.py
scripts/runbook_tests.py          RUNBOOK.md inventory generator + coverage check
scripts/leak_scan.py              the scan run before publishing (see below)
scripts/run_all.py                end-to-end runner with timings (CI uses it)
docs/                             FIXML_MAPPING.md, ORCHESTRATION.md
RUNBOOK.md                        on-call process + every test
```

## Leak scan

`scripts/leak_scan.py` scans every publishable file, the git history's
metadata, the generated data (Parquet decoded, not as bytes), the docs site,
the Elementary report, the FIXML samples, and every text column of the built
database. It applies generic patterns (personal paths, e-mail addresses,
bucket URIs, key-like strings) and, on the owner's machine, a private term
list kept outside the repo (the original exchange's name and ticker
prefixes, private names, the pricing model's column names, real figures, and
a sample of real ids drawn from the private database). CI runs the generic
part on every push.

## Ticker format (invented)

`{SERIES}.{YYYYMMDD}[T{HHMM}].{MATCHUP}[.{SELECTION}]`, e.g.
`BSBML.20250309T1310.HAWOWL.HAW` (moneyline) or
`BSBHR.20250309T1310.HAWOWL.HAWMORENO12_1` (a player prop); combos are
`CMB.<12 hex>`. Teams, players, leagues and series are fictional.
