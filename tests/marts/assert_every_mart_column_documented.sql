-- Dagster: shown as an asset check on mart_daily_trade_report (orchestration/, docs/ORCHESTRATION.md).
{{ config(meta={'dagster': {'ref': {'name': 'mart_daily_trade_report'}}}) }}
-- Every column of every (non-heavy) mart has a YAML description: it feeds the
-- docs site. The heavy marts are checked by assert_every_heavy_mart_column_documented.
-- depends_on: {{ ref('dim_markets') }} {{ ref('dim_combos') }} {{ ref('fct_fills') }}
-- depends_on: {{ ref('fct_fill_legs') }} {{ ref('mart_daily_pnl') }} {{ ref('mart_daily_trade_report') }}
{{ undocumented_mart_columns(heavy=false) }}
