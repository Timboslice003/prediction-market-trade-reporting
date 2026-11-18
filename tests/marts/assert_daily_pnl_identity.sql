-- Dagster: shown as an asset check on mart_daily_pnl (orchestration/, docs/ORCHESTRATION.md).
{{ config(meta={'dagster': {'ref': {'name': 'mart_daily_pnl'}}}) }}
-- On every mart_daily_pnl row, gross P&L = net P&L (fee-known fills)
-- + the fee not inside fill_cost + gross P&L of the unknown-fee fills,
-- and the row's fill count matches fct_fills in total.
select *
from {{ ref('mart_daily_pnl') }}
where abs(gross_pnl_dollars - (net_pnl_dollars + fees_outside_cost_dollars + gross_pnl_fee_unknown_dollars)) >= 0.000001
   or (select sum(n_fills) from {{ ref('mart_daily_pnl') }}) <> (select count(*) from {{ ref('fct_fills') }})
