-- Dagster: shown as an asset check on mart_daily_pnl (orchestration/, docs/ORCHESTRATION.md).
{{ config(meta={'dagster': {'ref': {'name': 'mart_daily_pnl'}}}) }}
-- fct_fills and mart_daily_pnl both carry exactly the graded totals: gross
-- P&L (payouts floored to the cent), cost, fees, and net P&L over fee-known
-- fills. The expected figures are computed by the generator from its own
-- ground truth, independently of dbt (on the original data they were the
-- reviewed figures of the grading work).
with t as (
    select 'fct_fills' as model,
           round(sum(gross_pnl_dollars), 2) as gross, round(sum(fill_cost_dollars), 2) as cost,
           round(sum(fee_dollars), 2) as fees, round(sum(net_pnl_dollars), 2) as net
    from {{ ref('fct_fills') }}
    union all
    select 'mart_daily_pnl',
           round(sum(gross_pnl_dollars), 2), round(sum(fill_cost_dollars), 2),
           round(sum(fees_known_dollars), 2), round(sum(net_pnl_dollars), 2)
    from {{ ref('mart_daily_pnl') }}
)
select * from t
where gross <> round({{ expected('graded_gross_pnl') }}, 2)
   or cost <> round({{ expected('python_total_cost') }}, 2)
   or fees <> round({{ expected('total_fees') }}, 2)
   or net <> round({{ expected('graded_net_pnl_fee_known') }}, 2)
