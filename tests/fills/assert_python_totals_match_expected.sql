-- Dagster: shown as an asset check on stg_python_graded (orchestration/, docs/ORCHESTRATION.md).
{{ config(meta={'dagster': {'ref': {'name': 'stg_python_graded'}}}) }}
-- The legacy Python script's totals equal the known figures, so the
-- reconciliation compares against the right baseline. Cent precision.
with t as (
    select
        round(sum(f.fill_cost_dollars), 2)      as cost,
        round(sum(p.revenue_dollars), 2)        as revenue,
        round(sum(p.realized_pnl_dollars), 2)   as gross_pnl,
        round(sum(f.fee_dollars), 2)            as fees
    from {{ ref('stg_python_graded') }} as p
    join {{ ref('stg_fills') }} as f using (fill_id)
)
select *
from t
where cost <> round({{ expected('python_total_cost') }}, 2)
   or revenue <> round({{ expected('python_total_revenue') }}, 2)
   or gross_pnl <> round({{ expected('python_total_realized_pnl') }}, 2)
   or fees <> round({{ expected('total_fees') }}, 2)
