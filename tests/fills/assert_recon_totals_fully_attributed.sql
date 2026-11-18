-- Every cent of the dbt-minus-legacy-script total difference sits on a categorized
-- mismatch row: the sum of differences over fills that "agree" (diffs below
-- $0.000001 each) stays under half a cent in total. Cost and fees use the
-- same fields on both sides, so they cannot differ.
with t as (
    select
        sum(revenue_diff_dollars)                                   as revenue_diff_total,
        sum(revenue_diff_dollars) filter (where is_mismatch)        as revenue_diff_categorized,
        sum(pnl_diff_dollars)                                       as pnl_diff_total,
        sum(pnl_diff_dollars) filter (where is_mismatch)            as pnl_diff_categorized
    from {{ ref('recon_fills_python_vs_dbt') }}
)
select *
from t
where abs(revenue_diff_total - coalesce(revenue_diff_categorized, 0)) >= 0.005
   or abs(pnl_diff_total - coalesce(pnl_diff_categorized, 0)) >= 0.005
