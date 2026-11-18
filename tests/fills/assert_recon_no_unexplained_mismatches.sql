-- Every fill where dbt and the legacy Python script disagree has an explanation.
-- Do not loosen: a new kind of difference must be investigated and given a
-- rule in recon_fills_python_vs_dbt, not filtered out here.
{{ config(severity='error') }}
select fill_id, dbt_parlay_result, python_parlay_result,
       revenue_diff_dollars, pnl_diff_dollars
from {{ ref('recon_fills_python_vs_dbt') }}
where mismatch_category = 'unexplained'
