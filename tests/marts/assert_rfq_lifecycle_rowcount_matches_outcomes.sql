-- Dagster: shown as an asset check on fct_rfq_lifecycle (orchestration/, docs/ORCHESTRATION.md).
{{ config(meta={'dagster': {'ref': {'name': 'fct_rfq_lifecycle'}}}) }}
-- fct_rfq_lifecycle has exactly one row per stg_rfq_outcomes decision.
select
    (select count(*) from {{ ref('fct_rfq_lifecycle') }}) as lifecycle_rows,
    (select count(*) from {{ ref('stg_rfq_outcomes') }})  as outcome_rows
where (select count(*) from {{ ref('fct_rfq_lifecycle') }})
   <> (select count(*) from {{ ref('stg_rfq_outcomes') }})
