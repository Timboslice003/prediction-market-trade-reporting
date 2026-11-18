-- Dagster: shown as an asset check on snap_market_results (orchestration/, docs/ORCHESTRATION.md).
{{ config(meta={'dagster': {'ref': {'name': 'snap_market_results'}}}) }}
-- snap_market_results keeps history, but each market has exactly one
-- current version (dbt_valid_to null), and every staged market is in it.
select market_ticker, count(*) filter (where dbt_valid_to is null) as n_current
from {{ ref('snap_market_results') }}
group by market_ticker
having count(*) filter (where dbt_valid_to is null) <> 1
union all
select market_ticker, 0
from {{ ref('stg_market_results') }}
where market_ticker not in (select market_ticker from {{ ref('snap_market_results') }})
