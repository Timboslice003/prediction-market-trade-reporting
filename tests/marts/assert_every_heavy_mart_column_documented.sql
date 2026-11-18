-- Dagster: shown as an asset check on mart_market_maker_monitoring (orchestration/, docs/ORCHESTRATION.md).
{{ config(meta={'dagster': {'ref': {'name': 'mart_market_maker_monitoring'}}}) }}
-- Every column of every heavy mart (fct_rfq_lifecycle and the monitoring
-- marts) has a YAML description. Runs with the heavy / monitoring selectors.
-- depends_on: {{ ref('fct_rfq_lifecycle') }} {{ ref('mart_market_maker_monitoring') }} {{ ref('mart_requester_activity') }}
{{ undocumented_mart_columns(heavy=true) }}
