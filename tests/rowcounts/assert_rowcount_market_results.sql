-- Dagster: shown as an asset check on stg_market_results (orchestration/, docs/ORCHESTRATION.md).
{{ config(meta={'dagster': {'ref': {'name': 'stg_market_results'}}}) }}
-- market_results: staged rows == manifest rows_written. One file, fetched
-- from the exchange's API with no lineage or _parse_error columns, so this
-- compares the table total.
{{ manifest_rowcount_reconciliation(
    table_name='market_results',
    staging_model=ref('stg_market_results'),
    source_relation=source('settlement', 'market_results'),
    has_parse_error_flag=false,
    per_file=false
) }}
