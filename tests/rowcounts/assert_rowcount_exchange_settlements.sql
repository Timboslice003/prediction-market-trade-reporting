-- Dagster: shown as an asset check on stg_exchange_settlements (orchestration/, docs/ORCHESTRATION.md).
{{ config(meta={'dagster': {'ref': {'name': 'stg_exchange_settlements'}}}) }}
-- exchange_settlements: staged rows + excluded rows == manifest rows_written, per source file.
{{ manifest_rowcount_reconciliation(
    table_name='exchange_settlements',
    staging_model=ref('stg_exchange_settlements'),
    source_relation=source('settlement', 'exchange_settlements')
) }}
