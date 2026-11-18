-- Dagster: shown as an asset check on stg_fills (orchestration/, docs/ORCHESTRATION.md).
{{ config(meta={'dagster': {'ref': {'name': 'stg_fills'}}}) }}
-- fills_settled: staged rows + excluded rows == manifest rows_written, per source file.
{{ manifest_rowcount_reconciliation(
    table_name='fills_settled',
    staging_model=ref('stg_fills'),
    source_relation=source('fills', 'fills_settled')
) }}
