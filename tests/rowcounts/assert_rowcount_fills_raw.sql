-- Dagster: shown as an asset check on stg_fills_raw (orchestration/, docs/ORCHESTRATION.md).
{{ config(meta={'dagster': {'ref': {'name': 'stg_fills_raw'}}}) }}
-- fills_raw: staged rows + excluded rows == manifest rows_written, per source file.
{{ manifest_rowcount_reconciliation(
    table_name='fills_raw',
    staging_model=ref('stg_fills_raw'),
    source_relation=source('fills', 'fills_raw')
) }}
