-- Dagster: shown as an asset check on stg_rfq_outcomes (orchestration/, docs/ORCHESTRATION.md).
{{ config(meta={'dagster': {'ref': {'name': 'stg_rfq_outcomes'}}}) }}
-- rfq_outcomes: staged rows + excluded rows == manifest rows_written, per source file.
{{ manifest_rowcount_reconciliation(
    table_name='rfq_outcomes',
    staging_model=ref('stg_rfq_outcomes'),
    source_relation=source('rfq', 'rfq_outcomes')
) }}
