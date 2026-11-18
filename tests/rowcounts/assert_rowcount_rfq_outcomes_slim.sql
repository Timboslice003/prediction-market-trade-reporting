-- Dagster: shown as an asset check on stg_rfq_outcomes_slim (orchestration/, docs/ORCHESTRATION.md).
{{ config(meta={'dagster': {'ref': {'name': 'stg_rfq_outcomes_slim'}}}) }}
-- rfq_outcomes_slim: staged rows + excluded rows == manifest rows_written, per source file.
{{ manifest_rowcount_reconciliation(
    table_name='rfq_outcomes_slim',
    staging_model=ref('stg_rfq_outcomes_slim'),
    source_relation=source('rfq', 'rfq_outcomes_slim')
) }}
