-- Dagster: shown as an asset check on stg_loss_budget (orchestration/, docs/ORCHESTRATION.md).
{{ config(meta={'dagster': {'ref': {'name': 'stg_loss_budget'}}}) }}
-- loss_budget: staged rows + excluded rows == manifest rows_written, per source file.
{{ manifest_rowcount_reconciliation(
    table_name='loss_budget',
    staging_model=ref('stg_loss_budget'),
    source_relation=source('settlement', 'loss_budget')
) }}
