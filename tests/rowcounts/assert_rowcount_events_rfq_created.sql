-- Dagster: shown as an asset check on stg_events_rfq_created (orchestration/, docs/ORCHESTRATION.md).
{{ config(meta={'dagster': {'ref': {'name': 'stg_events_rfq_created'}}}) }}
-- events_rfq_created: staged rows + excluded rows == manifest rows_written, per source file.
{{ manifest_rowcount_reconciliation(
    table_name='events_rfq_created',
    staging_model=ref('stg_events_rfq_created'),
    source_relation=source('events', 'events_rfq_created')
) }}
