-- Dagster: shown as an asset check on stg_events_other (orchestration/, docs/ORCHESTRATION.md).
{{ config(meta={'dagster': {'ref': {'name': 'stg_events_other'}}}) }}
-- events_other: staged rows + excluded rows == manifest rows_written, per source file.
{{ manifest_rowcount_reconciliation(
    table_name='events_other',
    staging_model=ref('stg_events_other'),
    source_relation=source('events', 'events_other')
) }}
