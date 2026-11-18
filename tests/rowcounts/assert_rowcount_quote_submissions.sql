-- Dagster: shown as an asset check on stg_quote_submissions (orchestration/, docs/ORCHESTRATION.md).
{{ config(meta={'dagster': {'ref': {'name': 'stg_quote_submissions'}}}) }}
-- quote_submissions: staged rows + excluded rows == manifest rows_written, per source file.
{{ manifest_rowcount_reconciliation(
    table_name='quote_submissions',
    staging_model=ref('stg_quote_submissions'),
    source_relation=source('quotes', 'quote_submissions')
) }}
