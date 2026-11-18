-- One row per RFQ decision, FILTERED decisions included, for the completeness
-- window only (all rfq_outcome_v1). The extract wrote this table pre-typed with
-- no raw_json, so staging only renames and converts timestamps. This is the
-- full-funnel denominator for that window; the full rfq_outcomes extract
-- skipped filtered decisions.

{{ config(materialized='view', tags=['heavy']) }}

with source as (

    select * from {{ source('rfq', 'rfq_outcomes_slim') }}
    where not _parse_error

)

select
    rfq_id,
    schema                                                  as record_schema,
    terminal_stage                                          as stage,
    exit_reason,
    creator_id,
    combo_ticker,
    collection_ticker,
    n_legs,
    n_requested_legs,
    {{ epoch_to_utc('received_ts') }}                       as received_at_utc,
    {{ utc_to_local(epoch_to_utc('received_ts')) }}         as received_at_local,
    cast({{ utc_to_local(epoch_to_utc('received_ts')) }} as date) as received_date_local,
    _source_key,
    _source_origin,
    _line_no
from source
