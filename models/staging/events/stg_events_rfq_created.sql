-- One row per rfq_created WebSocket frame logged in the completeness window.
-- Pre-typed by the extract (no raw_json): staging renames and converts
-- timestamps. created_ts is the exchange's clock (ISO string); received_ts is
-- ours. The events stream is lossy under load, and the bot writes it, so it
-- is silent whenever the bot is off: diagnostic only, never a population or
-- rate source.

{{ config(materialized='view', tags=['heavy']) }}

with source as (

    select * from {{ source('events', 'events_rfq_created') }}
    where not _parse_error

)

select
    rfq_id,
    creator_id,
    combo_ticker,
    collection_ticker,
    n_legs,
    sid,
    seq,
    {{ iso_to_utc('created_ts') }}                          as rfq_created_at_utc,
    {{ epoch_to_utc('received_ts') }}                       as received_at_utc,
    {{ utc_to_local(epoch_to_utc('received_ts')) }}         as received_at_local,
    cast({{ utc_to_local(epoch_to_utc('received_ts')) }} as date) as received_date_local,
    _source_key,
    _source_origin,
    _line_no
from source
