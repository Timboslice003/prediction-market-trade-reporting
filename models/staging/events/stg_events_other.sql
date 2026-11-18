-- One row per non-rfq_created WebSocket frame logged in the completeness
-- window: quote_created / quote_accepted / quote_executed for the bot's own
-- quotes. The events stream is lossy under load: diagnostic only, never a
-- population or rate source.
-- The WS payload is a JSON string inside the envelope's `raw` field, so it is
-- parsed twice. The exchange's prices / sizes arrive as strings ("0.6870") and
-- are cast here; created_ts / executed_ts are the exchange's clock (ISO).

with source as (

    select * from {{ source('events', 'events_other') }}
    where not _parse_error

),

parsed as (

    select
        _source_key,
        _source_origin,
        _line_no,
        received_ts,
        json_transform(raw_json ->> '$.raw', '{
            "type": "VARCHAR", "sid": "BIGINT", "seq": "BIGINT",
            "msg": {
                "quote_id": "VARCHAR", "rfq_id": "VARCHAR",
                "quote_creator_id": "VARCHAR", "rfq_creator_id": "VARCHAR",
                "market_ticker": "VARCHAR", "event_ticker": "VARCHAR",
                "yes_bid_dollars": "VARCHAR", "no_bid_dollars": "VARCHAR",
                "yes_contracts_offered_fp": "VARCHAR", "no_contracts_offered_fp": "VARCHAR",
                "accepted_side": "VARCHAR", "contracts_accepted_fp": "VARCHAR",
                "order_id": "VARCHAR", "client_order_id": "VARCHAR",
                "created_ts": "VARCHAR", "executed_ts": "VARCHAR"
            }
        }') as f
    from source

)

select
    _source_key || ':' || cast(_line_no as varchar)         as event_id,
    f.type                                                  as frame_type,
    f.sid,
    f.seq,
    f.msg.quote_id,
    f.msg.rfq_id,
    f.msg.quote_creator_id,
    f.msg.rfq_creator_id                                    as creator_id,
    f.msg.market_ticker                                     as combo_ticker,
    f.msg.event_ticker                                      as combo_event_ticker,
    cast(f.msg.yes_bid_dollars as decimal(18, 6))           as yes_bid_dollars,
    cast(f.msg.no_bid_dollars as decimal(18, 6))            as no_bid_dollars,
    cast(f.msg.yes_contracts_offered_fp as double)          as yes_contracts_offered,
    cast(f.msg.no_contracts_offered_fp as double)           as no_contracts_offered,
    f.msg.accepted_side,
    cast(f.msg.contracts_accepted_fp as double)             as contracts_accepted,
    f.msg.order_id,
    f.msg.client_order_id,
    {{ iso_to_utc('f.msg.created_ts') }}                    as created_at_utc,
    {{ iso_to_utc('f.msg.executed_ts') }}                   as executed_at_utc,
    {{ epoch_to_utc('received_ts') }}                       as received_at_utc,
    {{ utc_to_local(epoch_to_utc('received_ts')) }}         as received_at_local,
    _source_key,
    _source_origin,
    _line_no
from parsed
