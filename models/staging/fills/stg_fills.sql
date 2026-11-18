-- One row per fill (fill_id = the exchange's trade id), from the settled fill
-- file. FILL fields only: the legacy Python settlement script's answer
-- (parlay_result, revenue, realized_pnl, legs_settled) is in stg_python_graded;
-- legs are in stg_fill_legs.

with source as (

    select * from {{ source('fills', 'fills_settled') }}
    where not _parse_error

),

parsed as (

    select
        _source_key,
        _source_origin,
        _line_no,
        json_transform(raw_json, '{
            "schema": "VARCHAR", "fill_id": "VARCHAR", "quote_id": "VARCHAR",
            "rfq_id": "VARCHAR", "requester_creator_id": "VARCHAR", "order_id": "VARCHAR",
            "market_ticker": "VARCHAR", "side": "VARCHAR", "count": "DOUBLE",
            "max_payout": "DOUBLE", "n_legs": "INTEGER", "is_same_game_parlay": "BOOLEAN",
            "is_straight": "BOOLEAN", "yes_bid": "DOUBLE", "no_bid": "DOUBLE",
            "submit_price": "DOUBLE", "submit_cost_estimate": "DOUBLE",
            "fill_price": "DOUBLE", "fill_cost": "DOUBLE", "fee": "DOUBLE",
            "cost_source": "VARCHAR", "rfq_created_ts": "DOUBLE", "submitted_ts": "DOUBLE",
            "accepted_ts": "DOUBLE", "executed_ts": "DOUBLE", "received_ts": "DOUBLE",
            "submit_to_accept_sec": "DOUBLE", "source": "VARCHAR", "reverify_path": "VARCHAR"
        }') as j
    from source

),

renamed as (

    select
        -- identity
        j.fill_id,
        j.order_id,
        nullif(j.quote_id, '')                          as quote_id,  -- poll-discovered fills log an empty string
        nullif(j.rfq_id, '')                            as rfq_id,
        j.requester_creator_id                          as creator_id,
        j.market_ticker                                 as combo_ticker,
        j.side,
        j.schema                                        as record_schema,

        -- shape
        j.count                                         as contract_count,
        {{ to_money('j.max_payout') }}                  as max_payout_dollars,
        j.n_legs,
        j.is_same_game_parlay,
        j.is_straight,

        -- the bot's quote (dollars per contract)
        j.yes_bid                                       as yes_bid_dollars,
        j.no_bid                                        as no_bid_dollars,
        j.submit_price                                  as submit_price_dollars,
        {{ to_money('j.submit_cost_estimate') }}        as submit_cost_estimate_dollars,

        -- execution (exchange ledger values)
        {{ to_money('j.fill_price') }}                  as fill_price_dollars,
        {{ to_money('j.fill_cost') }}                   as fill_cost_dollars,
        {{ to_money('j.fee') }}                         as fee_dollars,
        j.cost_source,

        -- timestamps (UTC). accepted_ts is missing on a few fills: fall back
        -- to received_ts and flag it.
        {{ epoch_to_utc('j.rfq_created_ts') }}          as rfq_created_at_utc,
        {{ epoch_to_utc('j.submitted_ts') }}            as submitted_at_utc,
        coalesce(
            {{ epoch_to_utc('j.accepted_ts') }},
            {{ epoch_to_utc('j.received_ts') }}
        )                                               as accepted_at_utc,
        ({{ epoch_to_utc('j.accepted_ts') }}) is null   as is_accepted_at_from_received,
        {{ epoch_to_utc('j.executed_ts') }}             as executed_at_utc,
        {{ epoch_to_utc('j.received_ts') }}             as received_at_utc,
        j.submit_to_accept_sec,

        -- provenance (fill_source is the channel that logged it, not how it was booked)
        j.source                                        as fill_source,
        j.reverify_path,

        _source_key,
        _source_origin,
        _line_no

    from parsed

)

select
    *,
    {{ utc_to_local('accepted_at_utc') }}                   as accepted_at_local,
    cast({{ utc_to_local('accepted_at_utc') }} as date)     as accepted_date_local
from renamed
