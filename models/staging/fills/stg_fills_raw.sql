-- One row per line of the raw hourly fill log (fill_v2 / fill_v3).
-- NOT one row per fill: some trades are logged twice, once by the REST poll
-- (reverify_path poll_discovered) and once by the websocket channel
-- (reverify_confirmed), with different payloads. Both are real records, so
-- both are kept; the key is the source line (fill_record_id).
-- assert_fills_raw_channel_duplicates (warn) lists them.
-- fill_v2 (before the settled file begins) carries only the identity / size
-- subset. Legs and settlement fields are not staged here; stg_fill_legs /
-- stg_python_graded come from the settled file.

with source as (

    select * from {{ source('fills', 'fills_raw') }}
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
            "fill_price": "DOUBLE", "fill_cost": "DOUBLE", "fee": "DOUBLE",
            "cost_source": "VARCHAR", "accepted_ts": "DOUBLE", "received_ts": "DOUBLE",
            "source": "VARCHAR", "reverify_path": "VARCHAR"
        }') as j
    from source

),

renamed as (

    select
        _source_key || ':' || cast(_line_no as varchar)    as fill_record_id,
        j.fill_id,
        j.schema                                            as record_schema,
        j.order_id,
        nullif(j.quote_id, '')                              as quote_id,
        nullif(j.rfq_id, '')                                as rfq_id,
        j.requester_creator_id                              as creator_id,
        j.market_ticker                                     as combo_ticker,
        j.side,

        j.count                                             as contract_count,
        {{ to_money('j.max_payout') }}                      as max_payout_dollars,
        j.n_legs,
        j.is_same_game_parlay,
        {{ to_money('j.fill_price') }}                      as fill_price_dollars,
        {{ to_money('j.fill_cost') }}                       as fill_cost_dollars,
        {{ to_money('j.fee') }}                             as fee_dollars,
        j.cost_source,

        coalesce(
            {{ epoch_to_utc('j.accepted_ts') }},
            {{ epoch_to_utc('j.received_ts') }}
        )                                                   as accepted_at_utc,
        ({{ epoch_to_utc('j.accepted_ts') }}) is null       as is_accepted_at_from_received,
        {{ epoch_to_utc('j.received_ts') }}                 as received_at_utc,

        j.source                                            as fill_source,
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
