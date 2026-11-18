-- One row per fill leg (fill_id x leg_index), from the settled fill file.
-- Leg book prices are logged in cents and converted to dollars here.
-- Poll-discovered fills log only market_ticker and side for each leg.

with source as (

    select * from {{ source('fills', 'fills_settled') }}
    where not _parse_error

),

legs as (

    select
        fill_id,
        _source_key,
        _source_origin,
        _line_no,
        unnest(
            json_transform(raw_json -> '$.legs', '[{
                "market_ticker": "VARCHAR", "side": "VARCHAR", "sport": "VARCHAR",
                "market_type": "VARCHAR", "game_slug": "VARCHAR",
                "best_bid_at_submit": "DOUBLE", "best_ask_at_submit": "DOUBLE",
                "spread_cents_at_submit": "DOUBLE", "depth_touched_at_submit": "DOUBLE",
                "live_at_submit": "BOOLEAN"
            }]')
        ) as leg,
        unnest(range(1, cast(json_array_length(raw_json, '$.legs') as bigint) + 1)) as leg_index
    from source

)

select
    fill_id || ':' || cast(leg_index as varchar)            as fill_leg_id,
    fill_id,
    cast(leg_index as integer)                              as leg_index,
    leg.market_ticker,
    leg.side,
    leg.sport,
    leg.market_type,
    leg.game_slug,
    {{ cents_to_dollars('leg.best_bid_at_submit') }}        as best_bid_at_submit_dollars,
    {{ cents_to_dollars('leg.best_ask_at_submit') }}        as best_ask_at_submit_dollars,
    {{ cents_to_dollars('leg.spread_cents_at_submit') }}    as spread_at_submit_dollars,
    leg.depth_touched_at_submit                             as depth_touched_at_submit_contracts,
    leg.live_at_submit,
    _source_key,
    _source_origin,
    _line_no
from legs
