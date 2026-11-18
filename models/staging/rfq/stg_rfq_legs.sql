-- One row per RFQ leg (rfq_outcome_id x leg_index), for every staged RFQ outcome.
-- Built from stg_rfq_outcomes' typed legs list so it inherits the same row set
-- (parse-error exclusion included) without parsing the JSON a second time.
--
-- Leg shapes unified across eras:
--   old schemas:     metadata_sport / metadata_market_type / metadata_game_start_ts,
--                    event_ticker, metadata_status; no game_slug
--   rfq_outcome_v1:  sport / market_type / game_start_ts / game_slug /
--                    live_at_receipt
-- Book prices are logged in cents -> dollars here.
-- game_start_ts of 0 means "unknown" -> NULL.

{{ config(materialized='external_parquet', tags=['heavy']) }}

with outcomes as (

    select
        rfq_outcome_id,
        rfq_id,
        record_schema,
        received_date_local,
        legs,
        _source_key,
        _source_origin,
        _line_no
    from {{ ref('stg_rfq_outcomes') }}
    where len(legs) > 0

),

unnested as (

    select
        * exclude (legs),
        unnest(legs)                            as leg,
        unnest(range(1, len(legs) + 1))         as leg_index
    from outcomes

)

select
    rfq_outcome_id,
    rfq_id,
    cast(leg_index as integer)                                  as leg_index,
    record_schema,
    received_date_local,

    leg.market_ticker,
    leg.side,
    leg.event_ticker,                                           -- old schemas only (derivable from the ticker)
    coalesce(leg.sport, leg.metadata_sport)                     as sport,
    coalesce(leg.market_type, leg.metadata_market_type)         as market_type,
    leg.game_slug,                                              -- v1 only; dateless, collides across a series
    leg.metadata_status,                                        -- old schemas only

    {{ cents_to_dollars('leg.best_bid') }}                      as best_bid_dollars,
    {{ cents_to_dollars('leg.best_ask') }}                      as best_ask_dollars,
    leg.bid_depth                                               as bid_depth_contracts,
    leg.ask_depth                                               as ask_depth_contracts,
    {{ epoch_to_utc('leg.book_last_update_ts') }}               as book_last_update_at_utc,

    {{ epoch_to_utc('coalesce(leg.game_start_ts, leg.metadata_game_start_ts)') }} as game_start_at_utc,
    leg.live_at_receipt,                                        -- v1 only

    _source_key,
    _source_origin,
    _line_no
from unnested
