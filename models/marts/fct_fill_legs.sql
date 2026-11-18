-- One row per fill leg: the leg, its market's attributes (dim_markets) and
-- its outcome (int_fill_legs_resolved), with the fill's time, side and combo
-- carried along so leg-level questions are one query:
--   select distinct fill_id from fct_fill_legs
--   where competition = 'Soccer Continental Cup'
--     and accepted_date_local between '2025-03-11' and '2025-03-19'

{{ config(materialized='table') }}

with legs as (

    select * from {{ ref('stg_fill_legs') }}

),

resolved as (

    select * from {{ ref('int_fill_legs_resolved') }}

),

markets as (

    select * from {{ ref('dim_markets') }}

),

fills as (

    select
        fill_id, combo_ticker, side, n_legs, fill_origin, accepted_at_utc,
        accepted_at_local, accepted_date_local, accepted_date_utc, parlay_result
    from {{ ref('fct_fills') }}

)

select
    l.fill_leg_id,
    l.fill_id,
    l.leg_index,
    -- the fill
    f.combo_ticker,
    f.side                                                  as fill_side,
    f.n_legs,
    f.fill_origin,
    f.parlay_result,
    f.accepted_at_utc,
    f.accepted_at_local,
    f.accepted_date_local,
    f.accepted_date_utc,
    -- the leg's market
    l.market_ticker,
    l.side                                                  as leg_side,
    m.ticker_prefix,
    m.sport,
    m.competition,
    m.market_type,
    m.event_ticker,
    m.game_key,
    m.ticker_event_date,
    m.matchup_code,
    m.selection,
    m.selection_subject,
    m.selection_number,
    m.title                                                 as market_title,
    m.occurrence_at_utc,
    -- outcome
    r.leg_outcome,
    r.leg_outcome_reason,
    r.leg_value,
    m.market_result,
    m.settlement_value_dollars,
    m.settled_at_utc,
    -- the leg's book at submit (bot log; null on poll-discovered fills)
    l.best_bid_at_submit_dollars,
    l.best_ask_at_submit_dollars,
    l.spread_at_submit_dollars,
    l.live_at_submit,
    -- lineage
    l._source_key,
    l._line_no
from legs as l
join fills as f using (fill_id)
join markets as m using (market_ticker)
join resolved as r using (fill_leg_id)
