-- One row per combo (parlay) ticker the settled fill file traded. Legs come
-- from stg_fill_legs; every fill of a combo carries the same legs, so the legs
-- are read from one fill per combo. Combos that only appear in the exchange's
-- settlement feed (manual trades) have no leg list in the data and are not here.
--
-- Same-game: legs share a game when they share dim_markets.game_key
-- (competition + the ticker's date/time + matchup segment), e.g.
-- BSBML.20250309T1310.HAWOWL.HAW and BSBTOT.20250309T1310.HAWOWL.8.
-- is_same_game = at least two legs on one game (the bot's
-- is_same_game_parlay meaning). It is computed from tickers, so it does not
-- depend on the bot having logged a game slug for every leg; where the bot's
-- own flag disagrees, assert_same_game_flag_agrees (warn) lists the combo.
-- is_single_game = every leg on one game.

{{ config(materialized='table') }}

with one_fill as (

    select combo_ticker, min(fill_id) as fill_id
    from {{ ref('stg_fills') }}
    group by combo_ticker

),

legs as (

    select
        o.combo_ticker,
        l.leg_index,
        l.market_ticker,
        l.side                                              as leg_side,
        m.sport,
        m.competition,
        m.market_type,
        m.game_key,
        m.is_prefix_classified
    from one_fill as o
    join {{ ref('stg_fill_legs') }} as l using (fill_id)
    join {{ ref('dim_markets') }} as m using (market_ticker)

),

games as (

    select combo_ticker, game_key, count(*) as n_legs_on_game
    from legs
    group by combo_ticker, game_key

),

game_rollup as (

    select
        combo_ticker,
        count(*)                                            as n_games,
        max(n_legs_on_game)                                 as max_legs_on_one_game
    from games
    group by combo_ticker

),

leg_rollup as (

    select
        combo_ticker,
        count(*)                                            as n_legs,
        list(market_ticker order by leg_index)              as leg_market_tickers,
        list(leg_side order by leg_index)                   as leg_sides,
        list(distinct sport order by sport)                 as sports,
        list(distinct competition order by competition)     as competitions,
        list(distinct market_type order by market_type)     as market_types,
        count(distinct sport)                               as n_sports,
        count(distinct competition)                         as n_competitions,
        bool_and(is_prefix_classified)                      as are_all_legs_classified,
        min(sport)                                          as only_sport
    from legs
    group by combo_ticker

),

fill_rollup as (

    select
        combo_ticker,
        count(*)                                            as n_fills,
        count(distinct side)                                as n_sides_filled,
        bool_or(is_same_game_parlay)                        as bot_is_same_game_parlay,
        min(accepted_at_utc)                                as first_fill_at_utc,
        max(accepted_at_utc)                                as last_fill_at_utc
    from {{ ref('stg_fills') }}
    group by combo_ticker

)

select
    l.combo_ticker,
    l.n_legs,
    l.leg_market_tickers,
    l.leg_sides,
    l.sports,
    l.competitions,
    l.market_types,
    l.n_sports,
    l.n_competitions,
    case
        when not l.are_all_legs_classified then null
        when l.n_sports = 1 then l.only_sport
        else 'multi_sport'
    end                                                     as sport_group,
    g.n_games,
    g.max_legs_on_one_game >= 2                             as is_same_game,
    g.n_games = 1 and l.n_legs >= 2                         as is_single_game,
    f.bot_is_same_game_parlay,
    l.are_all_legs_classified,
    f.n_fills,
    f.n_sides_filled,
    f.first_fill_at_utc,
    f.last_fill_at_utc
from leg_rollup as l
join game_rollup as g using (combo_ticker)
join fill_rollup as f using (combo_ticker)
