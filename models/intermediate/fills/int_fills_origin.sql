-- One row per fill: who made it. The bot's fill log records every fill on the
-- account, including the separate taker script's (the bot discovers them by
-- REST poll), and no field says which is which. The taker ledgers
-- (stg_taker_plays) do.
--
-- A fill matches a taker play when it is on the play's combo ticker and has
-- the play's contract count (to 0.005). Of the matches:
--   taker_script: no quote_id. The account was the RFQ requester, so there is
--     no maker quote of ours.
--   a match WITH a quote_id: the fill carries the BOT's own quote id, and the
--     taker ledger lists the same rfq_id and quote_id. It looks like a
--     self-trade. On this account it is a one-sided bot maker fill: the
--     exchange settled only the bot's side, and the RFQ came from another
--     requester. The ledger row was rebuilt from the settled fill after the
--     fact, so it claims the bot's side as a take. fill_origin = bot_maker,
--     flagged is_self_trade_candidate.

{{ config(materialized='table') }}

with fills as (

    select * from {{ ref('stg_fills') }}

),

takes as (

    select * from {{ ref('stg_taker_plays') }}
    where action = 'take'

),

matched as (

    select
        fills.fill_id,
        fills.combo_ticker,
        fills.side,
        fills.quote_id,
        fills.rfq_id,
        takes.taker_play_id,
        takes.strategy                                      as taker_strategy,
        takes.quote_id                                      as taker_quote_id
    from fills
    left join takes
        on  fills.combo_ticker = takes.combo_ticker
        and abs(fills.contract_count - takes.contract_count) < 0.005

)

select
    fill_id,
    combo_ticker,
    side,
    case
        when taker_play_id is not null and quote_id is null then 'taker_script'
        else 'bot_maker'
    end                                                     as fill_origin,
    taker_play_id is not null                               as is_on_taker_ledger,
    taker_play_id is not null and quote_id is not null
        and quote_id = taker_quote_id                       as is_self_trade_candidate,
    taker_play_id,
    taker_strategy
from matched
