-- One row per trade (fill), keyed by trade date (UTC). Modeled on an
-- exchange's daily trade report; NOT an actual regulatory filing.
-- Column names and types are enforced by a dbt model contract
-- (_marts__models.yml).
--
-- price: the recorded fill price. On some estimate-cost fills it was never
--   recorded; there it is notional / quantity (price_source says which).
-- notional: quantity x price, i.e. fill_cost without the fee the legacy
--   pipeline folded into it on settle_enriched fills (equals count x price
--   exactly wherever price is recorded).
-- role: maker = the bot's quote was accepted; taker = the taker script
--   accepted another maker's quote.

{{ config(materialized='table') }}

with f as (

    select * from {{ ref('fct_fills') }}

),

priced as (

    select
        *,
        fill_cost_dollars - case when is_fee_in_fill_cost then fee_dollars else 0 end
                                                            as notional_raw
    from f

)

select
    cast(accepted_at_utc as date)                           as trade_date,
    cast(fill_id as varchar)                                as trade_id,
    cast(accepted_at_utc as timestamp)                      as execution_time_utc,
    cast(is_accepted_at_from_received as boolean)           as is_execution_time_fallback,
    cast(combo_ticker as varchar)                           as contract_ticker,
    cast(side as varchar)                                   as side,
    cast(contract_count as decimal(18, 2))                  as quantity,
    cast(coalesce(fill_price_dollars, notional_raw / contract_count) as decimal(18, 6))
                                                            as price,
    cast(case when fill_price_dollars is null then 'derived_from_estimated_cost'
              else 'recorded' end as varchar)               as price_source,
    cast(notional_raw as decimal(18, 6))                    as notional,
    cast(fee_dollars as decimal(18, 6))                     as fee,
    cast(trade_role as varchar)                             as role,
    cast(quote_id as varchar)                               as quote_id,
    cast(rfq_id as varchar)                                 as rfq_id,
    cast(n_legs as integer)                                 as leg_count,
    cast(is_cost_estimated as boolean)                      as is_cost_estimated
from priced
