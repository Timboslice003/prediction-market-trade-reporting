-- One row per fill leg (fill_id x leg_index): the leg's outcome from its
-- declared side and the exchange's market result. Graded independently of the
-- legacy Python settlement script: reads only stg_fill_legs +
-- stg_market_results, never stg_python_graded / legs_settled.
--
-- The rules port the legacy script's settlement classification:
--   no market row                      -> pending (the exchange returned 404)
--   status not settled / finalized     -> pending
--   terminal and result in {yes, no}   -> scored
--   terminal, any other result         -> void
--   a leg with no market_ticker        -> anomalous
--   resolved leg won = (result == the leg's declared side)
-- and the operator's written payout rule for scalar legs: a scalar leg
-- contributes settlement_value_dollars if declared YES and
-- 1 - settlement_value_dollars if declared NO; binary legs contribute 1 or 0.
-- The legacy script has no scalar state (it calls scalars anomalous and an
-- operator patched them by hand); dbt applies the rule natively, from the
-- exchange's own settlement value in market_results.
--
-- leg_outcome:
--   won / lost   binary result, compared with the leg's declared side
--   scalar       result 'scalar' with a settlement value (leg_value carries it)
--   void         terminal but neither binary nor a usable scalar, or no
--                market_ticker (the script's "anomalous")
--   unresolved   no market row, or market not terminal (the script's "pending")

{{ config(materialized='table') }}

with legs as (

    select * from {{ ref('stg_fill_legs') }}

),

markets as (

    select * from {{ ref('stg_market_results') }}

),

joined as (

    select
        legs.fill_leg_id,
        legs.fill_id,
        legs.leg_index,
        legs.market_ticker,
        legs.side                                   as leg_side,
        markets.market_ticker is not null           as has_market_row,
        markets.market_status,
        markets.market_result,
        markets.settlement_value_dollars,
        markets.settled_at_utc,
        coalesce(lower(markets.market_status) in ('settled', 'finalized'), false)
                                                    as is_market_terminal,
        legs._source_key,
        legs._line_no
    from legs
    left join markets
        on legs.market_ticker = markets.market_ticker

),

classified as (

    select
        *,
        case
            when nullif(trim(market_ticker), '') is null       then 'void'
            when not is_market_terminal                         then 'unresolved'
            when lower(market_result) in ('yes', 'no')
                then case when lower(market_result) = leg_side then 'won' else 'lost' end
            when lower(market_result) = 'scalar'
                 and settlement_value_dollars is not null       then 'scalar'
            else 'void'
        end                                                     as leg_outcome,
        case
            when nullif(trim(market_ticker), '') is null       then 'no_market_ticker'
            when not has_market_row                             then 'no_market_row'
            when not is_market_terminal                         then 'market_not_terminal'
            when lower(market_result) in ('yes', 'no')          then 'binary_result'
            when lower(market_result) = 'scalar'
                 and settlement_value_dollars is not null       then 'scalar_settlement_value'
            else 'terminal_non_binary'
        end                                                     as leg_outcome_reason
    from joined

)

select
    fill_leg_id,
    fill_id,
    leg_index,
    market_ticker,
    leg_side,
    leg_outcome,
    leg_outcome_reason,
    -- This leg's factor in the parlay's settlement value (the scalar payout
    -- rule). Null for void / unresolved legs.
    case leg_outcome
        when 'won'    then cast(1 as decimal(18, 6))
        when 'lost'   then cast(0 as decimal(18, 6))
        when 'scalar' then case leg_side
                               when 'yes' then settlement_value_dollars
                               when 'no'  then 1 - settlement_value_dollars
                           end
    end                                                         as leg_value,
    market_status,
    market_result,
    settlement_value_dollars,
    settled_at_utc,
    _source_key,
    _line_no
from classified
