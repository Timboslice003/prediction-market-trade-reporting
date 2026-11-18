-- One row per fill: dbt's own grading (parlay result, payout, revenue, fee,
-- gross and net P&L). Inputs: stg_fills (fill fields only) and
-- int_fill_legs_resolved. Never reads the legacy Python script's answer.
--
-- Parlay precedence, ported from the legacy script:
--   0. any leg lost                       -> 'no'
--   1. else any leg unresolved (pending)  -> null
--   2. else any leg void (anomalous)      -> 'result_anomalous', revenue and P&L null
--   3. else every leg won                 -> 'yes'
-- Scalar legs (a deliberate deviation): the script has no scalar state, so a
-- scalar leg makes the parlay result_anomalous and an operator patches it by
-- hand. dbt grades scalars natively with the operator's written payout rule:
-- the parlay's YES value is the PRODUCT of the leg values
-- (int_fill_legs_resolved.leg_value). So:
--   2b. else any leg scalar (no lost / unresolved / void) -> 'scalar'
--
-- Revenue:
--   our_side = the fill's own top-level side (yes|no), never assumed yes.
--   binary:  revenue = max_payout if parlay_result == our_side else 0
--   scalar:  revenue = max_payout x payout_per_contract, where
--            payout_per_contract = parlay YES value for a YES fill and
--            1 - that for a NO fill (a NO contract pays 1 - the YES value).
--            The operator's manual patch used the leg factor without looking
--            at the fill's side; recon_fills_python_vs_dbt categorizes that.
--   revenue_exact = max_payout x payout_per_contract, unrounded (6 dp).
--   revenue_paid  = revenue_exact rounded DOWN to the whole cent: what the
--            exchange actually pays on a fractional payout. Binary revenue is
--            count x $1 with a 2-dp count, so it is already whole cents and the
--            floor never moves it. The exchange floors per position (one payout
--            per ticker x side); dbt floors per fill, because a position-level
--            floor has no fill-level allocation.
--   gross_pnl = revenue_paid - fill_cost. fill_cost is the record's fill_cost
--            (refetched or estimated), never submit_cost_estimate.
--
-- Fee: the fill's `fee` field. It is NOT part of fill_cost on normal fills,
-- but on settle-time-enriched fills (reverify_path = 'settle_enriched',
-- cost_source = 'refetch') the legacy pipeline rebuilt fill_cost as
-- count x price + fee. There fill_cost already includes the fee. net_pnl
-- subtracts only the fee NOT already inside fill_cost, so it is never
-- double-counted. fee is null on estimate-cost fills: net_pnl is then null
-- (unknown), never gross.

{{ config(materialized='table') }}

with fills as (

    select * from {{ ref('stg_fills') }}

),

legs as (

    select * from {{ ref('int_fill_legs_resolved') }}

),

leg_rollup as (

    select
        fill_id,
        count(*)                                            as n_legs_graded,
        count(*) filter (where leg_outcome = 'won')         as n_legs_won,
        count(*) filter (where leg_outcome = 'lost')        as n_legs_lost,
        count(*) filter (where leg_outcome = 'scalar')      as n_legs_scalar,
        count(*) filter (where leg_outcome = 'void')        as n_legs_void,
        count(*) filter (where leg_outcome = 'unresolved')  as n_legs_unresolved,
        product(cast(leg_value as double))                  as leg_value_product
    from legs
    group by fill_id

),

graded as (

    select
        fills.fill_id,
        fills.combo_ticker,
        fills.side,
        fills.accepted_at_utc,
        fills.accepted_date_local,
        fills.fill_source,
        fills.reverify_path,
        fills.cost_source,
        fills.contract_count,
        fills.max_payout_dollars,
        fills.n_legs,
        coalesce(r.n_legs_graded, 0)                        as n_legs_graded,
        coalesce(r.n_legs_won, 0)                           as n_legs_won,
        coalesce(r.n_legs_lost, 0)                          as n_legs_lost,
        coalesce(r.n_legs_scalar, 0)                        as n_legs_scalar,
        coalesce(r.n_legs_void, 0)                          as n_legs_void,
        coalesce(r.n_legs_unresolved, 0)                    as n_legs_unresolved,
        case
            when coalesce(r.n_legs_graded, 0) = 0  then null                 -- no legs: cannot grade
            when r.n_legs_lost > 0                 then 'no'                 -- precedence 0
            when r.n_legs_unresolved > 0           then null                 -- precedence 1
            when r.n_legs_void > 0                 then 'result_anomalous'   -- precedence 2
            when r.n_legs_scalar > 0               then 'scalar'             -- 2b (payout rule)
            else 'yes'                                                       -- precedence 3
        end                                                 as parlay_result,
        r.leg_value_product,
        fills.fill_cost_dollars,
        fills.fee_dollars,
        (fills.reverify_path = 'settle_enriched' and fills.cost_source = 'refetch')
                                                            as is_fee_in_fill_cost,
        fills._source_key,
        fills._line_no
    from fills
    left join leg_rollup as r
        on fills.fill_id = r.fill_id

),

priced as (

    select
        *,
        -- What one contract on OUR side paid: 1 / 0 for binary parlays, the
        -- scalar value (or 1 - it for a NO fill) for scalar parlays.
        case
            when parlay_result in ('yes', 'no')
                then case when parlay_result = side then 1.0 else 0.0 end
            when parlay_result = 'scalar'
                then case side
                         when 'yes' then leg_value_product
                         when 'no'  then 1 - leg_value_product
                     end
        end                                                 as payout_per_contract_raw
    from graded

)

select
    fill_id,
    combo_ticker,
    side,
    accepted_at_utc,
    accepted_date_local,
    fill_source,
    reverify_path,
    cost_source,
    contract_count,
    max_payout_dollars,
    n_legs,
    n_legs_graded,
    n_legs_won,
    n_legs_lost,
    n_legs_scalar,
    n_legs_void,
    n_legs_unresolved,
    parlay_result,
    parlay_result is not null and parlay_result <> 'result_anomalous'
                                                            as is_pnl_graded,
    {{ to_money('leg_value_product') }}                     as parlay_yes_value,
    {{ to_money('payout_per_contract_raw') }}               as payout_per_contract,
    {{ to_money('max_payout_dollars * payout_per_contract_raw') }}
                                                            as revenue_exact_dollars,
    {{ floor_to_cent(to_money('max_payout_dollars * payout_per_contract_raw')) }}
                                                            as revenue_paid_dollars,
    fill_cost_dollars,
    fee_dollars,
    fee_dollars is not null                                 as is_fee_known,
    is_fee_in_fill_cost,
    -- fee not already inside fill_cost
    case when is_fee_in_fill_cost then 0 else fee_dollars end
                                                            as fee_outside_cost_dollars,
    {{ floor_to_cent(to_money('max_payout_dollars * payout_per_contract_raw')) }}
        - fill_cost_dollars                                 as gross_pnl_dollars,
    {{ to_money('max_payout_dollars * payout_per_contract_raw') }} - fill_cost_dollars
                                                            as gross_pnl_exact_dollars,
    {{ floor_to_cent(to_money('max_payout_dollars * payout_per_contract_raw')) }}
        - fill_cost_dollars
        - case when is_fee_in_fill_cost then 0 else fee_dollars end
                                                            as net_pnl_dollars,
    _source_key,
    _line_no
from priced
