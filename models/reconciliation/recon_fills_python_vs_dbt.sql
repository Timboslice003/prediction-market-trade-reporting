-- One row per fill: dbt's grading (int_fills_graded) next to the legacy Python
-- settlement script's (stg_python_graded), with the differences and, for
-- every fill that differs, a mismatch_category. This is the ONLY model that
-- reads both answers; dbt's grading never reads the script's.
--
-- dbt's side is its headline grading: revenue_paid (the exchange floors
-- fractional payouts to the cent) and gross_pnl built on it. The exact
-- product is kept as dbt_revenue_exact_dollars.
--
-- A fill differs when parlay_result differs, or revenue or P&L differ by more
-- than $0.000001. The three revenue categories require a revenue difference;
-- a P&L-only difference can only be rounding. Categories are rules on the
-- data, not lists of fill_ids, so a new kind of difference lands in
-- 'unexplained' (and fails assert_recon_no_unexplained_mismatches).
-- First matching rule wins:
--
--   python_scalar_patch_ignored_fill_side
--       A NO-side fill on a scalar parlay. The operator's manual patch
--       (multiplier = settlement value if the LEG is YES, else 1 - it) never
--       looks at the FILL's side, so it credited the YES value; a NO contract
--       pays 1 - the YES value. Python revenue = max_payout x parlay YES value.
--   python_scalar_patch_assumed_half
--       Python credited max_payout x 0.50 (an earlier blanket rule for voided
--       legs, later retracted), but the exchange's settlement value for this
--       fill's scalar leg is not 0.50.
--   python_books_exact_not_paid_cent
--       Python booked the exact product max_payout x value; the exchange paid
--       that floored to the cent, which is dbt's revenue.
--       Python revenue = dbt revenue_exact, and dbt revenue = floor(exact).
--   python_pnl_rounded_4dp
--       Revenue agrees; the patch rounded realized_pnl to 4 decimals, so P&L
--       differs by < $0.0001.
--   unexplained
--       Anything else.
--
-- Context flags (not categories; they explain nothing by themselves): the
-- dual-instance window, settle-time enrichment, fee already inside fill_cost.

{{ config(materialized='table') }}

with dbt_graded as (

    select * from {{ ref('int_fills_graded') }}

),

python_graded as (

    select * from {{ ref('stg_python_graded') }}

),

joined as (

    select
        coalesce(d.fill_id, p.fill_id)                      as fill_id,
        d.fill_id is not null                               as is_in_dbt,
        p.fill_id is not null                               as is_in_python,
        d.combo_ticker,
        d.side,
        d.accepted_at_utc,
        d.accepted_date_local,
        d.max_payout_dollars,
        d.n_legs,
        d.n_legs_scalar,
        d.parlay_yes_value,
        d.payout_per_contract,

        d.parlay_result                                     as dbt_parlay_result,
        p.parlay_result                                     as python_parlay_result,
        d.revenue_paid_dollars                              as dbt_revenue_dollars,
        d.revenue_exact_dollars                             as dbt_revenue_exact_dollars,
        p.revenue_dollars                                   as python_revenue_dollars,
        d.fill_cost_dollars,
        d.gross_pnl_dollars                                 as dbt_gross_pnl_dollars,
        p.realized_pnl_dollars                              as python_realized_pnl_dollars,
        d.fee_dollars,
        d.net_pnl_dollars                                   as dbt_net_pnl_dollars,

        d.revenue_paid_dollars - p.revenue_dollars          as revenue_diff_dollars,
        d.gross_pnl_dollars - p.realized_pnl_dollars        as pnl_diff_dollars,

        p.is_manually_fixed                                 as python_is_manually_fixed,
        p.manual_fix_note                                   as python_manual_fix_note,
        d.reverify_path,
        d.cost_source,
        d.is_fee_in_fill_cost,
        d.accepted_at_utc >= timestamp '{{ var("dual_instance_start_utc") }}'
            and d.accepted_at_utc < timestamp '{{ var("dual_instance_end_utc") }}'
                                                            as is_in_dual_instance_window
    from dbt_graded as d
    full outer join python_graded as p
        on d.fill_id = p.fill_id

),

flagged as (

    select
        *,
        not is_in_dbt
            or not is_in_python
            or dbt_parlay_result is distinct from python_parlay_result
            or coalesce(abs(revenue_diff_dollars) > 0.000001,
                        dbt_revenue_dollars is distinct from python_revenue_dollars)
            or coalesce(abs(pnl_diff_dollars) > 0.000001,
                        dbt_gross_pnl_dollars is distinct from python_realized_pnl_dollars)
                                                            as is_mismatch
    from joined

)

select
    *,
    case
        when not is_mismatch then null
        when not (is_in_dbt and is_in_python)
          or dbt_parlay_result is distinct from python_parlay_result
            then 'unexplained'
        when dbt_parlay_result = 'scalar'
         and abs(revenue_diff_dollars) > 0.000001
         and side = 'no'
         and abs(python_revenue_dollars - max_payout_dollars * parlay_yes_value) < 0.00005
            then 'python_scalar_patch_ignored_fill_side'
        when dbt_parlay_result = 'scalar'
         and abs(revenue_diff_dollars) > 0.000001
         and payout_per_contract <> 0.5
         and abs(python_revenue_dollars - max_payout_dollars * 0.5) < 0.00005
            then 'python_scalar_patch_assumed_half'
        when dbt_parlay_result = 'scalar'
         and abs(revenue_diff_dollars) > 0.000001
         and abs(python_revenue_dollars - dbt_revenue_exact_dollars) <= 0.000001
         and dbt_revenue_dollars = floor(dbt_revenue_exact_dollars * 100) / 100
         and abs(pnl_diff_dollars - revenue_diff_dollars) < 0.0001
            then 'python_books_exact_not_paid_cent'
        when abs(revenue_diff_dollars) <= 0.000001
         and abs(pnl_diff_dollars) < 0.0001
            then 'python_pnl_rounded_4dp'
        else 'unexplained'
    end                                                     as mismatch_category
from flagged
