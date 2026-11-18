-- One row per fill: the graded fill, ready to query. No logic of its own:
-- grading from int_fills_graded, origin from int_fills_origin, ids and
-- timestamps from stg_fills, combo attributes from dim_combos.

{{ config(materialized='table') }}

with fills as (

    select * from {{ ref('stg_fills') }}

),

graded as (

    select * from {{ ref('int_fills_graded') }}

),

origin as (

    select * from {{ ref('int_fills_origin') }}

),

combos as (

    select * from {{ ref('dim_combos') }}

)

select
    -- identity
    f.fill_id,
    f.order_id,
    f.quote_id,
    f.rfq_id,
    f.creator_id                                            as rfq_creator_id,
    f.combo_ticker,
    f.side,
    -- origin
    o.fill_origin,
    case o.fill_origin when 'bot_maker' then 'maker' when 'taker_script' then 'taker' end
                                                            as trade_role,
    o.is_self_trade_candidate,
    o.is_on_taker_ledger,
    o.taker_strategy,
    -- combo
    c.n_legs,
    c.sport_group,
    c.sports,
    c.competitions,
    c.market_types,
    c.n_games,
    c.is_same_game,
    c.is_single_game,
    -- size and price
    f.contract_count,
    f.max_payout_dollars,
    f.fill_price_dollars,
    f.submit_price_dollars,
    f.yes_bid_dollars,
    f.no_bid_dollars,
    -- grading and P&L
    g.parlay_result,
    g.is_pnl_graded,
    g.n_legs_won,
    g.n_legs_lost,
    g.n_legs_scalar,
    g.parlay_yes_value,
    g.payout_per_contract,
    g.fill_cost_dollars,
    g.revenue_exact_dollars,
    g.revenue_paid_dollars,
    g.gross_pnl_dollars,
    g.gross_pnl_exact_dollars,
    g.fee_dollars,
    g.fee_outside_cost_dollars,
    g.net_pnl_dollars,
    -- timestamps
    f.accepted_at_utc,
    f.accepted_at_local,
    f.accepted_date_local,
    cast(f.accepted_at_utc as date)                         as accepted_date_utc,
    f.rfq_created_at_utc,
    f.submitted_at_utc,
    f.received_at_utc,
    f.submit_to_accept_sec,
    -- data-quality flags
    f.cost_source,
    f.cost_source = 'estimate'                              as is_cost_estimated,
    g.is_fee_known,
    g.is_fee_in_fill_cost,
    f.is_accepted_at_from_received,
    f.quote_id is null                                      as is_quote_id_missing,
    f.quote_id is not null and f.rfq_id is null             as is_rfq_id_missing_with_quote,
    f.accepted_at_utc >= timestamp '{{ var("dual_instance_start_utc") }}'
        and f.accepted_at_utc < timestamp '{{ var("dual_instance_end_utc") }}'
                                                            as is_in_dual_instance_window,
    f.fill_source,
    f.reverify_path,
    f.record_schema,
    -- lineage
    f._source_key,
    f._line_no
from fills as f
join graded as g on f.fill_id = g.fill_id
join origin as o on f.fill_id = o.fill_id
left join combos as c on f.combo_ticker = c.combo_ticker
