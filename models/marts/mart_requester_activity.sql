-- One row per RFQ requester (creator_id) per week (Monday-start, reporting
-- dates): RFQs the bot saw from them, priced, quoted, filled, fill ratio and
-- notional. ONE maker's view: it counts the RFQs that reached this bot's
-- pricing decision log (filtered RFQs are not in the extract), and the fills
-- this bot made, so it is shaped like exchange-side requester surveillance
-- but sees only what one market maker saw. Every requester is an ordinary
-- row; nothing is special-cased by id.
--
-- is_heavy_price_checker: at least var requester_price_checker_min_rfqs RFQs
-- in the week and fills per RFQ below var requester_price_checker_max_fill_ratio.
-- Never set on the no_creator_id row (below): it is not one requester.
--
-- creator_id e3b0c442...b855 is SHA-256('') (var empty_creator_id_sha256):
-- the exchange hashed an EMPTY creator id, so those RFQs have no real
-- requester. The rows are kept with creator_id unchanged; requester_label
-- says 'no_creator_id'. NULL creator_id (bot fills with no RFQ link) is
-- labelled 'no_rfq_link'. Every other row's label is its creator_id.

{{ config(materialized='table') }}

with decisions as (

    select
        creator_id,
        cast(date_trunc('week', received_date_local) as date) as week_start_local,
        count(*)                                            as n_rfqs,
        count(*) filter (where stage in ('submitted', 'priced_presubmit',
                                         'priced_rejected', 'priced_logged'))
                                                            as n_rfqs_priced,
        count(*) filter (where submit_bucket = 'live_quote') as n_rfqs_quoted,
        count(*) filter (where is_filled)                   as n_rfqs_filled
    from {{ ref('fct_rfq_lifecycle') }}
    where is_counted_decision
    group by all

),

fills as (

    select
        f.rfq_creator_id                                    as creator_id,
        cast(date_trunc('week', f.accepted_date_local) as date) as week_start_local,
        count(*)                                            as n_fills,
        sum(t.quantity)                                     as contracts,
        sum(t.notional)                                     as notional_dollars,
        sum(f.gross_pnl_dollars)                            as bot_gross_pnl_dollars
    from {{ ref('fct_fills') }} as f
    join {{ ref('mart_daily_trade_report') }} as t on t.trade_id = f.fill_id
    -- Bot fills with no RFQ link (no rfq_id, so no creator) are kept, as
    -- creator_id NULL rows, so the fills total ties to fct_fills.
    where f.fill_origin = 'bot_maker'
    group by all

),

joined as (

    select
        coalesce(d.creator_id, f.creator_id)                as creator_id,
        coalesce(d.week_start_local, f.week_start_local)    as week_start_local,
        coalesce(d.n_rfqs, 0)                               as n_rfqs,
        coalesce(d.n_rfqs_priced, 0)                        as n_rfqs_priced,
        coalesce(d.n_rfqs_quoted, 0)                        as n_rfqs_quoted,
        coalesce(d.n_rfqs_filled, 0)                        as n_rfqs_filled,
        coalesce(f.n_fills, 0)                              as n_fills,
        coalesce(f.contracts, 0)                            as contracts,
        coalesce(f.notional_dollars, 0)                     as notional_dollars,
        coalesce(f.bot_gross_pnl_dollars, 0)                as bot_gross_pnl_dollars
    from decisions as d
    full outer join fills as f
        on d.creator_id = f.creator_id and d.week_start_local = f.week_start_local

)

select
    coalesce(creator_id, '(none)') || ':' || cast(week_start_local as varchar) as requester_week_id,
    creator_id,
    case
        when creator_id = '{{ var('empty_creator_id_sha256') }}' then 'no_creator_id'
        when creator_id is null then 'no_rfq_link'
        else creator_id
    end                                                     as requester_label,
    week_start_local,
    n_rfqs,
    n_rfqs_priced,
    n_rfqs_quoted,
    n_rfqs_filled,
    n_fills,
    case when n_rfqs > 0 then n_fills * 1.0 / n_rfqs end    as fills_per_rfq,
    case when n_rfqs_quoted > 0 then n_rfqs_filled * 1.0 / n_rfqs_quoted end as fill_ratio,
    contracts,
    notional_dollars,
    bot_gross_pnl_dollars,
    coalesce(creator_id <> '{{ var('empty_creator_id_sha256') }}', false)
        and n_rfqs >= {{ var('requester_price_checker_min_rfqs') }}
        and coalesce(n_fills * 1.0 / nullif(n_rfqs, 0), 0) < {{ var('requester_price_checker_max_fill_ratio') }}
                                                            as is_heavy_price_checker
from joined
