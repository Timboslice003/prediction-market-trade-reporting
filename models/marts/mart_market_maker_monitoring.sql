-- One row per reporting day: the bot as a market maker. ONE maker's view (this
-- bot's own logs), shaped like exchange-side market-maker monitoring.
--
-- Sources and their known gaps (flag columns say which days are affected):
--   decisions (fct_rfq_lifecycle, counted decisions only): filtered RFQs are
--     not in the extract, so n_rfqs_decided is RFQs that reached a pricing
--     decision, not every RFQ the exchange broadcast. A logging bug lost the
--     tail of some hours of decisions (vars decision_log_tail_loss_*).
--   submission attempts (stg_quote_submissions, `post` records): HTTP status
--     per attempt, so a retried 429 counts each time. Missing rows before var
--     submission_log_complete_from_date (archive copy only).
--   fills (fct_fills): bot maker fills by fill date; taker-script fills
--     reported apart and kept out of the acceptance rate.
-- acceptance_rate = fills per quote successfully sent, matched BY QUOTE:
-- numerator and denominator come from the same record set.
--   denominator: counted live quotes (submit_bucket 'live_quote');
--   numerator: bot maker fills whose quote_id is one of those live quotes,
--     credited to the quote's day (the decision's receipt date), not the
--     fill's;
--   n_unmatched_bot_maker_fills (by fill date): bot maker fills with no
--     counted live quote (no quote_id, or the quote's decision row is not
--     in the extract: tail loss, archive-copy gaps). Counted here so nothing
--     disappears; not in the rate.
-- Report period-level rates as sum(n_matched_fills) / sum(n_live_quotes),
-- never as an average of daily rates.
-- Alert thresholds are vars (dbt_project.yml mm_alert_*).

{{ config(materialized='table') }}

with decisions as (

    select
        received_date_local                                 as date_local,
        count(*)                                            as n_rfqs_decided,
        count(*) filter (where stage in ('submitted', 'priced_presubmit',
                                         'priced_rejected', 'priced_logged'))
                                                            as n_rfqs_priced,
        count(*) filter (where submit_bucket is not null)   as n_quoting_decisions,
        count(*) filter (where submit_bucket = 'live_quote') as n_live_quotes,
        count(*) filter (where submit_bucket = 'dropped_before_sending') as n_dropped_before_sending,
        count(*) filter (where submit_bucket = 'sent_but_rejected')      as n_sent_but_rejected,
        count(*) filter (where submit_bucket = 'send_outcome_unknown')   as n_send_outcome_unknown,
        count(*) filter (where submit_bucket = 'no_submission_record')   as n_no_submission_record,
        count(*) filter (where is_filled)                   as n_decisions_filled,
        quantile_cont(receipt_to_submit_ms, 0.5)  filter (where submit_bucket = 'live_quote') as latency_p50_ms,
        quantile_cont(receipt_to_submit_ms, 0.9)  filter (where submit_bucket = 'live_quote') as latency_p90_ms,
        quantile_cont(receipt_to_submit_ms, 0.99) filter (where submit_bucket = 'live_quote') as latency_p99_ms,
        count(receipt_to_submit_ms) filter (where submit_bucket = 'live_quote') as n_latency_samples
    from {{ ref('fct_rfq_lifecycle') }}
    where is_counted_decision
    group by received_date_local

),

submissions as (

    select
        recorded_date_local                                 as date_local,
        count(*) filter (where phase = 'post')              as n_quote_posts,
        count(*) filter (where phase = 'post' and http_status = 200) as n_post_200,
        count(*) filter (where phase = 'post' and http_status = 400) as n_post_400,
        count(*) filter (where phase = 'post' and http_status = 409) as n_post_409,
        count(*) filter (where phase = 'post' and http_status = 429) as n_post_429,
        count(*) filter (where phase = 'post' and http_status in (401, 403)) as n_post_401_403,
        count(*) filter (where phase = 'post' and http_status >= 500) as n_post_5xx,
        count(*) filter (where phase = 'post' and http_status is null) as n_post_no_status,
        count(*) filter (where phase = 'skipped')           as n_skipped_attempts
    from {{ ref('stg_quote_submissions') }}
    group by recorded_date_local

),

live_quotes as (

    select quote_id, received_date_local as quote_date_local
    from {{ ref('fct_rfq_lifecycle') }}
    where is_counted_decision and submit_bucket = 'live_quote'

),

bot_maker_fills as (

    select f.fill_id, f.accepted_date_local, q.quote_date_local
    from {{ ref('fct_fills') }} as f
    left join live_quotes as q on q.quote_id = f.quote_id
    where f.fill_origin = 'bot_maker'

),

matched_fills as (

    select quote_date_local as date_local, count(*) as n_matched_fills
    from bot_maker_fills
    where quote_date_local is not null
    group by quote_date_local

),

unmatched_fills as (

    select accepted_date_local as date_local, count(*) as n_unmatched_bot_maker_fills
    from bot_maker_fills
    where quote_date_local is null
    group by accepted_date_local

),

fills as (

    select
        accepted_date_local                                 as date_local,
        count(*) filter (where fill_origin = 'bot_maker')   as n_bot_maker_fills,
        count(*) filter (where fill_origin = 'taker_script') as n_taker_fills,
        sum(contract_count) filter (where fill_origin = 'bot_maker') as bot_maker_contracts,
        sum(fill_cost_dollars) filter (where fill_origin = 'bot_maker') as bot_maker_fill_cost_dollars
    from {{ ref('fct_fills') }}
    group by accepted_date_local

),

days as (

    select date_local from decisions
    union
    select date_local from submissions
    union
    select date_local from fills
    union
    select date_local from matched_fills

),

joined as (

    select
        d.date_local,
        coalesce(x.n_rfqs_decided, 0)                       as n_rfqs_decided,
        coalesce(x.n_rfqs_priced, 0)                        as n_rfqs_priced,
        coalesce(x.n_quoting_decisions, 0)                  as n_quoting_decisions,
        coalesce(x.n_live_quotes, 0)                        as n_live_quotes,
        coalesce(x.n_dropped_before_sending, 0)             as n_dropped_before_sending,
        coalesce(x.n_sent_but_rejected, 0)                  as n_sent_but_rejected,
        coalesce(x.n_send_outcome_unknown, 0)               as n_send_outcome_unknown,
        coalesce(x.n_no_submission_record, 0)               as n_no_submission_record,
        coalesce(x.n_decisions_filled, 0)                   as n_decisions_filled,
        coalesce(s.n_quote_posts, 0)                        as n_quote_posts,
        coalesce(s.n_post_200, 0)                           as n_post_200,
        coalesce(s.n_post_400, 0)                           as n_post_400,
        coalesce(s.n_post_409, 0)                           as n_post_409,
        coalesce(s.n_post_429, 0)                           as n_post_429,
        coalesce(s.n_post_401_403, 0)                       as n_post_401_403,
        coalesce(s.n_post_5xx, 0)                           as n_post_5xx,
        coalesce(s.n_post_no_status, 0)                     as n_post_no_status,
        coalesce(s.n_skipped_attempts, 0)                   as n_skipped_attempts,
        coalesce(f.n_bot_maker_fills, 0)                    as n_bot_maker_fills,
        coalesce(mf.n_matched_fills, 0)                     as n_matched_fills,
        coalesce(uf.n_unmatched_bot_maker_fills, 0)         as n_unmatched_bot_maker_fills,
        coalesce(f.n_taker_fills, 0)                        as n_taker_fills,
        coalesce(f.bot_maker_contracts, 0)                  as bot_maker_contracts,
        coalesce(f.bot_maker_fill_cost_dollars, 0)          as bot_maker_fill_cost_dollars,
        x.latency_p50_ms,
        x.latency_p90_ms,
        x.latency_p99_ms,
        coalesce(x.n_latency_samples, 0)                    as n_latency_samples
    from days as d
    left join decisions as x using (date_local)
    left join submissions as s using (date_local)
    left join fills as f using (date_local)
    left join matched_fills as mf using (date_local)
    left join unmatched_fills as uf using (date_local)

),

rated as (

    select
        *,
        case when n_live_quotes > 0 then n_matched_fills * 1.0 / n_live_quotes end
                                                            as acceptance_rate,
        case when n_quote_posts > 0 then n_post_429 * 1.0 / n_quote_posts end
                                                            as rate_limited_share,
        case when n_quoting_decisions > 0 then n_sent_but_rejected * 1.0 / n_quoting_decisions end
                                                            as rejected_share
    from joined

)

select
    *,
    coalesce(rate_limited_share > {{ var('mm_alert_429_share_max') }}, false)       as is_alert_rate_limited,
    coalesce(rejected_share > {{ var('mm_alert_rejected_share_max') }}, false)      as is_alert_rejections,
    coalesce(acceptance_rate < {{ var('mm_alert_acceptance_rate_min') }}, false)    as is_alert_low_acceptance,
    coalesce(latency_p99_ms > {{ var('mm_alert_latency_p99_ms_max') }}, false)      as is_alert_latency,
    case
        when date_local < date '{{ var("submission_log_complete_from_date") }}'  then 'submission_log_undercounted'
        when date_local between date '{{ var("decision_log_tail_loss_from_date") }}'
                            and date '{{ var("decision_log_tail_loss_until_date") }}' then 'decision_log_tail_loss'
        else 'clean'
    end                                                     as data_period,
    date_local < date '{{ var("submission_log_complete_from_date") }}'
                                                            as is_submission_log_undercounted,
    date_local between date '{{ var("decision_log_tail_loss_from_date") }}'
                   and date '{{ var("decision_log_tail_loss_until_date") }}'
                                                            as is_decision_log_tail_loss
from rated
