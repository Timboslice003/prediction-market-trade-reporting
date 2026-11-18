-- One row per RFQ decision (stg_rfq_outcomes grain): what the bot did with
-- each RFQ it priced, how a quoting decision's submission ended, and whether
-- it filled. Stored as Parquet outside the DuckDB file (external_parquet) and
-- tagged heavy: on big data, routine builds read it and never rebuild it.
--
-- submit_bucket, the unified split of every QUOTING decision; NULL for
-- decisions that did not try to quote (priced_rejected, gated_skipped,
-- breaker / balance skips, priced_logged):
--   live_quote              the quote reached the exchange (has a quote_id)
--   dropped_before_sending  drop_stale_predicted / both_sides_gated /
--                           drop_stale_age_exceeded / subpenny_both_bids
--   sent_but_rejected       the exchange answered with an error status:
--                           client_error (409 / 400), server_error (5xx),
--                           rate_limited (429), auth (401/403)
--   send_outcome_unknown    sent, no response recorded (network; or, in the
--                           old era, a `pre` record with no `post`)
--   no_submission_record    old era only: the decision says it would quote,
--                           but no submission record exists. Before var
--                           submission_log_complete_from_date the submission
--                           log is an archive copy missing rows; this bucket
--                           is that gap, not a bot outcome.
-- Two schema eras:
--   v1 (rfq_outcome_v1, stage = submitted): the decision row carries
--     quote_id, exit_reason, error_kind and http_status.
--   old (would_quote_v2, stage = priced_presubmit): no quote_id on the
--     decision. Each submission attempt (stg_quote_submissions, grouped by
--     attempt_id) is linked to the decision for the same rfq_id that was
--     received last at or before the attempt (ASOF), and kept only if the
--     attempt falls inside that decision's [received_at, exit_at]. The ASOF
--     makes each attempt belong to exactly one decision, including in the
--     dual-instance window where an RFQ has two decisions.
--
-- is_counted_decision: one decision per RFQ for counting. For an RFQ decided
-- twice (dual-instance window): the filled one first, else the earlier
-- (received_at, then _source_key / _line_no). The filled check must come
-- first: a filled quote can belong to the LATER decision. True on every RFQ
-- decided once.

{{ config(materialized='external_parquet', tags=['heavy']) }}

with decisions as (

    select
        rfq_outcome_id, rfq_id, record_schema, is_v1_schema, stage, exit_reason,
        error_kind, http_status, quote_id, creator_id, combo_ticker, n_legs,
        is_replacement, yes_bid_dollars, no_bid_dollars, max_payout_dollars,
        received_at_utc, received_at_local, received_date_local, exit_at_utc,
        submit_wire_at_utc, n_decisions_for_rfq, _source_key, _line_no
    from {{ ref('stg_rfq_outcomes') }}

),

old_era_quoting as (

    select rfq_outcome_id, rfq_id, received_at_utc, exit_at_utc
    from decisions
    where record_schema = 'would_quote_v2'

),

attempts as (

    -- old-era submission attempts
    select
        attempt_id,
        any_value(rfq_id)                                                       as rfq_id,
        min(submitted_at_utc) filter (where phase = 'pre')                      as pre_submitted_at_utc,
        min(coalesce(submitted_at_utc, recorded_at_utc)) filter (where phase = 'skipped')
                                                                                as skipped_at_utc,
        bool_or(phase = 'post')                                                 as has_post,
        any_value(quote_id) filter (where phase = 'post' and success)           as post_quote_id,
        any_value(http_status) filter (where phase = 'post')                    as post_http_status,
        any_value(error_kind) filter (where phase = 'post')                     as post_error_kind,
        any_value(skip_reason) filter (where phase = 'skipped')                 as skip_reason
    from {{ ref('stg_quote_submissions') }}
    where recorded_at_utc < timestamp '{{ var("old_era_attempts_before_utc") }}'
    group by attempt_id

),

attempts_timed as (

    select *, coalesce(pre_submitted_at_utc, skipped_at_utc) as attempt_at_utc
    from attempts

),

linked as (

    select a.*, d.rfq_outcome_id
    from attempts_timed as a
    asof join old_era_quoting as d
        on a.rfq_id = d.rfq_id and a.attempt_at_utc >= d.received_at_utc
    where a.attempt_at_utc <= d.exit_at_utc

),

old_era_outcome as (

    select
        rfq_outcome_id,
        count(*)                                                                as n_submission_attempts,
        any_value(post_quote_id) filter (where post_quote_id is not null)       as quote_id,
        bool_or(post_quote_id is not null)                                      as any_live,
        bool_or(has_post and post_quote_id is null and post_http_status is not null)
                                                                                as any_rejected,
        bool_or((has_post and post_quote_id is null and post_http_status is null)
                or (pre_submitted_at_utc is not null and not has_post))         as any_unknown,
        bool_or(skip_reason is not null)                                        as any_skipped,
        any_value(post_http_status) filter (where post_quote_id is null and post_http_status is not null)
                                                                                as http_status,
        any_value(post_error_kind) filter (where post_error_kind is not null)   as error_kind,
        any_value(skip_reason) filter (where skip_reason is not null)           as skip_reason,
        min(pre_submitted_at_utc)                                               as submitted_at_utc
    from linked
    group by rfq_outcome_id

),

unified as (

    select
        d.*,
        p.n_submission_attempts,
        case
            when d.record_schema = 'would_quote_v2' then p.quote_id
            else d.quote_id
        end                                                                     as unified_quote_id,
        case
            when d.stage = 'submitted' then
                case
                    when d.quote_id is not null                                   then 'live_quote'
                    when d.exit_reason in ('drop_stale_predicted', 'both_sides_gated',
                                           'drop_stale_age_exceeded', 'subpenny_both_bids')
                                                                                  then 'dropped_before_sending'
                    when d.error_kind in ('client_error', 'server_error',
                                          'rate_limited', 'auth')                 then 'sent_but_rejected'
                    when d.error_kind = 'network'                                 then 'send_outcome_unknown'
                    else 'unclassified'
                end
            when d.record_schema = 'would_quote_v2' then
                case
                    when p.rfq_outcome_id is null then 'no_submission_record'
                    when p.any_live              then 'live_quote'
                    when p.any_rejected          then 'sent_but_rejected'
                    when p.any_unknown           then 'send_outcome_unknown'
                    when p.any_skipped           then 'dropped_before_sending'
                    else 'unclassified'
                end
        end                                                                     as submit_bucket,
        case when d.record_schema = 'would_quote_v2' then p.http_status else d.http_status end
                                                                                as unified_http_status,
        case when d.record_schema = 'would_quote_v2' then p.error_kind else d.error_kind end
                                                                                as unified_error_kind,
        case when d.record_schema = 'would_quote_v2' then p.skip_reason end      as skip_reason,
        case when d.record_schema = 'would_quote_v2' then p.submitted_at_utc else d.submit_wire_at_utc end
                                                                                as submitted_at_utc
    from decisions as d
    left join old_era_outcome as p
        on d.rfq_outcome_id = p.rfq_outcome_id

),

fills_by_quote as (

    select
        quote_id,
        count(*)                                                                as n_fills,
        arg_min(fill_id, accepted_at_utc)                                       as first_fill_id
    from {{ ref('stg_fills') }}
    where quote_id is not null
    group by quote_id

),

with_fills as (

    select
        u.*,
        f.quote_id is not null                                                  as is_filled,
        coalesce(f.n_fills, 0)                                                  as n_fills,
        f.first_fill_id
    from unified as u
    left join fills_by_quote as f
        on u.unified_quote_id = f.quote_id

),

doubled_ranked as (

    select
        rfq_outcome_id,
        row_number() over (
            partition by rfq_id
            order by is_filled desc, received_at_utc, _source_key, _line_no
        ) = 1                                                                   as is_counted_decision
    from with_fills
    where n_decisions_for_rfq > 1

)

select
    w.rfq_outcome_id,
    w.rfq_id,
    w.record_schema,
    w.is_v1_schema,
    w.stage,
    w.exit_reason,
    w.submit_bucket,
    w.unified_http_status                                                       as http_status,
    w.unified_error_kind                                                        as error_kind,
    w.skip_reason,
    w.unified_quote_id                                                          as quote_id,
    w.n_submission_attempts,
    w.is_filled,
    w.n_fills,
    w.first_fill_id                                                             as fill_id,
    w.received_at_utc,
    w.received_at_local,
    w.received_date_local,
    w.exit_at_utc,
    w.submitted_at_utc,
    date_diff('microsecond', w.received_at_utc, w.submitted_at_utc) / 1000.0   as receipt_to_submit_ms,
    w.n_decisions_for_rfq,
    coalesce(r.is_counted_decision, true)                                       as is_counted_decision,
    w.creator_id,
    w.combo_ticker,
    w.n_legs,
    w.is_replacement,
    w.yes_bid_dollars,
    w.no_bid_dollars,
    w.max_payout_dollars,
    w._source_key,
    w._line_no
from with_fills as w
left join doubled_ranked as r
    on w.rfq_outcome_id = r.rfq_outcome_id
