-- One row per UTC hour of the completeness window (the only window both
-- sources cover): RFQs the exchange sent (stg_events_rfq_created, one
-- rfq_created frame each) against RFQs the bot recorded a decision for
-- (stg_rfq_outcomes_slim, which keeps filtered decisions too), matched on
-- rfq_id, and the likely cause of each hour's gap.
--
-- Matching: full outer join on hash(rfq_id) (both sources are unique on
-- rfq_id, tested). Hashing keeps a very large join inside memory on big
-- data; the expected number of 64-bit collisions is negligible. An RFQ's hour
-- is its exchange creation hour when the event exists, else the bot's
-- receipt hour.
--
-- Where in the hour the missing RFQs sit separates the causes:
--   n_missing_head  created before the bot's first recorded RFQ that hour
--   n_missing_tail  created after the bot's last recorded RFQ that hour,
--                   ignoring the final minute (see recording_bounds)
--   n_missing_mid   in between
-- likely_cause:
--   bot_off_no_data          neither of the bot's logs has anything this hour:
--                            bot off or no RFQ flow (the events log is written
--                            by the bot too)
--   no_gap                   nothing missing
--   bot_off                  the bot recorded no decision this hour
--   bot_off_partial          the gap is the head of the hour: the bot started
--                            (or resumed) mid-hour
--   decision_log_tail_loss   the gap is the tail of the hour and recording
--                            resumed promptly in the next hour: the logging
--                            bug that dropped the tail of an hour (before var
--                            tail_loss_fix_deployed_at_utc)
--   tail_loss_after_fix      the same tail signature in an hour after the fix
--   bot_stopped              tail gap, and the next hour does not resume promptly
--   unexplained              anything else (mostly mid-hour)
-- A small gap (below var completeness_min_gap_rfqs) is 'no_gap_within_tolerance'
-- so edge effects (an RFQ created 06:59:59 and received 07:00:00 at the window
-- boundary) do not drown the real gaps; the count is still shown.

{{ config(materialized='table') }}

{% set window_start = "timestamp '" ~ var('completeness_window_start_utc') ~ "'" %}
{% set window_end   = "timestamp '" ~ var('completeness_window_end_utc') ~ "'" %}
{% set fixed_at     = "timestamp '" ~ var('tail_loss_fix_deployed_at_utc') ~ "'" %}
{% set resume_max   = var('completeness_resume_max_sec') %}

with sent as (

    select hash(rfq_id) as k, rfq_created_at_utc as created_at_utc
    from {{ ref('stg_events_rfq_created') }}
    where rfq_created_at_utc >= {{ window_start }} and rfq_created_at_utc < {{ window_end }}

),

recorded as (

    select hash(rfq_id) as k, received_at_utc
    from {{ ref('stg_rfq_outcomes_slim') }}
    where received_at_utc >= {{ window_start }} and received_at_utc < {{ window_end }}

),

matched as (

    select
        s.created_at_utc,
        r.received_at_utc,
        date_trunc('hour', coalesce(s.created_at_utc, r.received_at_utc)) as hour_utc
    from sent as s
    full outer join recorded as r on s.k = r.k

),

recording_bounds as (

    select
        hour_utc,
        min(received_at_utc)                                as first_recorded_at_utc,
        max(received_at_utc)                                as last_recorded_at_utc,
        -- The tail boundary ignores the hour's final minute: when the writer
        -- recovers at the hour roll, RFQs created at :59 are written to the
        -- next file and would otherwise make a lost tail look complete.
        max(received_at_utc) filter (
            where received_at_utc < hour_utc + interval 59 minute
        )                                                   as last_recorded_before_final_minute_utc
    from matched
    where received_at_utc is not null
    group by hour_utc

),

hourly as (

    select
        m.hour_utc,
        count(*) filter (where m.created_at_utc is not null)                    as n_rfqs_sent,
        count(*) filter (where m.received_at_utc is not null)                   as n_rfqs_recorded,
        count(*) filter (where m.created_at_utc is not null and m.received_at_utc is not null)
                                                                                as n_rfqs_in_both,
        count(*) filter (where m.received_at_utc is null)                       as n_rfqs_missing,
        count(*) filter (where m.created_at_utc is null)                        as n_rfqs_recorded_no_event,
        count(*) filter (where m.received_at_utc is null
                         and (b.first_recorded_at_utc is null or m.created_at_utc < b.first_recorded_at_utc))
                                                                                as n_missing_head,
        count(*) filter (where m.received_at_utc is null
                         and m.created_at_utc > coalesce(b.last_recorded_before_final_minute_utc, b.first_recorded_at_utc))
                                                                                as n_missing_tail,
        count(*) filter (where m.received_at_utc is null
                         and m.created_at_utc >= b.first_recorded_at_utc
                         and m.created_at_utc <= coalesce(b.last_recorded_before_final_minute_utc, b.first_recorded_at_utc))
                                                                                as n_missing_mid,
        max(m.created_at_utc)                                                   as last_sent_at_utc,
        max(m.created_at_utc) filter (where m.created_at_utc < m.hour_utc + interval 59 minute)
                                                                                as last_sent_before_final_minute_utc,
        any_value(b.first_recorded_at_utc)                                      as first_recorded_at_utc,
        any_value(b.last_recorded_at_utc)                                       as last_recorded_at_utc,
        any_value(b.last_recorded_before_final_minute_utc)                      as last_recorded_before_final_minute_utc
    from matched as m
    left join recording_bounds as b using (hour_utc)
    group by m.hour_utc

),

hours as (

    select unnest(generate_series({{ window_start }}, {{ window_end }} - interval 1 hour, interval 1 hour)) as hour_utc

),

filled as (

    select
        h.hour_utc,
        coalesce(x.n_rfqs_sent, 0)                          as n_rfqs_sent,
        coalesce(x.n_rfqs_recorded, 0)                      as n_rfqs_recorded,
        coalesce(x.n_rfqs_in_both, 0)                       as n_rfqs_in_both,
        coalesce(x.n_rfqs_missing, 0)                       as n_rfqs_missing,
        coalesce(x.n_rfqs_recorded_no_event, 0)             as n_rfqs_recorded_no_event,
        coalesce(x.n_missing_head, 0)                       as n_missing_head,
        coalesce(x.n_missing_mid, 0)                        as n_missing_mid,
        coalesce(x.n_missing_tail, 0)                       as n_missing_tail,
        x.first_recorded_at_utc,
        x.last_recorded_at_utc,
        x.last_sent_at_utc,
        x.last_recorded_before_final_minute_utc,
        x.last_sent_before_final_minute_utc,
        lead(x.first_recorded_at_utc) over (order by h.hour_utc) as next_hour_first_recorded_at_utc
    from hours as h
    left join hourly as x using (hour_utc)

)

select
    hour_utc,
    {{ utc_to_local('hour_utc') }}                          as hour_local,
    n_rfqs_sent,
    n_rfqs_recorded,
    n_rfqs_in_both,
    n_rfqs_missing,
    case when n_rfqs_sent > 0 then n_rfqs_missing * 1.0 / n_rfqs_sent end as missing_share,
    n_rfqs_recorded_no_event,
    n_missing_head,
    n_missing_mid,
    n_missing_tail,
    first_recorded_at_utc,
    last_recorded_at_utc,
    last_sent_at_utc,
    last_recorded_before_final_minute_utc,
    date_diff('second', last_recorded_before_final_minute_utc, hour_utc + interval 1 hour)
                                                                            as recording_stopped_sec_before_hour_end,
    -- The rfq_created event log is written by the same bot and can lose its
    -- own tails: how long before the hour end it went silent (final minute ignored).
    date_diff('second', last_sent_before_final_minute_utc, hour_utc + interval 1 hour)
                                                                            as events_stopped_sec_before_hour_end,
    date_diff('second', hour_utc + interval 1 hour, next_hour_first_recorded_at_utc)
                                                                            as next_hour_resumed_after_sec,
    case
        when n_rfqs_sent = 0 and n_rfqs_recorded = 0                        then 'bot_off_no_data'
        when n_rfqs_missing = 0                                             then 'no_gap'
        when n_rfqs_missing < {{ var('completeness_min_gap_rfqs') }}        then 'no_gap_within_tolerance'
        when n_rfqs_recorded = 0                                            then 'bot_off'
        when n_missing_head >= greatest(n_missing_mid, n_missing_tail)      then 'bot_off_partial'
        when n_missing_tail >= n_missing_mid
         and date_diff('second', hour_utc + interval 1 hour, next_hour_first_recorded_at_utc) <= {{ resume_max }}
         and hour_utc < {{ fixed_at }}                                       then 'decision_log_tail_loss'
        when n_missing_tail >= n_missing_mid
         and date_diff('second', hour_utc + interval 1 hour, next_hour_first_recorded_at_utc) <= {{ resume_max }}
                                                                            then 'tail_loss_after_fix'
        when n_missing_tail >= n_missing_mid                                then 'bot_stopped'
        else 'unexplained'
    end                                                                     as likely_cause,
    hour_utc = {{ window_start }} or hour_utc = {{ window_end }} - interval 1 hour
                                                                            as is_window_edge_hour
from filled
