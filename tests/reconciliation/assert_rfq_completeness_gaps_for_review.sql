-- Hours where the bot recorded fewer RFQs than the exchange sent, stored for
-- review (dbt_test__audit). Warn: the gaps are known history (the
-- end-of-hour logging bug, bot outages), and each row carries its
-- likely_cause; this keeps them visible instead of letting them pass
-- silently. Hours with no data in either log (bot_off_no_data) have no
-- measurable gap and are counted in the model, not here.
{{ config(severity='warn', store_failures=true) }}
select hour_utc, hour_local, n_rfqs_sent, n_rfqs_recorded, n_rfqs_missing, missing_share,
       n_missing_head, n_missing_mid, n_missing_tail,
       recording_stopped_sec_before_hour_end, events_stopped_sec_before_hour_end,
       next_hour_resumed_after_sec, likely_cause
from {{ ref('recon_rfq_completeness_hourly') }}
where likely_cause not in ('no_gap', 'no_gap_within_tolerance', 'bot_off_no_data')
