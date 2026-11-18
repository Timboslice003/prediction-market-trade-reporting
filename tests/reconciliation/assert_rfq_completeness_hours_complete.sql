-- One row per UTC hour of the completeness window, and the missing split adds
-- up: head + mid + tail = missing on every hour.
{% set n_hours %}
    date_diff('hour', timestamp '{{ var("completeness_window_start_utc") }}',
                      timestamp '{{ var("completeness_window_end_utc") }}')
{% endset %}
select hour_utc, n_rfqs_missing, n_missing_head + n_missing_mid + n_missing_tail as split_total
from {{ ref('recon_rfq_completeness_hourly') }}
where n_missing_head + n_missing_mid + n_missing_tail <> n_rfqs_missing
union all
select null, (select count(*) from {{ ref('recon_rfq_completeness_hourly') }}), {{ n_hours }}
where (select count(*) from {{ ref('recon_rfq_completeness_hourly') }}) <> {{ n_hours }}
