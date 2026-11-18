-- rfq_id repeats in stg_rfq_outcomes only where two bot instances ran
-- concurrently (vars dual_instance_start_utc .. dual_instance_end_utc): each
-- repeated rfq_id exactly twice, all received inside that window, and as many
-- of them as were planted. Any other repeat fails this test.
with repeats as (

    select
        rfq_id,
        count(*)                as n,
        min(received_at_utc)    as first_received,
        max(received_at_utc)    as last_received
    from {{ ref('stg_rfq_outcomes') }}
    where n_decisions_for_rfq > 1
    group by rfq_id

),

summary as (

    select
        count(*)                                                    as n_rfq_ids,
        count(*) filter (where n <> 2)                              as n_not_twice,
        count(*) filter (where first_received < timestamp '{{ var("dual_instance_start_utc") }}'
                            or last_received >= timestamp '{{ var("dual_instance_end_utc") }}' + interval 1 minute)
                                                                    as n_outside_window
    from repeats

)

select *
from summary
where n_rfq_ids <> {{ expected('doubled_rfqs') }}
   or n_not_twice > 0
   or n_outside_window > 0
