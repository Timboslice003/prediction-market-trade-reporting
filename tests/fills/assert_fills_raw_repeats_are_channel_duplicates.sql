-- A fill_id may repeat in the raw fill log only as the same trade logged by
-- both channels: exactly two records, one REST poll (rest_poll) and one
-- websocket (ws), with the same ticker, side and count. Anything else is a
-- real duplicate or a conflicting record, and fails.
with repeats as (
    select
        fill_id,
        count(*)                                    as n_records,
        count(distinct fill_source)                 as n_channels,
        count(distinct combo_ticker || ':' || side || ':' || cast(contract_count as varchar)) as n_payloads
    from {{ ref('stg_fills_raw') }}
    group by fill_id
    having count(*) > 1
)
select * from repeats
where n_records <> 2 or n_channels <> 2 or n_payloads <> 1
