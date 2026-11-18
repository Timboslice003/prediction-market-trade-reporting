-- Dagster: shown as an asset check on stg_rfq_legs (orchestration/, docs/ORCHESTRATION.md).
{{ config(meta={'dagster': {'ref': {'name': 'stg_rfq_legs'}}}) }}
-- Cheap stand-in for unique(rfq_outcome_id, leg_index) on stg_rfq_legs (the
-- exact check is tagged full_rebuild_only). For every outcome with legs, the
-- legs table must hold exactly len(legs) rows, indexed 1..len(legs) (min, max
-- and sum of leg_index all agree). Outcomes are matched on (_source_key,
-- _line_no), the source line behind rfq_outcome_id: an integer-keyed group-by
-- instead of a string-pair distinct over every leg. Not a strict proof of
-- uniqueness (e.g. 1,1,4,4 would pass min/max/count/sum); the full test is.
-- Returns failing outcomes.

with outcomes as (

    select _source_key, _line_no, rfq_outcome_id, len(legs) as n_legs
    from {{ ref('stg_rfq_outcomes') }}
    where len(legs) > 0

),

legs as (

    select
        _source_key,
        _line_no,
        count(*)            as n_rows,
        min(leg_index)      as min_index,
        max(leg_index)      as max_index,
        sum(leg_index)      as sum_index
    from {{ ref('stg_rfq_legs') }}
    group by _source_key, _line_no

)

select
    coalesce(outcomes._source_key, legs._source_key) as _source_key,
    coalesce(outcomes._line_no, legs._line_no)       as _line_no,
    outcomes.rfq_outcome_id,
    outcomes.n_legs,
    legs.n_rows,
    legs.min_index,
    legs.max_index,
    legs.sum_index
from outcomes
full outer join legs
    on  legs._source_key = outcomes._source_key
    and legs._line_no = outcomes._line_no
where outcomes.n_legs is null
   or legs.n_rows is null
   or legs.n_rows    <> outcomes.n_legs
   or legs.min_index <> 1
   or legs.max_index <> outcomes.n_legs
   or legs.sum_index <> outcomes.n_legs * (outcomes.n_legs + 1) // 2
