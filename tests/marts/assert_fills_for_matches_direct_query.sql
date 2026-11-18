-- Dagster: shown as an asset check on fct_fill_legs (orchestration/, docs/ORCHESTRATION.md).
{{ config(meta={'dagster': {'ref': {'name': 'fct_fill_legs'}}}) }}
-- The fills_for macro returns exactly what the hand-written query does, at
-- both grains (Soccer Continental Cup, reporting days 2025-03-11..2025-03-19),
-- and the question is not trivially empty.
with by_macro as (
    select fill_id from ({{ fills_for(competition='Soccer Continental Cup', date_from='2025-03-11', date_to='2025-03-19') }})
),
by_hand as (
    select distinct fill_id from {{ ref('fct_fill_legs') }}
    where competition = 'Soccer Continental Cup'
      and accepted_date_local between date '2025-03-11' and date '2025-03-19'
),
legs_by_macro as (
    select count(*) as n from ({{ fills_for(competition='Soccer Continental Cup', date_from='2025-03-11', date_to='2025-03-19', grain='fill_leg') }})
),
legs_by_hand as (
    select count(*) as n from {{ ref('fct_fill_legs') }}
    where competition = 'Soccer Continental Cup'
      and accepted_date_local between date '2025-03-11' and date '2025-03-19'
)
select 'fill only in macro' as problem, fill_id from (select * from by_macro except select * from by_hand)
union all
select 'fill only in hand query', fill_id from (select * from by_hand except select * from by_macro)
union all
select 'leg count differs', null from legs_by_macro, legs_by_hand where legs_by_macro.n <> legs_by_hand.n
union all
select 'no fills match: the test proves nothing', null where not exists (select 1 from by_hand)
