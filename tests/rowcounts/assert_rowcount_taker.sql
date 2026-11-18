-- Dagster: shown as an asset check on stg_taker_plays (orchestration/, docs/ORCHESTRATION.md).
{{ config(meta={'dagster': {'ref': {'name': 'stg_taker_plays'}}}) }}
-- The taker ledgers are not in the extract manifest, so their row counts are
-- pinned to the files as written (plays: data rows, header excluded; log:
-- lines).
with t as (
    select 'plays' as what, strategy, count(*) as n from {{ ref('stg_taker_plays') }} group by all
    union all
    select 'log', strategy, count(*) from {{ ref('stg_taker_log') }} group by all
),

expected as (
    select * from (values
        ('plays', 'taker_a', {{ expected('taker_a_plays') }}),
        ('plays', 'taker_b', {{ expected('taker_b_plays') }}),
        ('log', 'taker_a', {{ expected('taker_a_log') }}),
        ('log', 'taker_b', {{ expected('taker_b_log') }})
    ) as e(what, strategy, n)
)

select e.what, e.strategy, e.n as expected_rows, t.n as staged_rows
from expected as e
full outer join t using (what, strategy)
where t.n is distinct from e.n
