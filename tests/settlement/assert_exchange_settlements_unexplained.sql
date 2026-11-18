-- Settlement rows no attribution rule explains, stored for review
-- (dbt_test__audit). Warn, not error: an unexplained row is a finding to
-- investigate, and it must stay visible rather than be filtered away.
{{ config(severity='warn', store_failures=true) }}
select *
from {{ ref('int_exchange_settlements_attributed') }}
where attribution_source = 'unexplained'
