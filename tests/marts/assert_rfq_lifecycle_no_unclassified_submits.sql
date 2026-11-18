-- Every quoting decision lands in a submit bucket: a quoting decision that
-- matches no rule ('unclassified') means a new exit_reason / error_kind /
-- submission shape upstream that needs a rule.
select submit_bucket, stage, record_schema, exit_reason, error_kind, count(*) as n
from {{ ref('fct_rfq_lifecycle') }}
where submit_bucket = 'unclassified'
   or (stage in ('submitted', 'priced_presubmit') and submit_bucket is null)
group by all
