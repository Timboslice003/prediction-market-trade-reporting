-- Exactly one counted decision per doubled RFQ (the RFQs decided twice in the
-- dual-instance window), and the filled instance is the counted one when
-- there is one. Checked on the doubled RFQs only, which keeps it cheap.
with d as (
    select rfq_id, is_counted_decision, is_filled
    from {{ ref('fct_rfq_lifecycle') }}
    where n_decisions_for_rfq > 1
)
select rfq_id
from d
group by rfq_id
having count(*) filter (where is_counted_decision) <> 1
    or (bool_or(is_filled) and not bool_or(is_counted_decision and is_filled))
