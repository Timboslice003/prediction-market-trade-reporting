-- int_fills_graded has exactly the fills of the settled file: as many as the
-- generator wrote (on the original data, a pinned, hand-verified count).
select count(*) as n_rows, {{ expected('fills_settled_rows') }} as n_expected
from {{ ref('int_fills_graded') }}
having count(*) <> {{ expected('fills_settled_rows') }}
