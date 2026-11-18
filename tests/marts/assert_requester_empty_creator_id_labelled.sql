-- creator_id e3b0c442...b855 is SHA-256 of the empty string (the RFQ had no
-- creator id). Pins it: the var really is sha256(''), every row with that
-- creator_id is labelled 'no_creator_id' and never flagged as a price
-- checker, and no other row carries that label.
select creator_id, requester_label, is_heavy_price_checker, 'bad label or flag' as problem
from {{ ref('mart_requester_activity') }}
where (creator_id = '{{ var('empty_creator_id_sha256') }}'
       and (requester_label <> 'no_creator_id' or is_heavy_price_checker))
   or (requester_label = 'no_creator_id'
       and creator_id is distinct from '{{ var('empty_creator_id_sha256') }}')
union all
select '{{ var('empty_creator_id_sha256') }}', null, null, 'var is not sha256 of empty string'
where sha256('') <> '{{ var('empty_creator_id_sha256') }}'
union all
select null, null, null, 'no row carries the no_creator_id label'
where not exists (select 1 from {{ ref('mart_requester_activity') }}
                  where requester_label = 'no_creator_id')
