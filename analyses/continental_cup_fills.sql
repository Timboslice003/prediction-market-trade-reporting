-- Ad-hoc question, answered with the fills_for macro: every fill with at least
-- one leg on a Soccer Continental Cup market, traded on reporting days
-- 2025-03-11..2025-03-19.
--   dbt compile -s continental_cup_fills     (SQL in target/compiled/...)
-- assert_fills_for_matches_direct_query checks the macro against a
-- hand-written query.
{{ fills_for(competition='Soccer Continental Cup', date_from='2025-03-11', date_to='2025-03-19') }}
