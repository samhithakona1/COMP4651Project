-- Origin: original
-- Q4: high-lift skill pairs, overall (year IS NULL) or for one year.
-- Params: year int or NULL, min_count int, min_lift float, limit int.
SELECT sa.skill_name AS skill_a, sb.skill_name AS skill_b, sa.category AS category_a, sb.category AS category_b,
       p.year, p.pair_count, p.n_postings, p.support, p.confidence_a_to_b, p.confidence_b_to_a, p.lift
FROM skill_pairs p
JOIN dim_skills sa ON sa.skill_id = p.skill_a_id
JOIN dim_skills sb ON sb.skill_id = p.skill_b_id
WHERE p.year IS NOT DISTINCT FROM %(year)s
  AND p.pair_count >= %(min_count)s
  AND p.lift >= %(min_lift)s
ORDER BY p.lift DESC, p.pair_count DESC
LIMIT %(limit)s;
