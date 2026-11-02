-- Origin: original
-- Skills with their all-time HN posting counts, for pickers.
SELECT skill_name, category, emerging_ai, sum(posting_count)::int AS postings
FROM mv_skill_year_stats
GROUP BY skill_name, category, emerging_ai
ORDER BY postings DESC;
