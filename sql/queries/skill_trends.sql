-- Origin: original
-- Q1: posting share and pay by year for the selected skills. Params: skills text[].
SELECT skill_name, year, posting_count, posting_share, salary_n, median_salary, p25_salary, p75_salary, year_postings
FROM mv_skill_year_stats
WHERE skill_name = ANY(%(skills)s)
ORDER BY skill_name, year;
