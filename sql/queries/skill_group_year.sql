-- Origin: original
-- Q1: median pay of postings mentioning any skill of group A vs any skill of group B, by year.
-- Params: group_a text[], group_b text[]. A posting in both groups counts in both.
WITH tagged AS (
    SELECT f.posting_id, d.year, f.salary_avg_usd,
           bool_or(s.skill_name = ANY(%(group_a)s)) AS in_a,
           bool_or(s.skill_name = ANY(%(group_b)s)) AS in_b
    FROM fact_job_postings f
    JOIN dim_date d           ON d.date_id = f.date_id
    JOIN bridge_job_skills bj ON bj.posting_id = f.posting_id
    JOIN dim_skills s         ON s.skill_id = bj.skill_id
    WHERE f.source_type = 'hn' AND NOT f.is_synthetic
    GROUP BY f.posting_id, d.year, f.salary_avg_usd
)
SELECT year, grp, count(*) AS postings, count(salary_avg_usd) AS salary_n,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY salary_avg_usd) AS median_salary
FROM (SELECT year, salary_avg_usd, 'A' AS grp FROM tagged WHERE in_a
      UNION ALL
      SELECT year, salary_avg_usd, 'B' FROM tagged WHERE in_b) g
GROUP BY year, grp
ORDER BY year, grp;
