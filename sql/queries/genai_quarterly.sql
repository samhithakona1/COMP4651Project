-- Origin: original
-- Q2: quarterly share of HN postings mentioning any of the given skills, and their median pay
-- vs all other postings. Params: skills text[], from_year int.
WITH p AS (
    SELECT f.posting_id, d.year, d.quarter, f.salary_avg_usd,
           EXISTS (SELECT 1 FROM bridge_job_skills bj JOIN dim_skills s ON s.skill_id = bj.skill_id
                   WHERE bj.posting_id = f.posting_id AND s.skill_name = ANY(%(skills)s)) AS is_genai
    FROM fact_job_postings f
    JOIN dim_date d ON d.date_id = f.date_id
    WHERE f.source_type = 'hn' AND NOT f.is_synthetic AND d.year >= %(from_year)s
)
SELECT year, quarter,
       count(*)                                              AS postings,
       count(*) FILTER (WHERE is_genai)                      AS genai_postings,
       count(*) FILTER (WHERE is_genai)::float / count(*)    AS genai_share,
       count(salary_avg_usd) FILTER (WHERE is_genai)         AS genai_salary_n,
       count(salary_avg_usd) FILTER (WHERE NOT is_genai)     AS other_salary_n,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY salary_avg_usd) FILTER (WHERE is_genai)     AS genai_median,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY salary_avg_usd) FILTER (WHERE NOT is_genai) AS other_median
FROM p
GROUP BY year, quarter
ORDER BY year, quarter;
