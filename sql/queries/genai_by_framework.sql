-- Origin: original
-- Q2: quarterly share per individual framework. Params: skills text[], from_year int.
WITH totals AS (
    SELECT d.year, d.quarter, count(*) AS postings
    FROM fact_job_postings f JOIN dim_date d ON d.date_id = f.date_id
    WHERE f.source_type = 'hn' AND NOT f.is_synthetic AND d.year >= %(from_year)s
    GROUP BY d.year, d.quarter
)
SELECT t.year, t.quarter, s.skill_name, count(bj.posting_id) AS postings,
       count(bj.posting_id)::float / max(t.postings) AS share
FROM totals t
CROSS JOIN dim_skills s
LEFT JOIN (fact_job_postings f
           JOIN dim_date d ON d.date_id = f.date_id AND f.source_type = 'hn' AND NOT f.is_synthetic
           JOIN bridge_job_skills bj ON bj.posting_id = f.posting_id)
       ON d.year = t.year AND d.quarter = t.quarter AND bj.skill_id = s.skill_id
WHERE s.skill_name = ANY(%(skills)s)
GROUP BY t.year, t.quarter, s.skill_name
ORDER BY t.year, t.quarter, s.skill_name;
