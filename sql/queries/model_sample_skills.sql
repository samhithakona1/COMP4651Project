-- Origin: original
-- (posting_id, skill) rows for the salary-model sample.
SELECT bj.posting_id, s.skill_name
FROM bridge_job_skills bj
JOIN dim_skills s ON s.skill_id = bj.skill_id
JOIN fact_job_postings f ON f.posting_id = bj.posting_id
WHERE f.source_type = 'hn' AND NOT f.is_synthetic
  AND f.salary_avg_usd IS NOT NULL AND f.salary_currency = 'USD' AND f.salary_flag = 'ok';
