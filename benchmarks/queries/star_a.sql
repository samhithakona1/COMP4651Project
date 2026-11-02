-- Origin: original
-- (a) Top 5 highest-paying skills co-occurring with Kubernetes in remote 2025 postings (>= 100 salaried postings).
SELECT s2.skill_name, count(*) AS postings,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY f.salary_avg_usd) AS median_salary
FROM dim_skills k
JOIN bridge_job_skills bk ON bk.skill_id = k.skill_id
JOIN fact_job_postings f  ON f.posting_id = bk.posting_id
JOIN dim_date d           ON d.date_id = f.date_id
JOIN bridge_job_skills b2 ON b2.posting_id = f.posting_id
JOIN dim_skills s2        ON s2.skill_id = b2.skill_id
WHERE k.skill_name = 'Kubernetes' AND s2.skill_name <> 'Kubernetes'
  AND d.year = 2025 AND f.is_remote AND f.salary_avg_usd IS NOT NULL
GROUP BY s2.skill_name
HAVING count(*) >= 100
ORDER BY median_salary DESC
LIMIT 5;
