-- Origin: original
-- (b) Yearly median posted pay for one skill (Rust).
SELECT d.year, count(*) AS postings,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY f.salary_avg_usd) AS median_salary
FROM dim_skills s
JOIN bridge_job_skills bj ON bj.skill_id = s.skill_id
JOIN fact_job_postings f  ON f.posting_id = bj.posting_id
JOIN dim_date d           ON d.date_id = f.date_id
WHERE s.skill_name = 'Rust' AND f.salary_avg_usd IS NOT NULL
GROUP BY d.year
ORDER BY d.year;
