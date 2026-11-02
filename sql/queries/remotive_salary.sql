-- Origin: original
-- Remotive sanity check: parsed annual USD salaries in the Remotive snapshot(s).
SELECT count(*) AS postings, count(salary_avg_usd) AS salary_n,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY salary_avg_usd) AS median_salary,
       min(d.full_date) AS first_date, max(d.full_date) AS last_date
FROM fact_job_postings f JOIN dim_date d ON d.date_id = f.date_id
WHERE f.source_type = 'remotive';
