-- Origin: original
SELECT d.year, count(*) AS postings, count(f.salary_avg_usd) AS with_salary,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY f.salary_avg_usd) AS median_salary
FROM fact_job_postings f JOIN dim_date d ON d.date_id = f.date_id
WHERE f.source_type = 'hn' AND NOT f.is_synthetic
GROUP BY d.year ORDER BY d.year;
