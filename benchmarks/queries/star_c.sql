-- Origin: original
-- (c) Remote vs Hybrid median posted pay by quarter.
SELECT d.year, d.quarter, f.work_modality, count(*) AS postings,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY f.salary_avg_usd) AS median_salary
FROM fact_job_postings f
JOIN dim_date d ON d.date_id = f.date_id
WHERE f.work_modality IN ('Remote', 'Hybrid') AND f.salary_avg_usd IS NOT NULL
GROUP BY d.year, d.quarter, f.work_modality
ORDER BY d.year, d.quarter, f.work_modality;
