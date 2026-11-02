-- Origin: original
SELECT year, quarter, work_modality, count(*) AS postings,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY salary_avg_usd) AS median_salary
FROM raw_flat_postings
WHERE work_modality IN ('Remote', 'Hybrid') AND salary_avg_usd IS NOT NULL
GROUP BY year, quarter, work_modality
ORDER BY year, quarter, work_modality;
