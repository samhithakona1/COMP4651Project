-- Origin: original
SELECT year, count(*) AS postings,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY salary_avg_usd) AS median_salary
FROM raw_flat_postings
WHERE 'Rust' = ANY(skills) AND salary_avg_usd IS NOT NULL
GROUP BY year
ORDER BY year;
