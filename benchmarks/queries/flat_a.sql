-- Origin: original
SELECT sk AS skill_name, count(*) AS postings,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY salary_avg_usd) AS median_salary
FROM raw_flat_postings, unnest(skills) AS sk
WHERE 'Kubernetes' = ANY(skills) AND sk <> 'Kubernetes'
  AND year = 2025 AND is_remote AND salary_avg_usd IS NOT NULL
GROUP BY sk
HAVING count(*) >= 100
ORDER BY median_salary DESC
LIMIT 5;
