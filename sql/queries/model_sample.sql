-- Origin: original
-- Salary-model sample: real HN postings with an in-range annual USD salary (hourly rates excluded).
SELECT f.posting_id, d.year, f.work_modality::text AS work_modality, f.experience_level::text AS experience_level,
       f.salary_avg_usd::float AS salary
FROM fact_job_postings f JOIN dim_date d ON d.date_id = f.date_id
WHERE f.source_type = 'hn' AND NOT f.is_synthetic
  AND f.salary_avg_usd IS NOT NULL AND f.salary_currency = 'USD' AND f.salary_flag = 'ok';
