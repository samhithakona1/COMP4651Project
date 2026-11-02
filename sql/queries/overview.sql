-- Origin: original
-- Headline counts for the dashboard home page (real HN postings only).
SELECT count(*)                                              AS postings,
       count(salary_avg_usd)                                 AS with_salary,
       count(*) FILTER (WHERE skill_count >= 2)              AS with_ge2_skills,
       count(DISTINCT company_id)                            AS companies,
       min(d.full_date)                                      AS first_date,
       max(d.full_date)                                      AS last_date
FROM fact_job_postings f
JOIN dim_date d ON d.date_id = f.date_id
WHERE f.source_type = 'hn' AND NOT f.is_synthetic;
