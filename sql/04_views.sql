-- Origin: original
-- Materialized views behind the dashboard. Real HN postings only (Remotive is a sanity-check source,
-- synthetic rows exist only in bench_* schemas). Refreshed CONCURRENTLY by the DAG's last task,
-- which needs the unique indexes below.

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_skill_year_stats AS
WITH base AS (
    SELECT f.posting_id, d.year, f.salary_avg_usd
    FROM fact_job_postings f
    JOIN dim_date d ON d.date_id = f.date_id
    WHERE f.source_type = 'hn' AND NOT f.is_synthetic
),
year_totals AS (
    SELECT year, count(*) AS year_postings,
           count(salary_avg_usd) AS year_salaried
    FROM base GROUP BY year
)
SELECT s.skill_id,
       s.skill_name,
       s.category,
       s.emerging_ai,
       b.year,
       count(*)                                                AS posting_count,
       count(*)::double precision / max(t.year_postings)       AS posting_share,
       count(b.salary_avg_usd)                                 AS salary_n,
       percentile_cont(0.5)  WITHIN GROUP (ORDER BY b.salary_avg_usd) AS median_salary,
       percentile_cont(0.25) WITHIN GROUP (ORDER BY b.salary_avg_usd) AS p25_salary,
       percentile_cont(0.75) WITHIN GROUP (ORDER BY b.salary_avg_usd) AS p75_salary,
       max(t.year_postings)                                    AS year_postings
FROM base b
JOIN bridge_job_skills bj ON bj.posting_id = b.posting_id
JOIN dim_skills s         ON s.skill_id = bj.skill_id
JOIN year_totals t        ON t.year = b.year
GROUP BY s.skill_id, s.skill_name, s.category, s.emerging_ai, b.year
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_skill_year ON mv_skill_year_stats (skill_id, year);
CREATE INDEX IF NOT EXISTS ix_mv_skill_year_name ON mv_skill_year_stats (skill_name, year);

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_modality_quarter_stats AS
SELECT d.year,
       d.quarter,
       f.work_modality,
       count(*)                AS posting_count,
       count(f.salary_avg_usd) AS salary_n,
       percentile_cont(0.5)  WITHIN GROUP (ORDER BY f.salary_avg_usd) AS median_salary,
       percentile_cont(0.25) WITHIN GROUP (ORDER BY f.salary_avg_usd) AS p25_salary,
       percentile_cont(0.75) WITHIN GROUP (ORDER BY f.salary_avg_usd) AS p75_salary
FROM fact_job_postings f
JOIN dim_date d ON d.date_id = f.date_id
WHERE f.source_type = 'hn' AND NOT f.is_synthetic
GROUP BY d.year, d.quarter, f.work_modality
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_modality_quarter
    ON mv_modality_quarter_stats (year, quarter, work_modality);
