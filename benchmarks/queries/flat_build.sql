-- Origin: original
-- Baseline arm: the same postings denormalised into one table with a skills text[] column and
-- NO secondary indexes (and no primary key), built from the star schema of the same scale.
DROP TABLE IF EXISTS raw_flat_postings;
CREATE TABLE raw_flat_postings AS
SELECT f.posting_id, f.source_type, f.external_id, f.posted_at, d.full_date, d.year, d.quarter, d.month,
       c.company_name, l.city, l.state_province, l.country, l.is_tech_hub, f.header_line,
       f.work_modality::text AS work_modality, f.experience_level::text AS experience_level, f.yoe_min,
       f.is_remote, f.salary_min_usd, f.salary_max_usd, f.salary_avg_usd, f.salary_currency, f.salary_flag,
       f.is_synthetic,
       COALESCE((SELECT array_agg(s.skill_name ORDER BY s.skill_name)
                 FROM bridge_job_skills bj JOIN dim_skills s ON s.skill_id = bj.skill_id
                 WHERE bj.posting_id = f.posting_id), '{}') AS skills
FROM fact_job_postings f
JOIN dim_date d      ON d.date_id = f.date_id
JOIN dim_companies c ON c.company_id = f.company_id
JOIN dim_locations l ON l.location_id = f.location_id;
ANALYZE raw_flat_postings;
