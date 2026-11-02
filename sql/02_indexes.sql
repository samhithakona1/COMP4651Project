-- Origin: original
-- Idempotent. dim_skills.skill_name, dim_companies.company_key and dim_locations.location_key are
-- UNIQUE, so they already have indexes.

-- Reverse lookup: "all postings with skill X" (the bridge PK only serves posting -> skills).
CREATE INDEX IF NOT EXISTS ix_bridge_skill_posting ON bridge_job_skills (skill_id, posting_id);

-- FK indexes on the fact table.
CREATE INDEX IF NOT EXISTS ix_fact_date     ON fact_job_postings (date_id);
CREATE INDEX IF NOT EXISTS ix_fact_company  ON fact_job_postings (company_id);
CREATE INDEX IF NOT EXISTS ix_fact_location ON fact_job_postings (location_id);

-- Modality-by-period salary aggregates (Q3 and the dashboard): index-only scans.
CREATE INDEX IF NOT EXISTS ix_fact_modality_date_salary
    ON fact_job_postings (work_modality, date_id) INCLUDE (salary_avg_usd)
    WHERE salary_avg_usd IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_fact_remote_salary
    ON fact_job_postings (is_remote, salary_avg_usd)
    WHERE salary_avg_usd IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_dim_date_year_month ON dim_date (year, month);

CREATE INDEX IF NOT EXISTS ix_skill_pairs_year_lift ON skill_pairs (year, lift DESC);
