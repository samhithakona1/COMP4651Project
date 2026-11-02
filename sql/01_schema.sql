-- Origin: original (follows the star schema described in the Group 14 report, plus the §5 additions)
-- Idempotent: safe to re-run. Objects are created in the current search_path, so the benchmark
-- can apply the same DDL to its own schemas (bench_*).

DO $$ BEGIN
  CREATE TYPE work_modality_enum AS ENUM ('Remote', 'Hybrid', 'Onsite', 'Unspecified');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
  CREATE TYPE experience_level_enum AS ENUM ('Junior', 'Mid', 'Senior', 'Staff_Principal', 'Unspecified');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

CREATE TABLE IF NOT EXISTS dim_date (
    date_id      INTEGER PRIMARY KEY,           -- yyyymmdd
    full_date    DATE     NOT NULL UNIQUE,
    year         SMALLINT NOT NULL,
    quarter      SMALLINT NOT NULL CHECK (quarter BETWEEN 1 AND 4),
    month        SMALLINT NOT NULL CHECK (month BETWEEN 1 AND 12),
    day          SMALLINT NOT NULL,
    day_of_week  SMALLINT NOT NULL CHECK (day_of_week BETWEEN 1 AND 7),  -- ISO, 1 = Monday
    is_weekend   BOOLEAN  NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_companies (
    company_id    BIGINT PRIMARY KEY,           -- xxhash64(company_key), stable across runs
    company_key   VARCHAR(200) NOT NULL UNIQUE, -- normalised natural key
    company_name  VARCHAR(200) NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_locations (
    location_id     BIGINT PRIMARY KEY,         -- xxhash64(location_key)
    location_key    VARCHAR(200) NOT NULL UNIQUE,
    city            VARCHAR(100),
    state_province  VARCHAR(100),
    country         VARCHAR(100),
    is_tech_hub     BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS dim_skills (
    skill_id     BIGINT PRIMARY KEY,            -- xxhash64(skill_name)
    skill_name   VARCHAR(100) NOT NULL UNIQUE,
    category     VARCHAR(50)  NOT NULL,
    emerging_ai  BOOLEAN      NOT NULL DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS fact_job_postings (
    posting_id        BIGINT PRIMARY KEY,       -- xxhash64(source_type || ':' || external_id)
    source_type       VARCHAR(16)  NOT NULL,    -- 'hn' | 'remotive'
    external_id       VARCHAR(64)  NOT NULL,
    thread_id         BIGINT,
    date_id           INTEGER NOT NULL REFERENCES dim_date (date_id),
    company_id        BIGINT  NOT NULL REFERENCES dim_companies (company_id),
    location_id       BIGINT  NOT NULL REFERENCES dim_locations (location_id),
    posted_at         TIMESTAMPTZ NOT NULL,
    header_line       VARCHAR(500),
    work_modality     work_modality_enum    NOT NULL DEFAULT 'Unspecified',
    experience_level  experience_level_enum NOT NULL DEFAULT 'Unspecified',
    yoe_min           SMALLINT,
    is_remote         BOOLEAN NOT NULL DEFAULT FALSE,
    salary_min_usd    NUMERIC(12, 2),
    salary_max_usd    NUMERIC(12, 2),
    -- Midpoint of a range; a one-sided value ("up to $220k", "$160,000 base") is used as-is.
    salary_avg_usd    NUMERIC(12, 2) GENERATED ALWAYS AS (
                          CASE WHEN salary_min_usd IS NOT NULL AND salary_max_usd IS NOT NULL
                               THEN (salary_min_usd + salary_max_usd) / 2
                               ELSE COALESCE(salary_min_usd, salary_max_usd) END) STORED,
    salary_currency   CHAR(3),
    salary_flag       VARCHAR(20),               -- ok | hourly | non_usd | out_of_range | none
    skill_count       SMALLINT NOT NULL DEFAULT 0,
    text_length       INTEGER,
    is_synthetic      BOOLEAN NOT NULL DEFAULT FALSE,
    loaded_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_fact_source_external UNIQUE (source_type, external_id),
    CONSTRAINT ck_salary_order CHECK (salary_min_usd IS NULL OR salary_max_usd IS NULL
                                      OR salary_min_usd <= salary_max_usd)
);

CREATE TABLE IF NOT EXISTS bridge_job_skills (
    posting_id  BIGINT NOT NULL REFERENCES fact_job_postings (posting_id) ON DELETE CASCADE,
    skill_id    BIGINT NOT NULL REFERENCES dim_skills (skill_id),
    PRIMARY KEY (posting_id, skill_id)
);

-- year IS NULL holds the all-years co-occurrence; NULLS NOT DISTINCT (PG15+) keeps one such row per pair.
CREATE TABLE IF NOT EXISTS skill_pairs (
    skill_a_id         BIGINT  NOT NULL REFERENCES dim_skills (skill_id),
    skill_b_id         BIGINT  NOT NULL REFERENCES dim_skills (skill_id),
    year               SMALLINT,
    pair_count         INTEGER NOT NULL,
    n_postings         INTEGER NOT NULL,        -- N: postings with >= 1 skill in that scope
    support            DOUBLE PRECISION NOT NULL,
    confidence_a_to_b  DOUBLE PRECISION NOT NULL,
    confidence_b_to_a  DOUBLE PRECISION NOT NULL,
    lift               DOUBLE PRECISION NOT NULL,
    CONSTRAINT uq_skill_pairs UNIQUE NULLS NOT DISTINCT (skill_a_id, skill_b_id, year),
    CONSTRAINT ck_pair_order CHECK (skill_a_id <> skill_b_id)
);

CREATE TABLE IF NOT EXISTS salary_model_coefficients (
    term       VARCHAR(120) PRIMARY KEY,
    coef       DOUBLE PRECISION NOT NULL,
    std_err    DOUBLE PRECISION NOT NULL,
    p_value    DOUBLE PRECISION,
    n_obs      INTEGER NOT NULL,
    fitted_at  TIMESTAMPTZ NOT NULL
);
