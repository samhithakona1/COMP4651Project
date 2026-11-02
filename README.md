# Tech Hiring Trends & Skill-Premium Analytics Platform

COMP4651 (Cloud Computing and Big Data Systems), HKUST, Fall 2026 · Group 14 · track W1.

An end-to-end pipeline over Hacker News “Ask HN: Who is hiring?” threads (2011 onwards) plus a
Remotive remote-jobs snapshot, answering four questions:

1. **Skill premium** — pay and posting volume for Rust / Go / Kubernetes vs PHP / Ruby on Rails / Java Spring.
2. **GenAI shift** — quarterly share of postings naming vLLM, LangChain, LlamaIndex, PyTorch, Hugging Face or Triton since 2023, and their pay premium.
3. **Remote penalty** — Remote vs Hybrid vs Onsite pay, controlling for skills, seniority and year.
4. **Skill clusters** — pairwise co-occurrence (support, confidence, lift) and Louvain clusters over time.

Every number used in the report or slides is produced by a script in this repo into `results/`
(see [Where the numbers come from](#where-the-numbers-come-from)); this README deliberately quotes none.

## Architecture

```
            ┌──────────────────────────── Airflow 2.10 (LocalExecutor) ─────────────────────────────┐
 HN Firebase│ find_thread → extract_hn → extract_remotive → validate_quality → spark_transform →    │
 HN Algolia │                                                    load_postgres → refresh_aggregates│
 Remotive   └──────┬──────────────────────────────────────────────────┬──────────────┬────────────┘
                   ▼                                                  ▼              ▼
   data/raw/source=*/year=*/month=*/part-0.jsonl  ──Spark 3.5──►  data/processed/*.parquet ──COPY──► PostgreSQL 16
   (atomic, idempotent partitions + _stats.json)   silver → gold   (star schema + bridge)            star schema, MVs
                                                                                                         │
                                                         analysis/*.py → results/*.json, *.csv           ▼
                                                                                              Streamlit dashboard
```

| Tier | Technology | Role here |
|---|---|---|
| Ingest | Apache Airflow 2.10.5, LocalExecutor | monthly DAG (2nd of each month, 06:00 UTC), retries with exponential backoff, quality gate, triggers Spark, load and MV refresh |
| Process | Apache Spark 3.5.4 (PySpark, local mode) | HTML cleaning, header parsing, salary parsing, modality/seniority/location, skill extraction, co-occurrence + lift |
| Store | PostgreSQL 16 | star schema (`fact_job_postings`, 4 dims, `bridge_job_skills`), `skill_pairs`, model coefficients, 2 materialized views |
| Serve | Streamlit 1.39 | 5 pages + home, pooled parameterised queries, salary estimator |

## Setup

Requirements: Docker Desktop (Compose v2) with **at least 8 GB** assigned to its VM, `make`, and
`python3` on the host (standard library only, for the memory profiler). Everything else runs in
containers with pinned versions (`docker/airflow/requirements.txt` + the official Airflow 2.10.5
constraints file, `docker/streamlit/requirements.txt`, pinned base images).

```bash
cp .env.example .env        # then set real passwords / secret key (never commit .env)
make all                    # build → up → backfill → transform → load → analysis → dashboard
```

`make all` runs, in order:

| Step | Command | What it does |
|---|---|---|
| 1 | `make up` | builds images, starts Postgres, Airflow (webserver :8080, scheduler) and Streamlit (:8501). On a new volume `docker/postgres/00_init.sh` creates the Airflow DB and applies `sql/01–04`. |
| 2 | `make backfill` | `python -m src.ingest.backfill --from 2011-01 --to 2026-10` (Algolia), quality gate per month |
| 3 | `make transform` | `spark-submit src/spark/build_dims.py --all` |
| 4 | `make load` | `python -m src.load.load_postgres --refresh` |
| 5 | `make analysis` | salary model, findings, label sample, extraction evaluation |
| 6 | `make dashboard` | http://localhost:8501 |

Other targets: `make down`, `make clean` (also deletes volumes), `make schema` (re-apply the
idempotent DDL), `make ingest MONTH=2026-10` (trigger the DAG), `make dag-test MONTH=2026-10`
(run the whole DAG synchronously), `make test`, `make dashboard-test`, `make bench`, `make figures`.
Airflow login: `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` from `.env`.

## Pipeline details

### Ingestion (`src/ingest/`, `dags/`)
- **Thread finder**: one Algolia request lists every story by `whoishiring`; titles must match
  `Ask HN: Who is hiring? (<Month> <YYYY>)` exactly (case-insensitive), which excludes “Who wants to
  be hired?”, “Freelancer?” and “remote workers” threads. Fallbacks: the `_whoishiring` account, then a
  full-text search requiring the exact title and ≥ 20 comments. Months with no qualifying thread are
  recorded, not invented (`results/ingest/backfill_summary.json`).
- **Monthly DAG** (`tech_hiring_ingest_pipeline`): HN Firebase API, top-level `kids` only; deleted,
  dead and text-less items are dropped and counted per reason. Target month = the run's logical-date
  month (the run on the 2nd of M+1 ingests month M once its thread is complete) or `conf.month`.
- **Backfill**: Algolia `search_by_date` on `comment,story_<id>`, keeping `parent_id == story_id`.
  Algolia lets a query page through at most 1000 hits, so threads are queried in created_at windows
  shaped like a thread's life (2 d / 7 d / 35 d / rest), each split in half while it exceeds the cap.
- **Politeness**: descriptive User-Agent (`HTTP_CONTACT` in `.env`), token buckets (Firebase 40 req/s,
  Algolia 10 req/s), pooled aiohttp (`limit=50`, DNS cache 300 s), retries on 429/5xx/timeouts with
  `min(16, 0.5·2^attempt) + U(0,1)` s and `Retry-After`, on-disk cache (`data/cache/`).
- **Remotive**: public API, **no key required**; `REMOTIVE_CATEGORY` (default `software-dev`) is only a
  filter. Remotive asks for infrequent calls and attribution and delays its listings; the client fetches
  at most once per UTC day (cached), the DAG attaches the snapshot only to a recent target month, and the
  dashboard credits Remotive. It is a sanity check for remote pay, never the main corpus.
- **Raw lake**: `data/raw/source={hn|remotive}/year=YYYY/month=MM/part-0.jsonl`, written to a hidden temp
  file and renamed (atomic); a re-run replaces exactly one partition. `_stats.json` beside it records
  fetched / kept / dropped counts.
- **Quality gate**: fails on a missing or empty file or > 5 % malformed rows (bad JSON, missing `id` or
  `text`); warns on a > 60 % change vs the previous month; writes `results/ingest/<YYYY-MM>.json`.

### Processing (`src/spark/`, `src/common/`)
- Raw → **silver** (`data/processed/_silver/postings`, partitioned by source/year/month, dynamic
  overwrite: only the months being processed are re-parsed) → **gold** star-schema Parquet rebuilt from
  all of silver (cheap, no regexes), so dims and co-occurrence always cover the full corpus.
- All rules live in `src/common/patterns.py`, `src/common/salary.py` and `src/spark/taxonomy.json`,
  shared verbatim by Spark and the Pandas baseline. Patterns use the common subset of Java and Python
  regex (Python compiled with `re.ASCII`); `tests/test_parity_real.py` checks that Spark and Pandas
  produce identical columns on a real month.
- Cleaning, header, company, location, modality, seniority and skills use built-in Spark SQL functions
  (`regexp_replace`, `regexp_extract`, `rlike`). Salary parsing needs multi-candidate context logic and
  runs as a vectorised **pandas UDF**.
- **Salary rules**: ranges and single values; a `k` on either bound applies to both (`$140-180k` →
  140,000–180,000, the bug in the report's regex); hourly × 2080; monthly × 12; non-USD → NULL with the
  currency recorded (an explicit code such as `CAD` beats a bare `$`); funding / revenue / valuation /
  bonus / 401k contexts ignored; < $30k or > $1.2M → NULL with `salary_flag = out_of_range`. Bare
  `90-150k` is accepted only in the header line. 69 test cases in `tests/test_salary_parser.py`.
- **Skills**: 196 skills in 5 categories; ambiguous names (Go, Rust, Chef, Ember, Swift, Spark, Ray,
  Triton, Julia, R, C) need context patterns. Default mode compiles the taxonomy into one `rlike` per
  skill inside the physical plan; `--skill-mode udf` broadcasts the compiled taxonomy to a pandas UDF.
- **Co-occurrence**: explode, self-join on `posting_id` with `skill_a < skill_b`, broadcast single-skill
  counts; `support = n_ab/N`, `confidence`, `lift = N·n_ab/(n_a·n_b)` with **N = postings with ≥ 1 skill**
  (postings without any taxonomy skill carry no co-occurrence information). Minimum `n_ab ≥ 30`
  (`--min-pair-count`), overall and per year, real HN postings only.
- Surrogate keys are `xxhash64` of natural keys, so they are stable across re-runs.

### Warehouse (`sql/`, `src/load/`)
- `sql/01_schema.sql` (enums, dims, fact with `salary_avg_usd` GENERATED STORED and
  `UNIQUE (source_type, external_id)`, bridge, `skill_pairs` with `UNIQUE NULLS NOT DISTINCT (a, b, year)`,
  `salary_model_coefficients`), `02_indexes.sql`, `03_dim_date_seed.sql`, `04_views.sql`
  (`mv_skill_year_stats`, `mv_modality_quarter_stats`, refreshed `CONCURRENTLY` by the DAG). All idempotent.
- Loader: pyarrow record batches → CSV → `COPY` into TEMP staging → `INSERT … ON CONFLICT DO UPDATE`;
  dims and facts commit per 50k-row batch; the derived sets (bridge, skill pairs) are replaced in one
  transaction each, deleting stale rows. `ANALYZE` afterwards. Re-loads are idempotent (`tests/test_loader.py`).
- Postgres settings for a small box (`docker-compose.yml`): `shared_buffers=256MB` (a quarter of the
  768 MB container limit), `work_mem=16MB` (per sort/hash node, sized for the percentile aggregates
  without risking many concurrent dashboard queries), `effective_cache_size=768MB` (planner hint matching
  the container's memory), `maintenance_work_mem=128MB` (index builds during loads).

### Analysis (`analysis/`)
- `fit_salary_model.py`: OLS on `ln(salary_avg_usd)` with top-30 skill dummies (plus every skill named
  in Q1/Q2 with ≥ 30 salaried postings), modality dummies (base **Hybrid**), experience dummies (base
  **Mid**), year fixed effects; HC1 robust SEs (statsmodels). Sample: HN postings with an in-range annual
  USD salary (hourly contract rates excluded). Writes `results/salary_model.json` (coefficients, HC1
  covariance, residual s.d.) and the `salary_model_coefficients` table.
- `findings.py`: every headline number for Q1–Q4 → `results/findings.json` and `results/findings/*.csv`.
- `label_sample.py` → `results/labels/label_sample.csv` (200 random postings); `eval_extraction.py` →
  per-skill precision/recall once the `true_skills` column has been labelled by hand.
- `make_figures.py` → `results/figures/*.png` and the benchmark summary tables.

### Dashboard (`app/`)
Home (volume), **Skill trends** (Q1), **GenAI** (Q2), **Remote penalty** (Q3, raw medians plus the
regression-adjusted gap with 95 % CIs), **Skill network** (Q4, force-directed graph with a year
slider), **Salary estimator** (`exp(x′β)` from the fitted model with the latest year effect, 95 % CI
for the mean and a 95 % prediction interval, n_obs, R²; no literals in UI code). Connections come from a
`ThreadedConnectionPool(2, 10)` (`@st.cache_resource`), queries are named files in `sql/queries/` run
with psycopg2 parameters and cached with `@st.cache_data(ttl=3600)`.

## Benchmarks (`benchmarks/`, `make bench`)
Every row records hardware, versions, git commit (`uncommitted` if run before the first commit) and
`is_synthetic`.
- `synth.py`: resamples real HN postings **with replacement** (seed 4651) to 10k / 50k / 200k / 1M with
  new ids — raw JSONL for Benchmark 1, star-schema Parquet for Benchmark 2. The real corpus is the
  non-synthetic scale point `real`.
- **Benchmark 1** (`spark_vs_pandas.py`): identical workload (raw JSONL → clean → salary → skills →
  co-occurrence with lift → Parquet). Each (engine, scale) runs in a fresh `bench` container with a hard
  memory limit (`BENCH_MEM_LIMIT`, default 6 GB); an OOM kill is a recorded outcome. Protocol: 1 warm-up
  + 5 measured runs at 10k / 50k / real; at 200k and 1M one measured run per engine because a single
  Pandas run there takes tens of minutes (`BENCH_FULL=1` restores the full protocol; each CSV row
  states its protocol). Peak RSS = psutil, 200 ms samples, whole process tree (Spark driver JVM + Python
  workers). Wall-clock includes Spark start-up.
- **Benchmark 2** (`query_latency.py`): baseline = the same data in one denormalised table
  `raw_flat_postings` (`skills text[]`, no indexes, no PK) vs the indexed star schema, same engine, per
  scale in schema `bench_<scale>`. Workloads: (a) top-5 highest-paying skills co-occurring with
  Kubernetes in remote 2025 postings with ≥ 100 postings; (b) yearly median pay for Rust; (c) remote vs
  hybrid median by quarter. Warm: 5 warm-ups + 50 timed runs. Cold (`run_cold.sh`): before **each**
  measurement Postgres is restarted and the Docker VM page cache dropped; `COLD_RUNS` (default 10)
  per combination, so cold p99 is close to the maximum of 10 samples. `EXPLAIN (ANALYZE, BUFFERS)` for
  every scale/arm/workload in `results/bench/explain/` (unedited).
- **Memory** (`memory_profile.py`, host): `docker stats` ~1/s during idle, ingest (12 months re-fetched),
  transform, load and dashboard use (every page executed by `app/smoke_test.py`), peak per service and
  peak total.

## Tests
`make test` (pytest inside the runner container; Spark tests use a `local[2]` session):
salary parser (69 cases), skills (aliases, false positives, Spark-vs-Python parity, UDF mode),
modality / seniority / company / location, co-occurrence (hand-computed exact values, no (a,a) or (b,a)),
quality gate, loader idempotency (needs Postgres), extraction metrics, and Spark-vs-Pandas parity on
one real month. `make dashboard-test` executes every dashboard page against the live warehouse.

## Where the numbers come from

| Number | File | Produced by |
|---|---|---|
| Threads found / missing, comments fetched, postings kept | `results/ingest/backfill_summary.json`, `results/ingest/<month>.json` | `src.ingest.backfill`, quality gate |
| Postings parsed, with salary, with ≥ 2 skills, table sizes | `results/processing/build_dims_summary.json`, `results/load/*.json` | `build_dims.py`, loader |
| Q1–Q4 headline findings | `results/findings.json`, `results/findings/*.csv` | `analysis/findings.py` |
| Salary model coefficients | `results/salary_model.json`, `results/salary_model_coefficients.csv` | `analysis/fit_salary_model.py` |
| Extraction precision / recall | `results/extraction_eval.json` | `analysis/eval_extraction.py` (after manual labels) |
| Benchmark 1 | `results/bench/spark_vs_pandas.csv`, `…_summary.csv`, `summary.json` | `spark_vs_pandas.py`, `make_figures.py` |
| Benchmark 2 | `results/bench/query_latency.csv`, `…_summary.csv`, `explain/` | `query_latency.py`, `run_cold.sh` |
| Memory | `results/bench/memory_profile.{csv,json}` | `memory_profile.py` |
| Charts | `results/figures/*.png` | `analysis/make_figures.py` |

## Reused vs original code

All source files carry an `# Origin:` header. No third-party source code was copied; the project
*uses* libraries (Airflow, PySpark, pandas, pyarrow, psycopg2, aiohttp, statsmodels, networkx,
Streamlit, Plotly, matplotlib) through their public APIs.

| File(s) | Origin |
|---|---|
| `docker-compose.yml`, `docker/*/Dockerfile`, `docker/postgres/00_init.sh`, `Makefile` | original (uses the official `apache/airflow`, `postgres` and `python` images and Airflow's published constraints file) |
| `dags/tech_hiring_ingest_pipeline.py` | original |
| `src/common/*` (config, log, paths, patterns, salary, taxonomy, db, salary_model) | original |
| `src/ingest/*` (http_base, hn_client, algolia_client, remotive_client, thread_finder, extract, backfill, quality_gate) | original |
| `src/spark/*` (session, clean_parse, skills, cooccurrence, build_dims), `src/spark/taxonomy.json` | original |
| `src/load/load_postgres.py`, `src/baseline/pandas_pipeline.py` | original |
| `sql/01_schema.sql` | original, following the star schema described in the Group 14 report plus the §5 additions |
| `sql/02–04*.sql`, `sql/queries/*.sql`, `benchmarks/queries/*.sql` | original |
| `analysis/*`, `benchmarks/*`, `app/*`, `tests/*` | original |
| Chart colours (`app/style.py`, `analysis/make_figures.py`) | palette values from a validated categorical reference palette; chart code original |

## Limitations
- **Single node.** Spark runs in local mode inside one container; the benchmarks measure single-machine
  parallelism, not cluster scaling. Points above the real corpus are synthetic resamples (duplicated
  texts), which flatters any engine that caches.
- **Regex extraction.** Skills, salary, modality, seniority and location come from hand-written patterns;
  they miss unusual phrasings and can misfire on ambiguous words despite context rules. Precision/recall
  needs the 200-posting manual labelling (`results/labels/label_sample.csv`). Location covers a fixed list
  of hubs plus a few countries.
- **Salary coverage.** Few HN postings state pay before pay-transparency laws (2022+), so pay analyses for
  early years rest on small samples; medians with < 20 salaried postings are suppressed. Posted ranges are
  not realised pay; the regression describes postings, not causal effects.
- **Append-only lake.** Raw partitions are replaced per month, but the warehouse upserts: a posting
  deleted upstream after it was loaded stays in the fact table. No time travel / ACID table format
  (Delta Lake or Iceberg would add this).
- **Remotive** exposes only a small, delayed snapshot of current jobs; it cannot be backfilled and is used
  only as a sanity check.
- **Thread coverage.** January–March 2011 have no qualifying “Who is hiring?” thread and are absent.
