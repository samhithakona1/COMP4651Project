# CLAUDE.md — Tech Hiring Trends & Skill-Premium Analytics Platform

COMP4651 (Cloud Computing and Big Data Systems), HKUST, Fall 2026 · Group 14 · Wide project, track W1 (Data Analytics Platforms).

This file is the build spec for the whole codebase. Read it fully before writing code. Build in the milestone order at the bottom, and stop at the end of each milestone so the team can review it.

---

## 1. What we are building

An end-to-end analytics pipeline over public tech job postings (Hacker News "Who is Hiring?" threads, 2011 onwards, plus Remotive remote jobs). It must answer four questions:

1. **Skill premium:** pay and posting volume for modern systems skills (Rust, Go, Kubernetes) vs legacy stacks (PHP, Ruby on Rails, Java Spring), by year.
2. **GenAI shift:** the quarterly share of postings that mention GenAI frameworks (vLLM, LangChain, LlamaIndex, PyTorch, Hugging Face, Triton) from 2023, and their pay premium.
3. **Remote penalty:** the pay difference for Remote vs Hybrid vs Onsite, controlling for skills and seniority.
4. **Skill clusters:** pairwise co-occurrence (support, confidence, lift) between skills, and how clusters change over time.

Stack (four technologies, each with a distinct role, as track W1 requires):

| Tier | Technology | Role |
|---|---|---|
| Ingest | Apache Airflow 2.x (LocalExecutor) | Monthly ingestion DAG, retries, quality gate, triggers downstream jobs |
| Process | Apache Spark 3.5 (PySpark) | Cleaning, salary parsing, skill extraction, co-occurrence |
| Store | PostgreSQL 16 | Star schema with a bridge table, indexed for OLAP queries |
| Serve | Streamlit 1.38+ | Interactive dashboard and salary estimator |

Everything runs with Docker Compose on one machine. Target memory budget: ≤ 3.5 GB of active RAM. **Measure it** with `docker stats` and report the real figure.

### Non-negotiable rules

- **No hard-coded results.** Every number in the report or slides (row counts, speedups, latencies, salary premiums, lift values, regression coefficients) must come from a script in this repo that writes a CSV or JSON to `results/`. The salary estimator reads coefficients that `analysis/fit_salary_model.py` fitted; it does not use literals in the UI code.
- **Reproducible.** `make all` (or the documented sequence) goes from an empty machine to a running dashboard. Pin every dependency version.
- **Honest data volume.** Log real counts at every stage: comments fetched, postings kept, postings with parsed salary, and postings with ≥ 2 skills. The report's "1.5M postings" figure is unverified; HN threads usually hold a few hundred to about a thousand top-level comments a month. Use whatever the real count is. For benchmark scale points above the real count, generate synthetic data by resampling real postings with new IDs (see §7), and label it as synthetic in results.
- **Mark reused vs our own code.** The course requires this. Put a header comment in each file: `# Origin: original` or `# Origin: adapted from <source>`.
- **Be polite to the APIs.** Set a descriptive User-Agent, obey rate limits, and cache raw responses so re-runs don't refetch.

---

## 2. Repository layout

```
tech-hiring-analytics/
├── CLAUDE.md
├── README.md                    # setup, run, reproduce benchmarks, reused-vs-own code table
├── Makefile                     # up, down, ingest, backfill, transform, load, bench, test, all
├── docker-compose.yml
├── .env.example                 # PG creds, data paths, Spark memory; never commit .env
├── docker/
│   ├── airflow/Dockerfile       # Airflow + Java (for spark-submit) + pyspark
│   ├── spark/Dockerfile         # only if a separate Spark container is used
│   └── streamlit/Dockerfile
├── dags/
│   └── tech_hiring_ingest_pipeline.py
├── src/
│   ├── common/                  # config, logging, paths
│   ├── ingest/
│   │   ├── hn_client.py         # async Firebase client: rate limit, backoff, pooling
│   │   ├── algolia_client.py    # historical backfill
│   │   ├── remotive_client.py
│   │   ├── thread_finder.py     # month → "Who is Hiring?" story id
│   │   └── quality_gate.py
│   ├── spark/
│   │   ├── session.py
│   │   ├── clean_parse.py       # HTML cleaning, salary, modality, seniority, location
│   │   ├── skills.py            # taxonomy matching
│   │   ├── cooccurrence.py
│   │   ├── build_dims.py        # dims, fact, bridge → Parquet
│   │   └── taxonomy.json
│   ├── load/
│   │   └── load_postgres.py     # Parquet → PostgreSQL (COPY via psycopg, idempotent)
│   └── baseline/
│       └── pandas_pipeline.py   # same logic as the Spark pipeline, single-node Pandas
├── sql/
│   ├── 01_schema.sql
│   ├── 02_indexes.sql
│   ├── 03_dim_date_seed.sql     # or generated in Python
│   └── queries/                 # the named analytical queries the dashboard uses
├── analysis/
│   ├── fit_salary_model.py      # OLS → results/salary_model.json + coefficient table in PG
│   ├── findings.py              # computes every headline finding → results/findings.json
│   └── make_figures.py          # regenerates every chart for the report and slides
├── benchmarks/
│   ├── synth.py                 # scale real data to 10k / 50k / 200k / 1M rows
│   ├── spark_vs_pandas.py       # Benchmark 1
│   ├── query_latency.py         # Benchmark 2
│   └── memory_profile.py        # docker stats sampling during each stage
├── app/
│   ├── streamlit_app.py
│   ├── db.py                    # pooled connections, cached queries
│   └── pages/                   # trends, GenAI, remote, network, estimator
├── tests/                       # pytest
├── data/                        # git-ignored: raw/, processed/, synthetic/
└── results/                     # CSVs and JSON from benchmarks and analysis (committed)
```

---

## 3. Ingestion (Airflow) — owner: Student A

### Sources

- **HN Firebase API:** `https://hacker-news.firebaseio.com/v0/item/{id}.json`. The monthly story by user `whoishiring` titled "Ask HN: Who is hiring? (Month YYYY)" has a `kids` array; each top-level kid is one posting. Skip `deleted`, `dead`, and items without `text`. Use only top-level comments, not replies.
- **Algolia HN API:** `https://hn.algolia.com/api/v1/`, for finding story ids and for the backfill. Find the story with `search?query="Who is hiring?"&tags=story,author_whoishiring`, then match the title to the month exactly (the API also returns "Who wants to be hired?" and "Freelancer?" threads; exclude them). For comments, use `search_by_date?tags=comment,story_{id}&hitsPerPage=1000`, and keep only hits where `parent_id == story_id` (top-level). Algolia caps how many hits one query can page through, so if a thread exceeds the cap, split it with `numericFilters=created_at_i>...,created_at_i<...`.
- **Remotive API:** `https://remotive.com/api/remote-jobs`. Check its terms before use: it asks for infrequent calls and attribution, and its data is delayed. Fetch once per DAG run at most and cache the result. It is used only to sanity-check remote pay, not as the main corpus.

### Async client (`hn_client.py`)

- `aiohttp` with `TCPConnector(limit=50, ttl_dns_cache=300)` and `asyncio.Semaphore` for concurrency.
- Token-bucket rate limiter, configurable: Firebase 40 req/s by default, Algolia 10 req/s.
- Retry on 429/500/502/503/504 and timeouts: `sleep = min(16, 0.5 * 2**attempt) + uniform(0, 1)`, 5 attempts. Honour `Retry-After` if present.
- On-disk cache keyed by item id, so a re-run doesn't refetch.

### Raw lake

`data/raw/source={hn|remotive}/year=YYYY/month=MM/part-0.jsonl`, one JSON object per line, written atomically (write to temp, then rename). Re-running a month overwrites only that partition, so runs are idempotent.

### Quality gate (`quality_gate.py`)

Fail the task if the file is missing or empty, or if malformed rows (bad JSON, or missing `id` or `text`) exceed 5%. Also warn if the count differs from the previous month by more than 60%. Write a per-run JSON report to `results/ingest/{year}-{month}.json` with fetched, kept, dropped and malformed counts.

### DAG (`dags/tech_hiring_ingest_pipeline.py`)

- `schedule="0 6 2 * *"` (2nd of each month, 06:00 UTC), `catchup=False`, `max_active_runs=1`. Retries 3, exponential backoff, `max_retry_delay` 15 min.
- Tasks: `find_thread → extract_hn → extract_remotive → validate_quality → spark_transform → load_postgres → refresh_aggregates`. The report's version stopped at validate; the full chain is required here.
- `spark_transform` calls `spark-submit` (BashOperator) on `src/spark/build_dims.py` for the run's month.
- A separate `backfill_hn` DAG, or a CLI `python -m src.ingest.backfill --from 2011-01 --to 2026-10`, handles history.
- Use the TaskFlow API and XCom only for small values such as paths and counts.

---

## 4. Processing (Spark) — owner: Student B

### Session config (`session.py`)

`local[*]`, driver memory from env (default 2g), `spark.sql.shuffle.partitions=16`, `spark.sql.execution.arrow.pyspark.enabled=true`, `spark.local.dir=/data/spark-tmp` (so spills go to disk). Read the raw lake with partition columns so that date filters prune folders.

### Cleaning and parsing (`clean_parse.py`)

Prefer built-in Spark SQL functions (`regexp_extract`, `regexp_replace`) over Python UDFs, because they are faster. Where a UDF is unavoidable, use a pandas UDF.

1. **HTML:** unescape entities, replace `<p>` with a newline, strip tags, collapse whitespace.
2. **Header line:** HN postings conventionally start with `Company | Role | Location | REMOTE/ONSITE | Salary`. Split the first line on `|` to get company and location candidates. Fall back gracefully when the format differs.
3. **Salary → annual USD** `(salary_min_usd, salary_max_usd)`:
   - Ranges: `$140k - $180k`, `$140-180k`, `140,000 - 180,000 USD`, `$150K–$200K`, `$140k to $180k`.
   - Single values: `up to $220k`, `$160,000 base`, `Base: 130k`.
   - Hourly: `$85/hr`, `$100 - $125 per hour`. Multiply by 2,080.
   - A `k` on either bound applies to both (`$140-180k` means 140,000–180,000). **The report's regex got this wrong; fix it.**
   - Leave non-USD currencies (€, £, CHF, ₹) as NULL with `salary_currency` recorded. Don't convert unless an FX table is added.
   - Equity-only, `DOE`, and `competitive` → NULL.
   - Out of range (< $30k or > $1.2M per year) → NULL with a flag column.
   - Ignore "$2M seed" or "$50M Series B" funding mentions. Add a negative pattern for funding, raise and valuation context.
4. **Work modality:** `Remote | Hybrid | Onsite | Unspecified` from keywords (REMOTE, hybrid, onsite, on-site, in office) in the header first, then the body. "Remote (US only)" counts as Remote.
5. **Experience level:** `Junior | Mid | Senior | Staff_Principal | Unspecified` from title keywords (intern/junior/new grad; senior/sr; staff/principal/lead/architect) and "N+ years". Also store `yoe_min` as an integer when stated.
6. **Location:** normalise to (city, state/province, country) with a small lookup of major hubs. Set the tech-hub flag for SF Bay Area, NYC and Seattle.

### Skill taxonomy (`taxonomy.json` + `skills.py`)

```json
{ "Kubernetes": { "category": "Cloud/DevOps", "emerging_ai": false,
                  "patterns": ["\\bkubernetes\\b", "\\bk8s\\b", "\\bkube\\b"] },
  "Go":         { "category": "Languages", "emerging_ai": false,
                  "patterns": ["\\bgolang\\b", "\\bgo\\b(?=\\s*(,|/|\\)|developer|engineer|services|backend|lang))"] } }
```

- 150+ skills across Languages, Cloud/DevOps, AI/ML, Data, and Frameworks/Web. Include every skill named in §1 plus Next.js, Tailwind, Node.js, React, CUDA, JAX, Ray, ONNX, MLflow, TensorRT, Kafka, Flink, Snowflake, ClickHouse, dbt, Terraform, Prometheus, Grafana, Helm, OpenTelemetry, Elixir, Zig, Swift and Kotlin.
- Patterns are case-insensitive with word boundaries. Ambiguous words (Go, Rust, Chef, Ember, Swift, Spark, Ray, Triton, Julia) need context patterns; "trust" and "frustrated" must not match Rust.
- Broadcast the compiled taxonomy. Output `skills: array<string>`, distinct.
- Write a precision check: `analysis/label_sample.py` samples 200 postings to a CSV for manual labelling, and `analysis/eval_extraction.py` reports per-skill precision and recall. Put the numbers in the report.

### Co-occurrence (`cooccurrence.py`)

- Explode `(posting_id, skill)`, self-join on `posting_id` with `skill_a < skill_b`, and `count` per pair.
- Use broadcast single-skill counts to compute `support = n_ab/N`, `confidence_a_to_b`, `confidence_b_to_a`, and `lift = N·n_ab/(n_a·n_b)`. Use the number of postings with ≥ 1 skill as N, and document the choice.
- Apply a minimum support (n_ab ≥ 30) before writing, to filter noise. Make it a parameter.
- Also compute per-year co-occurrence, for "clusters over time".
- Clusters: run Louvain or greedy modularity (networkx) on the lift > 1 graph in `analysis/findings.py`. Report clusters with their mean pairwise lift.

### Outputs

Parquet in `data/processed/{dim_date,dim_companies,dim_locations,dim_skills,fact_job_postings,bridge_job_skills,skill_pairs,skill_pairs_yearly}/`. Generate surrogate keys deterministically (stable across re-runs), e.g. a hash of the natural key or a lookup against existing dims.

---

## 5. Warehouse (PostgreSQL) — owner: Student C

Use the DDL from the report as the starting point (`sql/01_schema.sql`): the enums `work_modality_enum` and `experience_level_enum`, plus `dim_date`, `dim_companies`, `dim_locations`, `dim_skills`, `fact_job_postings` (with `salary_avg_usd` as a GENERATED STORED column and `UNIQUE (source_type, external_id)`), and `bridge_job_skills (PK posting_id, skill_id)`. Additions:

- `fact_job_postings`: add `yoe_min SMALLINT`, `salary_currency CHAR(3)`, `salary_flag VARCHAR(20)`, and `is_synthetic BOOLEAN DEFAULT FALSE`.
- `skill_pairs(skill_a_id, skill_b_id, year NULL, pair_count, support, confidence_a_to_b, confidence_b_to_a, lift)`.
- `salary_model_coefficients(term, coef, std_err, p_value, n_obs, fitted_at)`.
- Index (`sql/02_indexes.sql`): `bridge_job_skills(skill_id, posting_id)`, FK indexes on the fact table, `fact(work_modality, date_id) INCLUDE (salary_avg_usd) WHERE salary_avg_usd IS NOT NULL`, `fact(is_remote, salary_avg_usd) WHERE salary_avg_usd IS NOT NULL`, and `dim_date(year, month)`. `dim_skills.skill_name` is UNIQUE, so it already has an index.
- Materialized views for dashboard aggregates (`mv_skill_year_stats`, `mv_modality_quarter_stats`), refreshed by the DAG's last task.
- Loader: COPY from Parquet via a pyarrow → CSV stream into staging tables, then `INSERT … ON CONFLICT DO UPDATE` into the final tables, in one transaction per batch. Run `ANALYZE` afterwards.
- Postgres config for a small box: `shared_buffers=256MB`, `work_mem=16MB`, `effective_cache_size=768MB`. Document them.

---

## 6. Dashboard (Streamlit) — owner: Student C

- `db.py`: `psycopg2.pool.ThreadedConnectionPool(2, 10)` behind `@st.cache_resource`, and query helpers behind `@st.cache_data(ttl=3600)`. All SQL is parameterised; never build SQL with f-strings from widget input.
- Pages:
  1. **Skill trends** (Q1): posting share and median pay by year for selected skills.
  2. **GenAI** (Q2): quarterly penetration and premium.
  3. **Remote penalty** (Q3): modality comparison plus the regression-adjusted gap with confidence intervals.
  4. **Skill network** (Q4): force-directed graph of high-lift pairs, with a year slider.
  5. **Salary estimator**.
- **Salary estimator, done correctly:** `analysis/fit_salary_model.py` fits OLS on `ln(salary_avg_usd)` with skill dummies (top N skills), modality dummies (Hybrid as the base), experience-level dummies, and year fixed effects, using statsmodels with robust (HC1) standard errors. It writes coefficients to `results/salary_model.json` and to `salary_model_coefficients`. The app predicts `exp(β0 + Σβ)` for the chosen inputs, using the latest year's effect. Show the 95% interval and n_obs, and note that it describes postings, not causal effects. The report's version used hand-typed additive percentages; don't reproduce that.
- Footprint target: under 350 MB steady state. Measure it.

---

## 7. Benchmarks — owners: Student B (Benchmark 1), Student C (Benchmark 2)

Every benchmark writes raw per-run rows to `results/bench/*.csv` and records hardware, software versions, the git commit, and whether the data is synthetic. `analysis/make_figures.py` draws the charts from those CSVs.

### Synthetic scaling (`benchmarks/synth.py`)

Resample real cleaned postings with replacement to 10k, 50k, 200k and 1M rows, assigning new posting ids and keeping the skill and salary distributions. Use a fixed seed.

### Benchmark 1: Spark vs Pandas (`spark_vs_pandas.py`)

- Same workload in both: read the raw JSONL, then clean, parse salary, extract skills, run co-occurrence with lift, and write Parquet. `src/baseline/pandas_pipeline.py` must reuse the same regexes and taxonomy, so the comparison is fair.
- 1 warm-up run and 5 measured runs per (engine, scale). Record median wall-clock and peak RSS (Spark: driver plus Python workers; sample with psutil every 200 ms).
- Run Pandas in a container with a memory limit (e.g. `--memory=8g`) so an OOM is a recorded outcome ("OOM at 1M with an 8 GB limit"), not a crashed laptop.
- Report speedup = Pandas time / Spark time, plus the crossover point.

### Benchmark 2: query latency (`query_latency.py`)

- **Baseline:** pick one and describe it consistently in the report. Recommended: the same data in one denormalised PostgreSQL table, `raw_flat_postings`, with a `skills text[]` column and no secondary indexes. Comparing that against the indexed star schema in the same engine isolates the schema and index design. Optionally, add Spark SQL over raw JSONL as a third arm.
- Workloads (at least 3, since the guidelines ask for multiple workloads): (a) the "top 5 highest-paying skills co-occurring with Kubernetes in remote 2025 postings, ≥ 100 postings" query from the report; (b) the yearly median pay for one skill; (c) the remote-vs-hybrid median by quarter.
- For each (arm, workload, scale): 5 warm-up runs, then 50 timed runs. Report p50, p90 and p99, separately for cold cache (after a restart) and warm cache.
- Save `EXPLAIN (ANALYZE, BUFFERS)` output for each arm and workload to `results/bench/explain/` and quote it in the report. Don't edit the plans by hand.

### Memory profile (`memory_profile.py`)

Sample `docker stats` every second during ingest, transform, load and dashboard use, and report the peak per service and the total.

---

## 8. Tests (`tests/`, pytest)

- `test_salary_parser.py`: at least 40 cases covering every format in §4.3, including k on one bound, hourly, funding false positives, non-USD and out-of-range.
- `test_skills.py`: aliases (k8s → Kubernetes), false positives (trust, frustrated, "go the extra mile", Spark as in "spark joy"), and case-insensitivity.
- `test_modality_seniority.py`.
- `test_cooccurrence.py`: a tiny hand-computed dataset that checks counts, support, confidence and lift exactly, and that no (a,a) or (b,a) duplicates appear.
- `test_quality_gate.py`.
- `test_loader.py`: re-loading the same Parquet is idempotent (row counts unchanged).
- Run Spark tests with a local session fixture (`local[2]`).

---

## 9. Milestones (stop after each for review)

1. **Skeleton:** repo layout, docker-compose (Postgres, Airflow LocalExecutor, Streamlit) with memory limits, Makefile, `.env.example`, and schema plus indexes applied on startup. `make up` works.
2. **Ingestion:** the clients, thread finder, quality gate and DAG through `validate_quality`, plus the backfill CLI. Ingest 3 real months end to end and print the counts.
3. **Processing:** cleaning, parsing, taxonomy, co-occurrence and Parquet outputs, with their unit tests. Run on the 3 months.
4. **Load and warehouse:** the loader, materialized views and the full DAG chain. Then run the full backfill and report the real totals.
5. **Analysis:** `fit_salary_model.py`, `findings.py` (one JSON with every headline number for Q1–Q4) and `label_sample.py`/`eval_extraction.py`.
6. **Dashboard:** all five pages, reading from PG and `results/`.
7. **Benchmarks:** `synth.py`, both benchmarks, the memory profile and `make_figures.py`.
8. **Docs:** the README with setup, a reproduce-everything section, a table of reused vs original code by file, and a limitations section (single node, regex extraction, append-only lake). Optionally, add Delta Lake as an extension.

## 10. Deadlines

- Slides: 8 Nov 2026, 23:59.
- Report (PDF, ≤ 5,000 words) and source code (ZIP): 1 Dec 2026, 23:59.
- Work is accepted up to 3 days late, with 10% deducted per day.

Once milestone 7 is done, update the report and slides with the real numbers from `results/`.
