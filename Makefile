# Origin: original
# Entry points. `make all` goes from an empty machine (Docker installed) to a running dashboard.

SHELL := /bin/bash
COMPOSE := docker compose
GIT_COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo uncommitted)
HOST_LABEL ?= $(shell uname -sm)
RUN := $(COMPOSE) run --rm -e GIT_COMMIT=$(GIT_COMMIT) -e HOST_LABEL="$(HOST_LABEL)" runner
BENCH := $(COMPOSE) --profile bench run --rm -e GIT_COMMIT=$(GIT_COMMIT) -e HOST_LABEL="$(HOST_LABEL)" bench
FROM ?= 2011-01
TO ?= 2026-10
MONTH ?= 2026-10
SPARK_SUBMIT = spark-submit --master "$${SPARK_MASTER:-local[*]}" --driver-memory "$${SPARK_DRIVER_MEMORY:-2g}"

.PHONY: help env build up down clean schema ingest dag-test backfill transform load refresh analysis \
        dashboard dashboard-test test bench bench-synth bench-spark-pandas bench-query bench-memory figures all

help:
	@grep -E '^[a-z-]+:' Makefile | cut -d: -f1 | tr '\n' ' '; echo

env:
	@test -f .env || (cp .env.example .env && echo "created .env from .env.example")

build: env
	$(COMPOSE) --profile tools build

up: build
	$(COMPOSE) up -d --wait postgres airflow-webserver airflow-scheduler streamlit
	@echo "Airflow   http://localhost:8080   Dashboard http://localhost:8501"

down:
	$(COMPOSE) down

clean:  ## removes containers AND volumes (warehouse + Airflow metadata)
	$(COMPOSE) down -v

schema:  ## re-apply idempotent DDL to an existing database volume
	for f in 01_schema 02_indexes 03_dim_date_seed 04_views; do \
	  $(COMPOSE) exec -T postgres sh -c "psql -v ON_ERROR_STOP=1 -U \$$POSTGRES_USER -d \$$POSTGRES_DB -f /sql/$$f.sql" || exit 1; \
	done

ingest:  ## trigger the monthly DAG for MONTH=YYYY-MM (asynchronous; watch it in the Airflow UI)
	$(COMPOSE) exec airflow-scheduler airflow dags unpause tech_hiring_ingest_pipeline
	$(COMPOSE) exec airflow-scheduler airflow dags trigger tech_hiring_ingest_pipeline --conf '{"month": "$(MONTH)"}'

dag-test:  ## run the whole DAG synchronously for MONTH=YYYY-MM
	$(COMPOSE) exec airflow-scheduler airflow dags test tech_hiring_ingest_pipeline "$(MONTH)-02" \
	  -c '{"month": "$(MONTH)"}'

backfill:
	$(RUN) python -m src.ingest.backfill --from $(FROM) --to $(TO)

transform:
	$(RUN) bash -c '$(SPARK_SUBMIT) src/spark/build_dims.py --all'

load:
	$(RUN) python -m src.load.load_postgres --refresh

refresh:
	$(RUN) python -m src.load.load_postgres --refresh-only

analysis:
	$(RUN) python -m analysis.fit_salary_model
	$(RUN) python -m analysis.findings
	$(RUN) python -m analysis.label_sample
	$(RUN) python -m analysis.eval_extraction

dashboard:
	$(COMPOSE) up -d --wait streamlit
	@echo "Dashboard http://localhost:8501"

dashboard-test:  ## execute every page headlessly against the live warehouse
	$(COMPOSE) exec -T streamlit python -m app.smoke_test

test:
	$(RUN) python -m pytest -q tests

# ---- benchmarks -------------------------------------------------------------------------------
bench-synth:
	$(RUN) python -m benchmarks.synth

bench-spark-pandas:  ## Benchmark 1; each (engine, scale) runs in a fresh memory-limited container
	$(COMPOSE) stop streamlit airflow-webserver airflow-scheduler
	for scale in $$($(RUN) python -m benchmarks.synth --list-scales); do \
	  for engine in pandas spark; do \
	    $(BENCH) python -m benchmarks.spark_vs_pandas --engine $$engine --scale $$scale || exit 1; \
	  done; \
	done
	$(COMPOSE) up -d --wait airflow-webserver airflow-scheduler streamlit

bench-query:  ## Benchmark 2: warm runs in-container, cold runs restart Postgres + drop the VM page cache
	$(RUN) python -m benchmarks.query_latency prepare
	$(RUN) python -m benchmarks.query_latency warm
	bash benchmarks/run_cold.sh

bench-memory:  ## needs only the host's python3 (stdlib) and docker
	python3 benchmarks/memory_profile.py

figures:
	$(RUN) python -m analysis.make_figures

bench: bench-synth bench-spark-pandas bench-query bench-memory figures

all: up backfill transform load analysis dashboard
	@echo "Done. Dashboard: http://localhost:8501"
