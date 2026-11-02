#!/bin/bash
# Origin: original
# Runs once, on an empty data volume (docker-entrypoint-initdb.d). Creates the Airflow metadata
# database and applies the warehouse DDL to $POSTGRES_DB. `make schema` re-applies the same
# idempotent SQL on an existing volume.
set -euo pipefail
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-SQL
  CREATE DATABASE ${AIRFLOW_DB:-airflow};
SQL
for f in /sql/01_schema.sql /sql/02_indexes.sql /sql/03_dim_date_seed.sql /sql/04_views.sql; do
  echo "applying $f"
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -f "$f"
done
