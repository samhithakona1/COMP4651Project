# Origin: original
# Airflow 2.10.5 (Python 3.11) + OpenJDK 17 (for spark-submit) + PySpark and the analysis stack.
# One image serves the Airflow scheduler/webserver and the `runner` tool container
# (tests, analysis, benchmarks), so every Python step runs on the same pinned versions.
FROM apache/airflow:2.10.5-python3.11

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && rm -rf /var/lib/apt/lists/* \
 && ln -sfn "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /usr/lib/jvm/jre
ENV JAVA_HOME=/usr/lib/jvm/jre

USER airflow
COPY docker/airflow/requirements.txt /tmp/requirements.txt
# Airflow's official constraints pin pyspark, pandas, pyarrow, numpy, aiohttp, psycopg2, networkx,
# psutil and pytest; requirements.txt pins the remaining packages explicitly.
RUN pip install --no-cache-dir \
      --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt \
      -r /tmp/requirements.txt

ENV PYTHONPATH=/opt/project \
    PROJECT_ROOT=/opt/project \
    PYSPARK_PYTHON=python \
    PYSPARK_DRIVER_PYTHON=python
WORKDIR /opt/project
