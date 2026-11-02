# Origin: original
import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    from pyspark.sql import SparkSession

    tmp = tmp_path_factory.mktemp("spark-tmp")
    session = (SparkSession.builder.master("local[2]").appName("tests")
               .config("spark.sql.shuffle.partitions", "4")
               .config("spark.ui.enabled", "false")
               .config("spark.local.dir", str(tmp))
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.execution.arrow.pyspark.enabled", "true")
               .getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def pg_available() -> bool:
    try:
        import psycopg2

        from src.common import config
        psycopg2.connect(config.pg_dsn(), connect_timeout=2).close()
        return True
    except Exception:  # noqa: BLE001
        return False


requires_pg = pytest.mark.skipif(not os.environ.get("PG_HOST") or not pg_available(),
                                 reason="PostgreSQL not reachable")
