import os
import shutil
import subprocess
import sys

import duckdb
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "jobs"))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "plugins")))

# PySpark 3.5 requires Java 8–17. Java 21+ removes Subject.getSubject() used by Hadoop.
# On macOS, auto-select Java 8 when the default JVM is newer (CI uses ubuntu-latest with Java 11).
if shutil.which("/usr/libexec/java_home"):
    _java8 = subprocess.run(
        ["/usr/libexec/java_home", "-v", "1.8"],
        capture_output=True, text=True
    ).stdout.strip()
    if _java8:
        os.environ.setdefault("JAVA_HOME", _java8)

LAB_JOBS_PATH = os.path.join(os.path.dirname(__file__), "..", "jobs")
SQL_PATH = os.path.join(LAB_JOBS_PATH, "bronze2silver__orders.sql")


@pytest.fixture(scope="session")
def dagbag():
    pytest.importorskip("airflow", reason="airflow not installed — DAG tests run in CI only")
    from airflow.models import DagBag

    return DagBag(dag_folder=LAB_JOBS_PATH, include_examples=False)


@pytest.fixture
def con():
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA bronze; CREATE SCHEMA silver;")
    conn.execute("""
        CREATE TABLE bronze.orders_raw (
            order_id BIGINT,
            customer_id INTEGER,
            customer_name VARCHAR,
            amount DOUBLE,
            order_date DATE
        )
    """)
    conn.execute("""
        CREATE TABLE silver.orders (
            order_id BIGINT,
            customer_id INTEGER,
            amount DOUBLE,
            order_date DATE,
            PRIMARY KEY (order_id, order_date)
        )
    """)
    conn.execute("""
        INSERT INTO bronze.orders_raw VALUES
            (1, 101, 'Alice',  100.0, '2026-01-15'),
            (2, 102, 'Bob',    200.0, '2026-01-15'),
            (3, 103, 'Carol',   50.0, '2026-01-15'),
            (4, NULL, NULL,    42.0,  '2026-01-15'),
            (5, 104, 'Dan',   -15.0,  '2026-01-15'),
            (6, 105, 'Eve',    80.0,  '2026-01-16')
    """)
    with open(SQL_PATH) as f:
        silver_sql = f.read()
    yield conn, silver_sql
    conn.close()


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[1]")
        .config("spark.driver.memory", "1g")
        .config("spark.ui.enabled", "false")
        .appName("test-session-08-lab")
        .getOrCreate()
    )
