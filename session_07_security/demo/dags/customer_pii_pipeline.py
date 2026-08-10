import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLCheckOperator

logger = logging.getLogger(__name__)

TRINO_HOST = "trino"
TRINO_PORT = 8443
AUDIT_LOG_PATH = "/opt/airflow/logs/audit/pii_access.log"
SPARK_CONF = {"spark.driver.host": "airflow-worker", "spark.driver.bindAddress": "0.0.0.0"}
PY_FILES = "/opt/airflow/jobs/security_utils.py"


def _spark_submit(task_id: str, application: str) -> SparkSubmitOperator:
    return SparkSubmitOperator(
        task_id=task_id,
        application=application,
        conn_id="spark_default",
        application_args=["--ds", "{{ ds }}"],
        py_files=PY_FILES,
        conf=SPARK_CONF,
        verbose=False,
    )


@dag(
    dag_id="session_07_customer_pii_pipeline",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(hours=1),
    },
    tags=["security-demo", "pii", "bronze-silver-gold"],
)
def customer_pii_pipeline():
    validate_source_connection = SQLCheckOperator(
        task_id="validate_source_connection",
        conn_id="postgres_source_conn",
        sql="sql/customer_pii_pipeline/check_source.sql",
    )

    ingest_bronze = _spark_submit("ingest_bronze", "/opt/airflow/jobs/ingest_bronze.py")
    transform_silver = _spark_submit("transform_silver", "/opt/airflow/jobs/transform_silver.py")
    transform_gold = _spark_submit("transform_gold", "/opt/airflow/jobs/transform_gold.py")

    @task
    def reconcile(ds=None):
        import os

        import hvac
        import trino
        from trino.auth import BasicAuthentication

        vault = hvac.Client(
            url=os.environ.get("VAULT_ADDR", "http://vault:8200"),
            token=os.environ["VAULT_TOKEN"],
        )
        trino_secret = vault.secrets.kv.v2.read_secret_version(
            path="dataops/trino", raise_on_deleted_version=True
        )["data"]["data"]

        conn = trino.dbapi.connect(
            host=TRINO_HOST,
            port=TRINO_PORT,
            user=trino_secret["admin_user"],
            catalog="delta",
            schema="gold",
            http_scheme="https",
            auth=BasicAuthentication(trino_secret["admin_user"], trino_secret["admin_password"]),
            verify=os.environ["TRINO_CA_CERT"],  # exported CA cert, not verify=False
        )
        cur = conn.cursor()

        cur.execute(
            f"SELECT COUNT(*) FROM delta.gold.customer_analytics WHERE updated_at = DATE '{ds}'"
        )
        gold_count = cur.fetchone()[0]
        assert gold_count > 0, f"gold.customer_analytics empty for {ds}"

        cur.execute(
            f"SELECT COUNT(*) FROM delta.gold.customer_analytics"
            f" WHERE updated_at = DATE '{ds}' AND customer_id IS NULL"
        )
        null_ids = cur.fetchone()[0]
        assert null_ids == 0, f"Found {null_ids} rows with NULL customer_id for {ds}"

        logger.info(f"Reconcile passed for {ds}: {gold_count} rows, 0 null customer_id")

    @task
    def show_audit_summary(ds=None):
        try:
            with open(AUDIT_LOG_PATH) as f:
                lines = f.readlines()
        except FileNotFoundError:
            logger.warning("No audit log file found yet.")
            return
        logger.info(f"PII access audit trail (last 10 entries, {len(lines)} total):")
        for line in lines[-10:]:
            logger.info(line.strip())

    (
        validate_source_connection
        >> ingest_bronze
        >> transform_silver
        >> transform_gold
        >> reconcile()
        >> show_audit_summary()
    )


customer_pii_pipeline()
