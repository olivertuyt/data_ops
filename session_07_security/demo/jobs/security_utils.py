import json
import logging
import os
from datetime import datetime, timezone

import hvac

AUDIT_LOG_PATH = "/var/log/pii_audit/pii_access.log"

audit_logger = logging.getLogger("audit.pii_access")


def get_logger(name: str) -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    return logging.getLogger(name)


def get_vault_client() -> hvac.Client:
    addr = os.environ.get("VAULT_ADDR", "http://vault:8200")
    token = os.environ["VAULT_TOKEN"]
    client = hvac.Client(url=addr, token=token)
    if not client.is_authenticated():
        raise RuntimeError(f"Failed to authenticate to Vault at {addr}")
    return client


def get_vault_secret(path: str) -> dict:
    client = get_vault_client()
    response = client.secrets.kv.v2.read_secret_version(path=path, raise_on_deleted_version=True)
    return response["data"]["data"]


def get_spark_session(app_name: str, minio_secret: dict):
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")
        .config("spark.hadoop.fs.s3a.endpoint", minio_secret["endpoint"])
        .config("spark.hadoop.fs.s3a.access.key", minio_secret["access_key"])
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret["secret_key"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.sql.warehouse.dir", "s3a://silver/")
        .enableHiveSupport()
        .getOrCreate()
    )


def log_pii_access(job_name: str, table: str, columns: list, record_count: int, ds: str) -> None:
    record = {
        "event": "PII_ACCESS",
        "job": job_name,
        "table": table,
        "pii_columns": columns,
        "record_count": record_count,
        "execution_date": ds,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    line = json.dumps(record)
    audit_logger.info(line)
    try:
        os.makedirs(os.path.dirname(AUDIT_LOG_PATH), exist_ok=True)
        with open(AUDIT_LOG_PATH, "a") as f:
            f.write(line + "\n")
    except OSError as e:
        audit_logger.warning(f"Could not write audit log file: {e}")
