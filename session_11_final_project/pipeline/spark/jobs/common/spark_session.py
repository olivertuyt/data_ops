"""Spark session configured for the Polaris REST catalog."""

from __future__ import annotations

from pyspark.sql import SparkSession

from common.config import required_env


def create_spark_session(app_name: str) -> SparkSession:
    credential = f"{required_env('POLARIS_CLIENT_ID')}:" f"{required_env('POLARIS_CLIENT_SECRET')}"
    return (
        SparkSession.builder.appName(app_name)
        .master(required_env("SPARK_MASTER"))
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.polaris", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.polaris.type", "rest")
        .config("spark.sql.catalog.polaris.uri", required_env("POLARIS_CATALOG_URI"))
        .config("spark.sql.catalog.polaris.warehouse", required_env("ICEBERG_WAREHOUSE"))
        .config("spark.sql.catalog.polaris.rest.auth.type", "oauth2")
        .config(
            "spark.sql.catalog.polaris.oauth2-server-uri",
            required_env("POLARIS_OAUTH_URI"),
        )
        .config("spark.sql.catalog.polaris.scope", "PRINCIPAL_ROLE:ALL")
        .config("spark.sql.catalog.polaris.credential", credential)
        .config(
            "spark.sql.catalog.polaris.header.X-Iceberg-Access-Delegation",
            "vended-credentials",
        )
        .config("spark.driver.memory", required_env("SPARK_DRIVER_MEMORY"))
        .config("spark.sql.session.timeZone", required_env("SHOPVN_TIMEZONE"))
        .config("spark.sql.shuffle.partitions", required_env("SPARK_SHUFFLE_PARTITIONS"))
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
