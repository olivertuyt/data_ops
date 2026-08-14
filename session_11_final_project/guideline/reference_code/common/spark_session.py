"""Spark session configured for the Polaris REST catalog."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

from common.config import required_env


def create_spark_session(app_name: str) -> SparkSession:
    credential = (
        f"{required_env('POLARIS_CLIENT_ID')}:"
        f"{required_env('POLARIS_CLIENT_SECRET')}"
    )
    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[2]"))
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.polaris", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.polaris.type", "rest")
        .config(
            "spark.sql.catalog.polaris.uri",
            os.getenv("POLARIS_CATALOG_URI", "http://polaris:8181/api/catalog"),
        )
        .config(
            "spark.sql.catalog.polaris.warehouse",
            os.getenv("ICEBERG_WAREHOUSE", "shopvn_catalog"),
        )
        .config("spark.sql.catalog.polaris.scope", "PRINCIPAL_ROLE:ALL")
        .config("spark.sql.catalog.polaris.credential", credential)
        .config(
            "spark.sql.catalog.polaris.header.X-Iceberg-Access-Delegation",
            "vended-credentials",
        )
        .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "4"))
        .config("spark.sql.adaptive.enabled", "true")
    )
    return builder.getOrCreate()
