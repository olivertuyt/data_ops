import os
from pyspark.sql import SparkSession

def build_spark(app_name):
    return (
        SparkSession.builder
        .appName(app_name)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            "spark.sql.catalog.polaris",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config(
            "spark.sql.catalog.polaris.catalog-impl",
            "org.apache.iceberg.rest.RESTCatalog",
        )
        .config(
            "spark.sql.catalog.polaris.uri",
            os.getenv(
                "POLARIS_URI",
                "http://polaris:8181/api/catalog",
            ),
        )
        .config(
            "spark.sql.catalog.polaris.warehouse",
            os.getenv(
                "POLARIS_WAREHOUSE",
                "s3://shopvn-lakehouse/warehouse",
            ),
        )
        .config(
            "spark.sql.catalog.polaris.token",
            os.getenv("POLARIS_TOKEN", ""),
        )
        .config(
            "spark.sql.catalog.polaris.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        )
        .config(
            "spark.sql.catalog.polaris.s3.endpoint",
            os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        )
        .config(
            "spark.sql.catalog.polaris.s3.access-key-id",
            os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        )
        .config(
            "spark.sql.catalog.polaris.s3.secret-access-key",
            os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        )
        .config(
            "spark.sql.catalog.polaris.s3.path-style-access",
            "true",
        )
        .getOrCreate()
    )