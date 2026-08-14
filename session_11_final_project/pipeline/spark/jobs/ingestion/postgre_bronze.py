import argparse, os
from pyspark.sql import functions as F
from datetime import datetime, timezone

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.utils import AnalysisException
from common.spark_session import build_spark
from common.iceberg_utils import (create_namespace_if_needed,
    create_table_if_needed,
    merge_into_iceberg)

TABLES = {
    "customers": "customer_id",
    "products": "product_id",
    "orders": "order_id",
    "order_items": "item_id",
    "vouchers": "voucher_code",
    "inventory": "product_id",
    "inventory_transactions": "txn_id",
    "returns": "return_id",
    "product_reviews": "review_id",
}

def get_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument("--run-id", required=True)

    parser.add_argument("--tables", default=",".join(TABLES.keys()),help="Comma-separated list of tables to ingest")

    parser.add_argument(
        "--start-date",
        default=None,
        help="Used for orders, for example 2026-06-01",
    )

    parser.add_argument(
        "--end-date",
        default=None,
        help="Used for orders, for example 2026-06-30",
    )

    return parser.parse_args()

def read_postgres_table(spark, table_name, args):
    host = os.getenv("SHOPVN_DB_HOST", "postgres")
    port = os.getenv("SHOPVN_DB_PORT", "5432")
    database = os.getenv("SHOPVN_DB_NAME", "shopvn")
    user = os.getenv("SHOPVN_DB_USER", "shopvn_reader")
    password = os.getenv("SHOPVN_DB_PASSWORD", "readonly123")

    jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}"

    reader = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table_name)
        .option("user", user)
        .option("password", password)
        .option("driver", "org.postgresql.Driver")
        .option("fetchsize", "10000")
    )

    if table_name == "orders" and args.start_date and args.end_date:
        query = f"""
            (SELECT * FROM orders WHERE order_date BETWEEN DATE '{args.start_date}' AND DATE '{args.end_date}') AS orders_window"""

        reader = reader.option("dbtable", query)

    return reader.load()

def add_metadata(df, table_name, run_id):
    return (
        df
        .withColumn("_source_system", F.lit("postgresql"))
        .withColumn("_source_table", F.lit(table_name))
        .withColumn("_run_id", F.lit(run_id))
        .withColumn("_ingested_at", F.current_timestamp())
    )

def main():
    args = get_arguments()
    spark = build_spark("shopvb-postgres-bronze")

    try:
        create_namespace_if_needed(spark, "polaris.bronze")

        selected_tables = [
            item.strip()
            for item in args.tables.split(",")
            if item.strip()
        ]

        for table_name in selected_tables:
            if table_name not in TABLES:
                raise ValueError(f"Table {table_name} is not supported. Supported tables: {', '.join(TABLES.keys())}")

            primary_key = TABLES[table_name]

            print(f"Ingesting table: {table_name}")

            source_df = read_postgres_table(spark, table_name, args)

            source_count = source_df.count()

            if source_count == 0:
                print(f"No data found in table {table_name}. Skipping ingestion.")
                continue

            bronze_df = add_metadata(source_df, table_name, args.run_id)

            target_table = f"polaris.bronze.{table_name}"

            create_table_if_needed(spark, target_table, bronze_df)

            merge_condition = (
                f"target.`{primary_key}` "
                f"= source.`{primary_key}`"
            )

            merge_into_iceberg(
                spark=spark,
                target_table=target_table,
                source_df=bronze_df,
                merge_condition=merge_condition,
            )

            print(
                f"Loaded {source_count} rows from "
                f"{table_name} into {target_table}"
            )
    finally:
        spark.stop()

if __name__ == "__main__":
    main()