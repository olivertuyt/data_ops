import argparse
import json
import os
import time
from datetime import datetime, timezone

import requests
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from common.iceberg_utils import (
    build_spark,
    create_namespace_if_needed,
    create_table_if_needed,
    merge_into_iceberg,
)


SHIPMENT_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("carrier", StringType(), True),
        StructField("tracking_code", StringType(), True),
        StructField("status", StringType(), True),
        StructField(
            "actual_shipping_fee",
            LongType(),
            True,
        ),
        StructField("shipped_at", StringType(), True),
        StructField(
            "actual_delivery_date",
            StringType(),
            True,
        ),
        StructField(
            "estimated_delivery_date",
            StringType(),
            True,
        ),
        StructField(
            "recipient_province",
            StringType(),
            True,
        ),
        StructField(
            "recipient_district",
            StringType(),
            True,
        ),
        StructField(
            "delivery_attempts",
            IntegerType(),
            True,
        ),
        StructField(
            "failure_reason",
            StringType(),
            True,
        ),
        StructField(
            "_raw_payload",
            StringType(),
            True,
        ),
    ]
)


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--run-id", required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)

    parser.add_argument(
        "--batch-size",
        type=int,
        default=50,
    )

    return parser.parse_args()


def read_order_ids(spark, args):
    host = os.getenv("SHOPVN_DB_HOST", "postgres")
    port = os.getenv("SHOPVN_DB_PORT", "5432")
    database = os.getenv("SHOPVN_DB_NAME", "shopvn")
    user = os.getenv("SHOPVN_DB_USER", "shopvn_reader")
    password = os.getenv("SHOPVN_DB_PASSWORD", "readonly123")

    jdbc_url = (
        f"jdbc:postgresql://{host}:{port}/{database}"
    )

    query = f"""
    (
        SELECT order_id
        FROM orders
        WHERE order_date BETWEEN
            DATE '{args.start_date}'
            AND DATE '{args.end_date}'
        ORDER BY order_id
    ) AS order_ids
    """

    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", query)
        .option("user", user)
        .option("password", password)
        .option("driver", "org.postgresql.Driver")
        .option("fetchsize", "10000")
        .load()
    )


def chunk_values(values, batch_size):
    for index in range(0, len(values), batch_size):
        yield values[index:index + batch_size]


def retry_after_seconds(response):
    try:
        payload = response.json()

        if payload.get("retry_after") is not None:
            return float(payload["retry_after"])
    except Exception:
        pass

    header_value = response.headers.get("Retry-After")

    if header_value:
        try:
            return float(header_value)
        except ValueError:
            pass

    return 5.0


def call_api(session, base_url, api_key, order_ids):
    if not order_ids or len(order_ids) > 50:
        raise ValueError(
            "API batch size must be between 1 and 50"
        )

    url = (
        f"{base_url.rstrip('/')}/v1/shipments"
    )

    headers = {
        "X-API-Key": api_key,
        "Accept": "application/json",
    }

    timeout_attempt = 0
    server_attempt = 0

    while True:
        try:
            response = session.get(
                url=url,
                headers=headers,
                params={
                    "order_ids": ",".join(order_ids)
                },
                timeout=(5, 35),
            )

        except requests.Timeout as exc:
            if timeout_attempt >= 3:
                return {
                    "shipments": [],
                    "not_found": [],
                    "errors": [
                        {
                            "error_type": "timeout_exhausted",
                            "message": str(exc),
                            "order_ids": order_ids,
                        }
                    ],
                }

            time.sleep(
                [1, 2, 4][timeout_attempt]
            )

            timeout_attempt += 1
            continue

        if response.status_code == 200:
            payload = response.json()

            shipments = payload.get(
                "shipments",
                [],
            )

            for shipment in shipments:
                shipment["_raw_payload"] = json.dumps(
                    shipment,
                    ensure_ascii=False,
                )

            return {
                "shipments": shipments,
                "not_found": payload.get(
                    "not_found",
                    [],
                ),
                "errors": [],
            }

        if response.status_code == 401:
            raise RuntimeError(
                "API authentication failed"
            )

        if response.status_code == 400:
            raise RuntimeError(
                "API returned HTTP 400. "
                "Check batch size."
            )

        if response.status_code == 404:
            return {
                "shipments": [],
                "not_found": order_ids,
                "errors": [],
            }

        if response.status_code == 429:
            time.sleep(
                retry_after_seconds(response)
            )
            continue

        if response.status_code == 500:
            if server_attempt >= 1:
                return {
                    "shipments": [],
                    "not_found": [],
                    "errors": [
                        {
                            "error_type": (
                                "server_error_exhausted"
                            ),
                            "message": response.text[:1000],
                            "order_ids": order_ids,
                        }
                    ],
                }

            server_attempt += 1
            time.sleep(2)
            continue

        return {
            "shipments": [],
            "not_found": [],
            "errors": [
                {
                    "error_type": (
                        f"http_{response.status_code}"
                    ),
                    "message": response.text[:1000],
                    "order_ids": order_ids,
                }
            ],
        }


def add_metadata(df, run_id):
    return (
        df
        .withColumn(
            "_source_system",
            F.lit("logistics_api"),
        )
        .withColumn(
            "_source_endpoint",
            F.lit("/v1/shipments"),
        )
        .withColumn(
            "_run_id",
            F.lit(run_id),
        )
        .withColumn(
            "_ingested_at",
            F.current_timestamp(),
        )
    )


def write_api_errors(spark, errors, run_id):
    if not errors:
        return

    rows = [
        (
            run_id,
            error["error_type"],
            error["message"],
            json.dumps(error["order_ids"]),
            datetime.now(timezone.utc),
        )
        for error in errors
    ]

    schema = StructType(
        [
            StructField("run_id", StringType(), False),
            StructField(
                "error_type",
                StringType(),
                False,
            ),
            StructField(
                "message",
                StringType(),
                True,
            ),
            StructField(
                "order_ids",
                StringType(),
                True,
            ),
            StructField(
                "logged_at",
                TimestampType(),
                False,
            ),
        ]
    )

    error_df = spark.createDataFrame(
        rows,
        schema=schema,
    )

    target_table = "polaris.audit.api_errors"

    create_namespace_if_needed(
        spark,
        "polaris.audit",
    )

    create_table_if_needed(
        spark=spark,
        table_name=target_table,
        df=error_df,
    )

    merge_condition = """
        target.run_id = source.run_id
        AND target.error_type = source.error_type
        AND target.order_ids = source.order_ids
    """

    merge_into_iceberg(
        spark=spark,
        target_table=target_table,
        source_df=error_df,
        merge_condition=merge_condition,
    )


def main():
    args = parse_args()
    spark = build_spark("shopvn-api-bronze")

    base_url = os.getenv(
        "LOGISTICS_API_URL",
        "http://api:8000",
    )

    api_key = os.getenv(
        "LOGISTICS_API_KEY",
        "shopvn-logistics-key-2026",
    )

    batch_size = min(
        max(args.batch_size, 1),
        50,
    )

    try:
        order_df = read_order_ids(
            spark=spark,
            args=args,
        )

        order_ids = [
            row["order_id"]
            for row in order_df.collect()
            if row["order_id"] is not None
        ]

        all_shipments = []
        all_errors = []

        with requests.Session() as session:
            for batch in chunk_values(
                order_ids,
                batch_size,
            ):
                result = call_api(
                    session=session,
                    base_url=base_url,
                    api_key=api_key,
                    order_ids=batch,
                )

                all_shipments.extend(
                    result["shipments"]
                )

                all_errors.extend(
                    result["errors"]
                )

        write_api_errors(
            spark=spark,
            errors=all_errors,
            run_id=args.run_id,
        )

        if not all_shipments:
            print(
                "No API shipments returned. "
                "Bronze load skipped."
            )
            return

        shipment_df = spark.createDataFrame(
            all_shipments,
            schema=SHIPMENT_SCHEMA,
        )

        shipment_df = add_metadata(
            df=shipment_df,
            run_id=args.run_id,
        )

        target_table = "polaris.bronze.api_shipments"

        create_namespace_if_needed(
            spark,
            "polaris.bronze",
        )

        create_table_if_needed(
            spark=spark,
            table_name=target_table,
            df=shipment_df,
        )

        merge_condition = """
            target.order_id = source.order_id
        """

        merge_into_iceberg(
            spark=spark,
            target_table=target_table,
            source_df=shipment_df,
            merge_condition=merge_condition,
        )

        print(
            f"API Bronze completed. "
            f"shipments={len(all_shipments)}"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()