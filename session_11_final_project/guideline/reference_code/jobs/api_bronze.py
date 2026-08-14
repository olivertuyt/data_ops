"""Ingest logistics shipments with bounded batches and explicit retry rules."""

from __future__ import annotations

import argparse
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from pyspark.sql import functions as F, types as T

from common.audit import write_run_audit
from common.config import required_env, validate_date_range, validate_run_id
from common.iceberg import merge_upsert
from common.spark_session import create_spark_session


BATCH_SIZE = 50
SHIPMENT_SCHEMA = T.StructType(
    [
        T.StructField("order_id", T.StringType(), True),
        T.StructField("carrier", T.StringType(), True),
        T.StructField("tracking_code", T.StringType(), True),
        T.StructField("status", T.StringType(), True),
        T.StructField("actual_shipping_fee", T.LongType(), True),
        T.StructField("shipped_at", T.StringType(), True),
        T.StructField("actual_delivery_date", T.StringType(), True),
        T.StructField("estimated_delivery_date", T.StringType(), True),
        T.StructField("recipient_province", T.StringType(), True),
        T.StructField("recipient_district", T.StringType(), True),
        T.StructField("delivery_attempts", T.IntegerType(), True),
        T.StructField("failure_reason", T.StringType(), True),
        T.StructField("_raw_payload", T.StringType(), False),
        T.StructField("_source_system", T.StringType(), False),
        T.StructField("_run_id", T.StringType(), False),
        T.StructField("_ingested_at", T.TimestampType(), False),
    ]
)
API_ERROR_SCHEMA = T.StructType(
    [
        T.StructField("run_id", T.StringType(), False),
        T.StructField("business_date", T.StringType(), False),
        T.StructField("error_type", T.StringType(), False),
        T.StructField("order_ids", T.StringType(), False),
        T.StructField("status_code", T.IntegerType(), True),
        T.StructField("message", T.StringType(), False),
        T.StructField("recorded_at", T.TimestampType(), False),
    ]
)


@dataclass
class ApiError:
    run_id: str
    business_date: str
    error_type: str
    order_ids: str
    status_code: int | None
    message: str
    recorded_at: datetime


class RequestRateLimiter:
    def __init__(self, minimum_interval_seconds: float = 0.61) -> None:
        self.minimum_interval_seconds = minimum_interval_seconds
        self.last_request_at = 0.0

    def wait(self) -> None:
        elapsed = time.monotonic() - self.last_request_at
        if elapsed < self.minimum_interval_seconds:
            time.sleep(self.minimum_interval_seconds - elapsed)
        self.last_request_at = time.monotonic()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--landing-dir", default="/opt/airflow/landing/api")
    return parser.parse_args()


def retry_after_seconds(response: requests.Response) -> float:
    header = response.headers.get("Retry-After")
    if header:
        try:
            return max(0.0, float(header))
        except ValueError:
            pass
    try:
        return max(0.0, float(response.json().get("retry_after", 5)))
    except (ValueError, TypeError, requests.JSONDecodeError):
        return 5.0


def fetch_batch(
    session: requests.Session,
    limiter: RequestRateLimiter,
    base_url: str,
    api_key: str,
    order_ids: list[str],
) -> tuple[list[dict[str, Any]], list[str], ApiError | None]:
    if not 1 <= len(order_ids) <= BATCH_SIZE:
        raise ValueError(f"API batch size must be between 1 and {BATCH_SIZE}")
    timeout_retries = 0
    server_retries = 0
    while True:
        limiter.wait()
        try:
            response = session.get(
                f"{base_url.rstrip('/')}/v1/shipments",
                headers={"X-API-Key": api_key},
                params={"order_ids": ",".join(order_ids)},
                timeout=(5, 35),
            )
        except requests.Timeout:
            if timeout_retries >= 3:
                return [], [], ApiError(
                    run_id="",
                    business_date="",
                    error_type="timeout_exhausted",
                    order_ids=",".join(order_ids),
                    status_code=None,
                    message="Timeout after three retries",
                    recorded_at=datetime.now(timezone.utc).replace(tzinfo=None),
                )
            time.sleep((1, 2, 4)[timeout_retries])
            timeout_retries += 1
            continue

        if response.status_code == 200:
            body = response.json()
            return body.get("shipments", []), body.get("not_found", []), None
        if response.status_code == 404:
            return [], order_ids, None
        if response.status_code == 429:
            time.sleep(retry_after_seconds(response))
            continue
        if response.status_code == 500:
            if server_retries == 0:
                server_retries += 1
                time.sleep(2)
                continue
            return [], [], ApiError(
                run_id="",
                business_date="",
                error_type="http_500_exhausted",
                order_ids=",".join(order_ids),
                status_code=500,
                message=response.text[:1000],
                recorded_at=datetime.now(timezone.utc).replace(tzinfo=None),
            )
        if response.status_code in {400, 401}:
            raise RuntimeError(
                f"Non-retryable API response {response.status_code}: "
                f"{response.text[:1000]}"
            )
        return [], [], ApiError(
            run_id="",
            business_date="",
            error_type=f"http_{response.status_code}",
            order_ids=",".join(order_ids),
            status_code=response.status_code,
            message=response.text[:1000],
            recorded_at=datetime.now(timezone.utc).replace(tzinfo=None),
        )


def write_json_line(stream, shipment: dict[str, Any], run_id: str) -> None:
    output = dict(shipment)
    output["_raw_payload"] = json.dumps(
        shipment, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    output["_source_system"] = "logistics_api"
    output["_run_id"] = run_id
    output["_ingested_at"] = datetime.now(timezone.utc).isoformat()
    stream.write(json.dumps(output, ensure_ascii=False) + "\n")


def persist_api_errors(spark, errors: list[ApiError]) -> None:
    if not errors:
        return
    frame = spark.createDataFrame([error.__dict__ for error in errors], API_ERROR_SCHEMA)
    merge_upsert(
        spark,
        frame,
        "polaris.audit.api_errors",
        ["run_id", "business_date", "error_type", "order_ids"],
    )


def main() -> None:
    args = parse_args()
    run_id = validate_run_id(args.run_id)
    validate_date_range(args.start_date, args.end_date)
    spark = create_spark_session("shopvn-api-bronze")
    landing_dir = Path(args.landing_dir)
    landing_dir.mkdir(parents=True, exist_ok=True)
    part_path = landing_dir / f"shipments_{run_id}.ndjson.part"
    ready_path = landing_dir / f"shipments_{run_id}.ndjson.ready"
    errors: list[ApiError] = []
    not_found_count = 0
    shipment_count = 0

    try:
        orders = (
            spark.table("polaris.bronze.orders")
            .where(
                (F.col("order_date") >= F.lit(args.start_date).cast("date"))
                & (F.col("order_date") <= F.lit(args.end_date).cast("date"))
            )
            .select("order_id", "order_date")
            .dropDuplicates(["order_id"])
            .orderBy("order_id")
        )
        base_url = required_env("LOGISTICS_API_URL")
        api_key = required_env("LOGISTICS_API_KEY")
        limiter = RequestRateLimiter()
        session = requests.Session()
        batch: list[tuple[str, str]] = []

        with part_path.open("w", encoding="utf-8", newline="\n") as stream:
            for row in orders.toLocalIterator():
                batch.append((row["order_id"], str(row["order_date"])))
                if len(batch) < BATCH_SIZE:
                    continue
                ids = [item[0] for item in batch]
                shipments, not_found, error = fetch_batch(
                    session, limiter, base_url, api_key, ids
                )
                for shipment in shipments:
                    write_json_line(stream, shipment, run_id)
                shipment_count += len(shipments)
                not_found_count += len(not_found)
                if error:
                    error.run_id = run_id
                    error.business_date = batch[0][1]
                    errors.append(error)
                batch.clear()

            if batch:
                ids = [item[0] for item in batch]
                shipments, not_found, error = fetch_batch(
                    session, limiter, base_url, api_key, ids
                )
                for shipment in shipments:
                    write_json_line(stream, shipment, run_id)
                shipment_count += len(shipments)
                not_found_count += len(not_found)
                if error:
                    error.run_id = run_id
                    error.business_date = batch[0][1]
                    errors.append(error)
        os.replace(part_path, ready_path)

        staged = (
            spark.read.schema(SHIPMENT_SCHEMA).json(str(ready_path))
            .withColumn("_ingested_at", F.to_timestamp("_ingested_at"))
        )
        merge_upsert(
            spark,
            staged,
            "polaris.bronze.api_shipments",
            ["order_id"],
        )
        persist_api_errors(spark, errors)
        write_run_audit(
            spark,
            run_id=run_id,
            stage="bronze_api",
            object_name="api_shipments",
            business_date=f"{args.start_date}:{args.end_date}",
            status="FAIL" if errors else "PASS",
            source_count=shipment_count + not_found_count,
            target_count=shipment_count,
            error_message=(
                f"errors={len(errors)}, not_found={not_found_count}"
                if errors
                else f"not_found={not_found_count}"
            ),
        )
        if errors:
            raise RuntimeError(
                f"API extraction completed with {len(errors)} exhausted technical errors"
            )
    finally:
        part_path.unlink(missing_ok=True)
        spark.stop()


if __name__ == "__main__":
    main()
