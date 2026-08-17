"""Ingest logistics shipments with bounded batches and explicit retry rules."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests
from common.audit import write_run_audit
from common.config import ApiSourceConfig, validate_date_range, validate_run_id
from common.iceberg import assert_unique_non_null, merge_upsert
from common.spark_session import create_spark_session
from pyspark.sql import functions as F
from pyspark.sql import types as T

BATCH_SIZE = 50
MAX_RATE_LIMIT_RETRIES = 8
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
        T.StructField("_source_batch_id", T.StringType(), False),
        T.StructField("_source_row_hash", T.StringType(), False),
        T.StructField("_source_system", T.StringType(), False),
        T.StructField("_run_id", T.StringType(), False),
        T.StructField("_ingested_at", T.TimestampType(), False),
    ]
)
API_EVENT_SCHEMA = T.StructType(
    [
        T.StructField("run_id", T.StringType(), False),
        T.StructField("start_date", T.StringType(), False),
        T.StructField("end_date", T.StringType(), False),
        T.StructField("event_type", T.StringType(), False),
        T.StructField("order_ids", T.StringType(), False),
        T.StructField("status_code", T.IntegerType(), True),
        T.StructField("blocks_downstream", T.BooleanType(), False),
        T.StructField("message", T.StringType(), False),
        T.StructField("recorded_at", T.TimestampType(), False),
    ]
)


@dataclass
class ApiEvent:
    run_id: str
    start_date: str
    end_date: str
    event_type: str
    order_ids: str
    status_code: int | None
    blocks_downstream: bool
    message: str
    recorded_at: datetime


class RequestRateLimiter:
    def __init__(self, requests_per_minute: int) -> None:
        if requests_per_minute <= 0:
            raise ValueError("requests_per_minute must be positive")
        self.minimum_interval_seconds = 60.0 / requests_per_minute
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
    parser.add_argument("--landing-dir", required=True)
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


def validate_success_payload(payload: Any) -> tuple[list[dict[str, Any]], list[str]]:
    if not isinstance(payload, dict):
        raise RuntimeError("API contract failure: response must be an object")
    shipments = payload.get("shipments")
    # The fixture omits not_found/not_found_count when every requested order exists.
    not_found = payload.get("not_found", [])
    if not isinstance(shipments, list) or not isinstance(not_found, list):
        raise RuntimeError("API contract failure: shipments and not_found must be arrays")
    if any(not isinstance(item, dict) or not item.get("order_id") for item in shipments):
        raise RuntimeError("API contract failure: every shipment requires order_id")
    count = payload.get("count")
    if count is not None and (not isinstance(count, int) or count != len(shipments)):
        raise RuntimeError("API contract failure: count must equal the shipments length")
    not_found_count = payload.get("not_found_count")
    if not_found_count is not None and (
        not isinstance(not_found_count, int) or not_found_count != len(not_found)
    ):
        raise RuntimeError("API contract failure: not_found_count must equal not_found length")
    return shipments, [str(item) for item in not_found]


def technical_event(
    event_type: str,
    order_ids: list[str],
    status_code: int | None,
    message: str,
) -> ApiEvent:
    return ApiEvent(
        run_id="",
        start_date="",
        end_date="",
        event_type=event_type,
        order_ids=",".join(order_ids),
        status_code=status_code,
        blocks_downstream=True,
        message=message[:1000],
        recorded_at=datetime.now(UTC).replace(tzinfo=None),
    )


def fetch_batch(
    session: requests.Session,
    limiter: RequestRateLimiter,
    config: ApiSourceConfig,
    order_ids: list[str],
) -> tuple[list[dict[str, Any]], list[str], ApiEvent | None]:
    if not 1 <= len(order_ids) <= BATCH_SIZE:
        raise ValueError(f"API batch size must be between 1 and {BATCH_SIZE}")
    timeout_retries = 0
    server_retries = 0
    rate_limit_retries = 0
    while True:
        limiter.wait()
        try:
            response = session.get(
                f"{config.base_url}/v1/shipments",
                headers={"X-API-Key": config.api_key},
                params={"order_ids": ",".join(order_ids)},
                timeout=(config.connect_timeout_seconds, config.read_timeout_seconds),
            )
        except requests.Timeout:
            if timeout_retries >= 3:
                return (
                    [],
                    [],
                    technical_event(
                        "timeout_exhausted", order_ids, None, "Timeout after 1s/2s/4s retries"
                    ),
                )
            time.sleep((1, 2, 4)[timeout_retries])
            timeout_retries += 1
            continue

        if response.status_code == 200:
            shipments, not_found = validate_success_payload(response.json())
            return shipments, not_found, None
        if response.status_code == 404:
            return [], order_ids, None
        if response.status_code == 429:
            if rate_limit_retries >= MAX_RATE_LIMIT_RETRIES:
                return (
                    [],
                    [],
                    technical_event(
                        "rate_limit_exhausted",
                        order_ids,
                        429,
                        f"HTTP 429 after {MAX_RATE_LIMIT_RETRIES} retries",
                    ),
                )
            time.sleep(retry_after_seconds(response))
            rate_limit_retries += 1
            continue
        if response.status_code >= 500:
            if server_retries == 0:
                server_retries += 1
                time.sleep(1)
                continue
            return (
                [],
                [],
                technical_event(
                    "server_error_exhausted", order_ids, response.status_code, response.text
                ),
            )
        if response.status_code in {400, 401}:
            raise RuntimeError(
                f"Non-retryable API response {response.status_code}: {response.text[:1000]}"
            )
        return (
            [],
            [],
            technical_event(
                f"http_{response.status_code}", order_ids, response.status_code, response.text
            ),
        )


def write_json_line(stream, shipment: dict[str, Any], run_id: str, batch_id: str) -> None:
    raw_payload = json.dumps(shipment, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    output = dict(shipment)
    output["_raw_payload"] = raw_payload
    output["_source_batch_id"] = batch_id
    output["_source_row_hash"] = hashlib.sha256(raw_payload.encode("utf-8")).hexdigest()
    output["_source_system"] = "logistics_api"
    output["_run_id"] = run_id
    output["_ingested_at"] = datetime.now(UTC).isoformat()
    stream.write(json.dumps(output, ensure_ascii=False) + "\n")


def persist_api_events(spark, events: list[ApiEvent]) -> None:
    if not events:
        return
    frame = spark.createDataFrame([event.__dict__ for event in events], API_EVENT_SCHEMA)
    merge_upsert(
        spark,
        frame,
        "polaris.audit.api_events",
        ["run_id", "start_date", "end_date", "event_type", "order_ids"],
    )


def main() -> None:
    args = parse_args()
    run_id = validate_run_id(args.run_id)
    validate_date_range(args.start_date, args.end_date)
    config = ApiSourceConfig.from_env()
    spark = create_spark_session("shopvn-api-bronze")
    landing_dir = Path(args.landing_dir)
    landing_dir.mkdir(parents=True, exist_ok=True)
    part_path = landing_dir / f"shipments_{run_id}.ndjson.part"
    ready_path = landing_dir / f"shipments_{run_id}.ndjson.ready"
    events: list[ApiEvent] = []
    not_found_count = 0
    shipment_count = 0
    started = time.monotonic()

    try:
        orders = (
            spark.table("polaris.bronze.orders")
            .where(
                F.col("order_date").between(
                    F.lit(args.start_date).cast("date"),
                    F.lit(args.end_date).cast("date"),
                )
            )
            .select("order_id")
            .dropDuplicates(["order_id"])
            .orderBy("order_id")
        )
        limiter = RequestRateLimiter(config.requests_per_minute)
        session = requests.Session()
        batch: list[str] = []
        batch_number = 0
        with part_path.open("w", encoding="utf-8", newline="\n") as stream:
            for row in orders.toLocalIterator():
                batch.append(row["order_id"])
                if len(batch) < BATCH_SIZE:
                    continue
                batch_number += 1
                shipments, not_found, event = fetch_batch(session, limiter, config, batch)
                batch_id = f"{run_id}.{batch_number:06d}"
                for shipment in shipments:
                    write_json_line(stream, shipment, run_id, batch_id)
                shipment_count += len(shipments)
                not_found_count += len(not_found)
                if not_found:
                    events.append(
                        ApiEvent(
                            run_id=run_id,
                            start_date=args.start_date,
                            end_date=args.end_date,
                            event_type="not_found",
                            order_ids=",".join(not_found),
                            status_code=404,
                            blocks_downstream=False,
                            message="Expected order IDs without shipment",
                            recorded_at=datetime.now(UTC).replace(tzinfo=None),
                        )
                    )
                if event:
                    event.run_id = run_id
                    event.start_date = args.start_date
                    event.end_date = args.end_date
                    events.append(event)
                batch.clear()
            if batch:
                batch_number += 1
                shipments, not_found, event = fetch_batch(session, limiter, config, batch)
                batch_id = f"{run_id}.{batch_number:06d}"
                for shipment in shipments:
                    write_json_line(stream, shipment, run_id, batch_id)
                shipment_count += len(shipments)
                not_found_count += len(not_found)
                if not_found:
                    events.append(
                        ApiEvent(
                            run_id=run_id,
                            start_date=args.start_date,
                            end_date=args.end_date,
                            event_type="not_found",
                            order_ids=",".join(not_found),
                            status_code=404,
                            blocks_downstream=False,
                            message="Expected order IDs without shipment",
                            recorded_at=datetime.now(UTC).replace(tzinfo=None),
                        )
                    )
                if event:
                    event.run_id = run_id
                    event.start_date = args.start_date
                    event.end_date = args.end_date
                    events.append(event)
        session.close()
        os.replace(part_path, ready_path)
        staged = spark.read.schema(SHIPMENT_SCHEMA).json(str(ready_path))
        staged = staged.withColumn("_ingested_at", F.to_timestamp("_ingested_at"))
        assert_unique_non_null(staged, "api_shipments", ["order_id"])
        merge_upsert(spark, staged, "polaris.bronze.api_shipments", ["order_id"])
        persist_api_events(spark, events)
        technical_failures = [event for event in events if event.blocks_downstream]
        write_run_audit(
            spark,
            run_id=run_id,
            stage="bronze_api",
            object_name="api_shipments",
            start_date=args.start_date,
            end_date=args.end_date,
            status="FAIL" if technical_failures else "PASS",
            source_count=shipment_count + not_found_count,
            target_count=shipment_count,
            duration_ms=int((time.monotonic() - started) * 1000),
            error=(
                RuntimeError(f"{len(technical_failures)} exhausted API failures")
                if technical_failures
                else None
            ),
        )
        if technical_failures:
            raise RuntimeError(
                f"Operations publication blocked by {len(technical_failures)} API failures"
            )
    finally:
        part_path.unlink(missing_ok=True)
        spark.stop()


if __name__ == "__main__":
    main()
