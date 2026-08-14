"""Stream, verify, audit, and ingest raw marketplace CSV files from SFTP."""

from __future__ import annotations

import argparse
import hashlib
import os
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import paramiko
from pyspark.sql import functions as F, types as T

from common.audit import write_run_audit
from common.config import required_env, validate_date_range, validate_run_id
from common.iceberg import merge_upsert
from common.spark_session import create_spark_session


PARTNERS = ("lazada", "shopee", "tiktok")
RAW_COLUMNS = (
    "external_order_id",
    "shopvn_product_id",
    "seller_sku",
    "partner",
    "order_date",
    "quantity_sold",
    "sale_price",
    "platform_discount",
    "commission_rate",
    "net_revenue",
    "settlement_date",
    "status",
)
RAW_SCHEMA = T.StructType(
    [T.StructField(column, T.StringType(), True) for column in RAW_COLUMNS]
)
MANIFEST_SCHEMA = T.StructType(
    [
        T.StructField("run_id", T.StringType(), False),
        T.StructField("partner", T.StringType(), False),
        T.StructField("business_date", T.StringType(), False),
        T.StructField("file_name", T.StringType(), False),
        T.StructField("remote_path", T.StringType(), False),
        T.StructField("ready_path", T.StringType(), False),
        T.StructField("status", T.StringType(), False),
        T.StructField("expected_md5", T.StringType(), True),
        T.StructField("actual_md5", T.StringType(), True),
        T.StructField("byte_count", T.LongType(), True),
        T.StructField("error_message", T.StringType(), True),
        T.StructField("checked_at", T.TimestampType(), False),
    ]
)


@dataclass
class ManifestRecord:
    run_id: str
    partner: str
    business_date: str
    file_name: str
    remote_path: str
    ready_path: str
    status: str
    expected_md5: str | None = None
    actual_md5: str | None = None
    byte_count: int | None = None
    error_message: str | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--remote-base-dir", default="/marketplace/incoming")
    parser.add_argument("--landing-dir", default="/opt/airflow/landing/marketplace")
    return parser.parse_args()


def iter_dates(start: date, end: date):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def remote_exists(sftp: paramiko.SFTPClient, path: str) -> bool:
    try:
        sftp.stat(path)
        return True
    except OSError:
        return False


def download_streamed(
    sftp: paramiko.SFTPClient, remote_path: str, local_path: Path
) -> tuple[str, int]:
    local_path.parent.mkdir(parents=True, exist_ok=True)
    part_path = local_path.with_name(local_path.name + ".part")
    digest = hashlib.md5()
    byte_count = 0
    try:
        with sftp.open(remote_path, "rb") as source, part_path.open("wb") as target:
            while True:
                chunk = source.read(1024 * 1024)
                if not chunk:
                    break
                target.write(chunk)
                digest.update(chunk)
                byte_count += len(chunk)
        os.replace(part_path, local_path)
    except Exception:
        part_path.unlink(missing_ok=True)
        raise
    return digest.hexdigest(), byte_count


def read_remote_checksum(sftp: paramiko.SFTPClient, path: str) -> str:
    with sftp.open(path, "r") as stream:
        raw = stream.read()
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    value = raw.strip().split()[0].lower()
    if len(value) != 32 or any(char not in "0123456789abcdef" for char in value):
        raise RuntimeError(f"Invalid MD5 file: {path}")
    return value


def collect_files(args: argparse.Namespace) -> tuple[list[ManifestRecord], list[str]]:
    start, end = validate_date_range(args.start_date, args.end_date)
    transport = paramiko.Transport(
        (required_env("SFTP_HOST"), int(required_env("SFTP_PORT")))
    )
    transport.connect(
        username=required_env("SFTP_USER"),
        password=required_env("SFTP_PASSWORD"),
    )
    sftp = paramiko.SFTPClient.from_transport(transport)
    manifests: list[ManifestRecord] = []
    ready_files: list[str] = []
    try:
        for partner in PARTNERS:
            for business_date in iter_dates(start, end):
                file_name = f"{partner}_{business_date:%Y%m%d}.csv"
                remote_path = f"{args.remote_base_dir.rstrip('/')}/{partner}/{file_name}"
                ready_path = (
                    Path(args.landing_dir)
                    / partner
                    / business_date.isoformat()
                    / f"{file_name}.ready"
                )
                base = ManifestRecord(
                    run_id=args.run_id,
                    partner=partner,
                    business_date=business_date.isoformat(),
                    file_name=file_name,
                    remote_path=remote_path,
                    ready_path=str(ready_path),
                    status="PENDING",
                )
                if not remote_exists(sftp, remote_path) or not remote_exists(
                    sftp, f"{remote_path}.md5"
                ):
                    base.status = "MISSING"
                    base.error_message = "CSV or companion MD5 file is missing"
                    manifests.append(base)
                    continue
                try:
                    expected = read_remote_checksum(sftp, f"{remote_path}.md5")
                    download_path = ready_path.with_suffix("")
                    actual, byte_count = download_streamed(
                        sftp, remote_path, download_path
                    )
                    base.expected_md5 = expected
                    base.actual_md5 = actual
                    base.byte_count = byte_count
                    if actual != expected:
                        download_path.unlink(missing_ok=True)
                        base.status = "CORRUPT"
                        base.error_message = "MD5 checksum mismatch"
                    else:
                        ready_path.parent.mkdir(parents=True, exist_ok=True)
                        os.replace(download_path, ready_path)
                        base.status = "VALID"
                        ready_files.append(str(ready_path))
                except Exception as exc:
                    base.status = "ERROR"
                    base.error_message = str(exc)[:4000]
                manifests.append(base)
    finally:
        sftp.close()
        transport.close()
    return manifests, ready_files


def write_manifests(spark, records: list[ManifestRecord]) -> None:
    rows = []
    for record in records:
        item = asdict(record)
        item["checked_at"] = datetime.now(timezone.utc).replace(tzinfo=None)
        rows.append(item)
    frame = spark.createDataFrame(rows, MANIFEST_SCHEMA)
    merge_upsert(
        spark,
        frame,
        "polaris.audit.source_manifests",
        ["run_id", "partner", "business_date", "file_name"],
    )


def ingest_ready_files(spark, ready_files: list[str], run_id: str) -> int:
    if not ready_files:
        return 0
    raw = spark.read.option("header", "true").schema(RAW_SCHEMA).csv(ready_files)
    staged = (
        raw.withColumn("_source_file", F.input_file_name())
        .withColumn(
            "_record_hash",
            F.sha2(
                F.concat_ws(
                    "\u001f", *[F.coalesce(F.col(name), F.lit("")) for name in RAW_COLUMNS]
                ),
                256,
            ),
        )
        .withColumn("_source_system", F.lit("sftp"))
        .withColumn("_run_id", F.lit(run_id))
        .withColumn("_ingested_at", F.current_timestamp())
    )
    source_count = staged.count()
    merge_upsert(
        spark,
        staged,
        "polaris.bronze.marketplace_sales",
        ["_source_file", "_record_hash"],
    )
    return source_count


def main() -> None:
    args = parse_args()
    args.run_id = validate_run_id(args.run_id)
    validate_date_range(args.start_date, args.end_date)
    manifests, ready_files = collect_files(args)
    spark = create_spark_session("shopvn-sftp-bronze")
    try:
        write_manifests(spark, manifests)
        loaded = ingest_ready_files(spark, ready_files, args.run_id)
        integrity_failures = [
            record for record in manifests if record.status in {"CORRUPT", "ERROR"}
        ]
        write_run_audit(
            spark,
            run_id=args.run_id,
            stage="bronze_sftp",
            object_name="marketplace_sales",
            business_date=f"{args.start_date}:{args.end_date}",
            status="FAIL" if integrity_failures else "PASS",
            source_count=loaded,
            target_count=spark.table("polaris.bronze.marketplace_sales").count()
            if ready_files
            else 0,
            error_message=(
                f"{len(integrity_failures)} integrity failures; inspect source_manifests"
                if integrity_failures
                else None
            ),
        )
        if integrity_failures:
            raise RuntimeError(
                "SFTP integrity validation failed; valid files were loaded idempotently, "
                "but downstream publication is blocked"
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
