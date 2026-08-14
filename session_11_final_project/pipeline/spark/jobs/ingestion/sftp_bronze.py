import hashlib, os, paramiko
import argparse
from datetime import date, datetime, timedelta

from pyspark.sql import SparkSession, functions as F, types as T
from common.spark_session import create_spark_session

PARTNERS = ["lazada", "shopee", "tiktok"]

CSV_SCHEMA = T.StructType([
    T.StructField("external_order_id", T.StringType(), True),
    T.StructField("shopvn_product_id", T.IntegerType(), True),
    T.StructField("seller_sku", T.StringType(), True),
    T.StructField("partner", T.StringType(), True),
    T.StructField("order_date", T.StringType(), True),
    T.StructField("quantity_sold", T.IntegerType(), True),
    T.StructField("sale_price", T.IntegerType(), True),
    T.StructField("platform_discount", T.IntegerType(), True),
    T.StructField("commission_rate", T.DoubleType(), True),
    T.StructField("net_revenue", T.IntegerType(), True),
    T.StructField("settlement_date", T.StringType(), True),
    T.StructField("status", T.StringType(), True),
])

REQUIRED_COLUMNS = [field.name for field in CSV_SCHEMA.fields]
KEY_COLUMNS = ["partner", "external_order_id", "shopvn_product_id", "order_date"]

def md5_stream(path):
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def download_file(sftp, remote, local):
    tmp = local + ".part"
    sftp.get(remote, tmp)
    os.replace(tmp, local)

def read_expected_md5(path: str) -> str:
    with open(path, encoding="utf-8") as handle:
        return handle.read().strip().split()[0].lower()

def verify_md5(csv_path, md5_path):
    expected = open(md5_path, encoding = "utf-8").read().strip().split()[0]
    actual = md5_stream(csv_path)
    return expected.lower() == actual.lower(), expected, actual

def connect_sftp():
    host = os.getenv("SFTP_HOST", "sftp")
    port = int(os.getenv("SFTP_PORT", "22"))
    username = os.getenv("SFTP_USER", "marketplace_reader")
    password = os.getenv("SFTP_PASSWORD", "sftp_readonly_2026")

    transport = paramiko.Transport((host, port))
    transport.connect(username=username, password=password)
    return transport, paramiko.SFTPClient.from_transport(transport)

def remote_exists(sftp, path: str) -> bool:
    try:
        sftp.stat(path)
        return True
    except FileNotFoundError:
        return False
    except IOError:
        return False

def download_atomic(sftp, remote_path: str, local_path: str):
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    part_path = local_path + ".part"

    if os.path.exists(part_path):
        os.remove(part_path)

    sftp.get(remote_path, part_path)
    os.replace(part_path, local_path)

def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()

def date_range(start: date, end: date):
    current = start
    while current <= end:
        yield current
        current += timedelta(days = 1)

def collect_files_from_sftp(args):
    transport, sftp = connect_sftp()
    manifest = []
    valid_ready_paths = []

    try:
        for partner in PARTNERS:
            for business_date in date_range(parse_date(args.start_date), parse_date(args.end_date)):
                ymd = business_date.strftime("%Y%m%d")
                file_name = f"{partner}_{ymd}.csv"
                md5_name = f"{file_name}.md5"

                remote_csv = f"{args.remote_base_dir}/{partner}/{file_name}"
                remote_md5 = f"{args.remote_base_dir}/{partner}/{md5_name}"

                local_dir = os.path.join(args.landing_dir, partner, business_date.isoformat())
                local_csv = os.path.join(local_dir, file_name)
                local_md5 = os.path.join(local_dir, md5_name)
                ready_path = local_csv + ".ready"

                record = {
                    "source_system": "sftp",
                    "partner": partner,
                    "business_date": business_date.isoformat(),
                    "file_name": file_name,
                    "remote_path": remote_csv,
                    "local_path": ready_path,
                    "expected_md5": None,
                    "actual_md5": None,
                    "file_size_bytes": None,
                    "status": None,
                    "error_message": None,
                    "run_id": args.run_id,
                    "checked_at": datetime.utcnow().isoformat(),
                }

                if not remote_exists(sftp, remote_csv) or not remote_exists(sftp, remote_md5):
                    record["status"] = "MISSING"
                    record["error_message"] = "CSV or MD5 file is missing"
                    manifest.append(record)
                    continue

                try:
                    download_atomic(sftp, remote_csv, local_csv)
                    download_atomic(sftp, remote_md5, local_md5)

                    expected = read_expected_md5(local_md5)
                    actual = md5_stream(local_csv)

                    record["expected_md5"] = expected
                    record["actual_md5"] = actual
                    record["file_size_bytes"] = os.path.getsize(local_csv)

                    if expected != actual:
                        record["status"] = "CORRUPTED"
                        record["error_message"] = f"MD5 mismatch: expected {expected}, got {actual}"
                        manifest.append(record)
                        continue

                    os.replace(local_csv, ready_path)
                    record["status"] = "VALID"
                    manifest.append(record)
                    valid_ready_paths.append(ready_path)

                except Exception as e:
                    record["status"] = "ERROR"
                    record["error_message"] = str(e)
                    manifest.append(record)
    finally:
        sftp.close()
        transport.close()
    return manifest, valid_ready_paths

def write_manifest(spark, manifest):
    spark.sql("CREATE NAMEPSACE IF NOT EXISTS polaris.audit")

    schema = T.StructType([
        T.StructField("source_system", T.StringType(), False),
        T.StructField("partner", T.StringType(), True),
        T.StructField("business_date", T.StringType(), True),
        T.StructField("file_name", T.StringType(), True),
        T.StructField("remote_path", T.StringType(), True),
        T.StructField("local_path", T.StringType(), True),
        T.StructField("expected_md5", T.StringType(), True),
        T.StructField("actual_md5", T.StringType(), True),
        T.StructField("file_size_bytes", T.LongType(), True),
        T.StructField("status", T.StringType(), True),
        T.StructField("error_message", T.StringType(), True),
        T.StructField("run_id", T.StringType(), False),
        T.StructField("checked_at", T.StringType(), False),
    ])

    df = spark.createDataFrame(manifest, schema=schema)
    df = df.withColumn("business_date", F.to_date("business_date"))
    df = df.withColumn("checked_at", F.to_timestamp("checked_at"))

    spark.sql("""
        CREATE TABLE IF NOT EXISTS polaris.audit.source_manifests (
            source_system STRING,
            partner STRING,
            business_date DATE,
            file_name STRING,
            remote_path STRING,
            local_path STRING,
            expected_md5 STRING,
            actual_md5 STRING,
            file_size_bytes BIGINT,
            status STRING,
            error_message STRING,
            run_id STRING,
            checked_at TIMESTAMP
        )
        USING iceberg
    """)

    df.writeTo("polaris.audit.source_manifests").append()

def read_valid_csv_files(spark, paths, run_id):
    df = spark.read.option("header", "true").schema(CSV_SCHEMA).csv(paths)

    return (
        df.withColumn("order_date", F.to_date("order_date"))
          .withColumn("settlement_date", F.to_date("settlement_date"))
          .withColumn("_source_system", F.lit("sftp"))
          .withColumn("_source_file", F.input_file_name())
          .withColumn("_run_id", F.lit(run_id))
          .withColumn("_ingested_at", F.current_timestamp())
    )

def validate_marketplace_df(df):
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    for col_name in KEY_COLUMNS:
        null_count = df.filter(F.col(col_name).isNull()).count()
        if null_count > 0:
            raise ValueError(f"Column {col_name} has {null_count} null values")

    duplicate_count = ( df.groupBy(*KEY_COLUMNS).count().filter("count > 1").count() )
    if duplicate_count > 0:
        raise ValueError(f"Found {duplicate_count} duplicate rows based on key columns: {KEY_COLUMNS}")

    invalid_partner_count = df.filter(~F.col("partner").isin(PARTNERS)).count()
    if invalid_partner_count > 0:
        raise RuntimeError(f"Invalid partner values: {invalid_partner_count}")

    invalid_status_count = df.filter(~F.col("status").isin("completed", "returned", "disputed")).count()
    if invalid_status_count > 0:
        raise RuntimeError(f"Invalid status values: {invalid_status_count}")

def merge_marketplace_bronze(spark, df):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.bronze")

    df.createOrReplaceTempView("marketplace_sales_stage")

    spark.sql("""
        CREATE TABLE IF NOT EXISTS polaris.bronze.marketplace_sales (
            external_order_id STRING,
            shopvn_product_id INT,
            seller_sku STRING,
            partner STRING,
            order_date DATE,
            quantity_sold INT,
            sale_price INT,
            platform_discount INT,
            commission_rate DOUBLE,
            net_revenue INT,
            settlement_date DATE,
            status STRING,
            _source_system STRING,
            _source_file STRING,
            _run_id STRING,
            _ingested_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(order_date), partner)
    """)

    spark.sql("""
        MERGE INTO polaris.bronze.marketplace_sales AS target
        USING marketplace_sales_stage AS source
        ON target.partner = source.partner
        AND target.external_order_id = source.external_order_id
        AND target.shopvn_product_id = source.shopvn_product_id
        AND target.order_date = source.order_date

        WHEN MATCHED THEN UPDATE SET
            target.seller_sku = source.seller_sku,
            target.quantity_sold = source.quantity_sold,
            target.sale_price = source.sale_price,
            target.platform_discount = source.platform_discount,
            target.commission_rate = source.commission_rate,
            target.net_revenue = source.net_revenue,
            target.settlement_date = source.settlement_date,
            target.status = source.status,
            target._source_system = source._source_system,
            target._source_file = source._source_file,
            target._run_id = source._run_id,
            target._ingested_at = source._ingested_at

        WHEN NOT MATCHED THEN INSERT (
            external_order_id,
            shopvn_product_id,
            seller_sku,
            partner,
            order_date,
            quantity_sold,
            sale_price,
            platform_discount,
            commission_rate,
            net_revenue,
            settlement_date,
            status,
            _source_system,
            _source_file,
            _run_id,
            _ingested_at
        )
        VALUES (
            source.external_order_id,
            source.shopvn_product_id,
            source.seller_sku,
            source.partner,
            source.order_date,
            source.quantity_sold,
            source.sale_price,
            source.platform_discount,
            source.commission_rate,
            source.net_revenue,
            source.settlement_date,
            source.status,
            source._source_system,
            source._source_file,
            source._run_id,
            source._ingested_at
        )
    """)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--remote-base-dir", default="/marketplace/incoming")
    parser.add_argument("--landing-dir", default="/tmp/shopvn/landing/marketplace")
    args = parser.parse_args()

    spark = create_spark_session("sftp-bronze")

    manifest, valid_paths = collect_files_from_sftp(args)
    write_manifest(spark, manifest)

    valid_count = sum(1 for row in manifest if row["status"] == "VALID")
    corrupt_count = sum(r["status"] == "CORRUPTED" for r in manifest)   
    missing_count = sum(1 for row in manifest if row["status"] == "MISSING")
    error_count = sum(1 for row in manifest if row["status"] == "ERROR")

    print(
        f"SFTP manifest: valid={valid_count}, "
        f"missing={missing_count}, corrupt={corrupt_count}, error={error_count}"
    )

    if corrupt_count > 0 or error_count > 0:
        raise RuntimeError("Blocking SFTP ingestion because corrupt/error files were detected")

    if not valid_paths:
        print("No valid SFTP files found. Manifest was written; Bronze load skipped.")
        spark.stop()
        return

    df = read_valid_csv_files(spark, valid_paths, args.run_id)
    validate_marketplace_df(df)
    merge_marketplace_bronze(spark, df)

    loaded_count = df.count()
    print(f"Loaded marketplace Bronze rows: {loaded_count}")

    spark.stop()


if __name__ == "__main__":
    main()


