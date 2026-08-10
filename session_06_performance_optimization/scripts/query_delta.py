import argparse

from spark_common import build_spark, get_logger

log = get_logger(__name__)


def parse_args():
    p = argparse.ArgumentParser(description="Query a Delta table in the lakehouse.")
    p.add_argument("table", help="S3A path to the Delta table, e.g. s3a://silver/orders")
    p.add_argument("--describe", action="store_true", help="Print schema and row count, then exit")
    p.add_argument("--count", action="store_true", help="Print row count only, then exit")
    p.add_argument(
        "--sql",
        metavar="QUERY",
        help="Full SQL SELECT query; use 'tbl' as the table alias",
    )
    p.add_argument(
        "--where",
        metavar="EXPR",
        help="SQL WHERE expression, e.g. \"order_date = '2026-07-07'\"",
    )
    p.add_argument(
        "--columns",
        metavar="COL1,COL2,...",
        help="Comma-separated list of columns to display (default: all)",
    )
    p.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Number of rows to show (default: 10; ignored with --sql/--count/--describe)",
    )
    p.add_argument(
        "--truncate",
        action="store_true",
        default=False,
        help="Truncate long cell values in output (default: off)",
    )
    return p.parse_args()


def main():
    args = parse_args()

    spark = build_spark("query-delta")

    log.info("Reading Delta table: %s", args.table)
    df = spark.read.format("delta").load(args.table)

    # --describe: schema + partition info + row count
    if args.describe:
        print("\n=== Schema ===")
        df.printSchema()

        detail = spark.sql(f"DESCRIBE DETAIL delta.`{args.table}`")
        detail.select("format", "numFiles", "sizeInBytes").show(truncate=False)

        row_count = df.count()
        print("=" * 50)
        print(f"Row count: {row_count:,}")
        print("=" * 50)
        spark.stop()
        return

    # --count: row count only
    if args.count:
        row_count = df.count()
        print(f"\nRow count: {row_count:,}")
        spark.stop()
        return

    # --sql: arbitrary SQL query using 'tbl' as alias
    if args.sql:
        df.createOrReplaceTempView("tbl")
        result = spark.sql(args.sql)
        print("\n=== Query result (SQL) ===")
        result.show(n=args.limit, truncate=args.truncate)
        log.info("Query returned %d rows", result.count())
        spark.stop()
        return

    # Standard filter + column selection + limit
    if args.where:
        df = df.filter(args.where)

    if args.columns:
        cols = [c.strip() for c in args.columns.split(",")]
        df = df.select(*cols)

    print(f"\n=== Preview (limit={args.limit}) ===")
    df.show(n=args.limit, truncate=args.truncate)

    spark.stop()


if __name__ == "__main__":
    main()
