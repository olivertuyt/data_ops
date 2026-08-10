import datetime as dt

from pyspark.sql import functions as F
from spark_common import build_spark, get_logger

log = get_logger(__name__)
spark = build_spark(
    "setup-lab2-table",
    {"spark.sql.adaptive.enabled": "false", "spark.sql.shuffle.partitions": "50"},
)

orders = spark.read.format("delta").load("s3a://silver/orders")
TABLE = "s3a://silver/lab2_orders"

# Replays the daily ingest job's history (21 scheduled runs, compressed).
start = dt.date(2026, 7, 1)
end = dt.date(2026, 7, 21)
day = start
while day <= end:
    batch = orders.filter(F.col("order_date") == F.lit(str(day))).dropDuplicates(["order_id"])
    mode = "overwrite" if day == start else "append"
    batch.write.format("delta").mode(mode).partitionBy("order_date").save(TABLE)
    day += dt.timedelta(days=1)

log.info("lab2 table ready: daily history %s..%s -> %s", start, end, TABLE)
spark.stop()
