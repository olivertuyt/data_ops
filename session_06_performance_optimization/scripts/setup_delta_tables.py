from pyspark.sql import functions as F
from spark_common import build_spark, get_logger

log = get_logger(__name__)
spark = build_spark("setup-delta-tables")

BRONZE = "s3a://bronze"
SILVER = "s3a://silver"


def load(name, partition_by=None, n_files=8, derive=None):
    df = spark.read.parquet(f"{BRONZE}/{name}")
    if derive:
        df = derive(df)
    # One file per partition value (or a fixed count) — a clean file layout,
    # so table reads start from a healthy baseline.
    df = df.repartition(partition_by) if partition_by else df.repartition(n_files)
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition_by:
        writer = writer.partitionBy(partition_by)
    writer.save(f"{SILVER}/{name}")
    log.info("Loaded %s: %s rows -> %s/%s", name, f"{df.count():,}", SILVER, name)


load(
    "orders",
    partition_by="order_date",
    derive=lambda df: df.withColumn("order_date", F.to_date("order_purchase_timestamp")),
)
load("order_items", partition_by="order_date")
load("customers", n_files=4)
load("payments", n_files=8)

spark.stop()
