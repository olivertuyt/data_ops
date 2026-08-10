import sys

from spark_common import build_spark, get_logger

log = get_logger(__name__)
path = sys.argv[1]

spark = build_spark("get-num-files")
detail = spark.sql(f"DESCRIBE DETAIL delta.`{path}`").select("numFiles", "sizeInBytes").first()
df = spark.read.format("delta").load(path)
log.info("%s: numFiles=%d sizeInBytes=%d rows=%d", path, detail[0], detail[1], df.count())
spark.stop()
