import sys

from spark_common import build_spark, get_logger

log = get_logger(__name__)
path = sys.argv[1]

spark = build_spark("compact-files")
before = spark.sql(f"DESCRIBE DETAIL delta.`{path}`").select("numFiles").first()[0]
spark.sql(f"OPTIMIZE delta.`{path}`")
after = spark.sql(f"DESCRIBE DETAIL delta.`{path}`").select("numFiles").first()[0]
log.info("%s: numFiles %d -> %d", path, before, after)
spark.stop()
