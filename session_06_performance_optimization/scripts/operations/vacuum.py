import sys

from spark_common import build_spark, get_logger

log = get_logger(__name__)
path = sys.argv[1]
retain_hour = sys.argv[2]

# RETAIN 0 HOURS discards all history immediately — fine for a lab reset,
# never for production.
spark = build_spark(
    "vacuum", {"spark.databricks.delta.retentionDurationCheck.enabled": "false"}
)
spark.sql(f"VACUUM delta.`{path}` RETAIN {retain_hour} HOURS")
log.info("vacuumed %s", path)
spark.stop()
