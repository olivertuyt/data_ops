#!/usr/bin/env bash
# Keep only the 5 most recent Spark event logs; wipe worker/master temp dirs.
set -e

KEEP=5
CONTAINERS=(
  "session_06_performance_optimization-spark-worker-1"
  "session_06_performance_optimization-spark-worker-2"
  "s06-spark-master"
)

for c in "${CONTAINERS[@]}"; do
  docker exec "$c" sh -c 'rm -rf /tmp/blockmgr-* /tmp/spark-local-*'
  echo "cleaned tmp: $c"
done

# Trim event logs — keep 5 newest, delete the rest
docker exec s06-spark-master sh -c "
  ls -1t /tmp/spark-events/ | tail -n +$((KEEP + 1)) | \
  xargs -I{} rm -f /tmp/spark-events/{}
"
echo "event logs trimmed (kept ${KEEP} newest)"
