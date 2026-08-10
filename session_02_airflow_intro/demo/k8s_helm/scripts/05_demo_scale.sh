#!/usr/bin/env bash
# Horizontal worker scaling demo, the declarative way (helm upgrade).
# After scaling, open Flower (http://localhost:5556) to see the worker count change.
# Usage: ./scripts/05_demo_scale.sh [replicas]   (default 2; rerun with 1 to scale back)
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VALUES="$SCRIPT_DIR/../values-demo.yaml"
REPLICAS="${1:-2}"

echo "== Scaling workers → $REPLICAS replicas =="
helm upgrade airflow apache-airflow/airflow -n airflow \
  --version 1.15.0 -f "$VALUES" --set workers.replicas="$REPLICAS" >/dev/null

echo "== Watch pods (Ctrl+C once $REPLICAS workers are Running) =="
kubectl get pods -n airflow -l component=worker --watch
