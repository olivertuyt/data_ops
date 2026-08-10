#!/usr/bin/env bash
# Cleanup after the demo.
# Usage: ./scripts/99_cleanup.sh          → stop tunnels + minikube stop (KEEPS image cache — next demo starts fast)
#        ./scripts/99_cleanup.sh --full   → uninstall release + delete cluster (loses cache, full re-pull next time)
set -euo pipefail

echo "== Stopping port-forwards =="
pkill -f "kubectl port-forward svc/airflow" 2>/dev/null || echo "  (no tunnels running)"

if [ "${1:-}" = "--full" ]; then
  echo "== Uninstalling Airflow release =="
  helm uninstall airflow -n airflow 2>/dev/null || true
  echo "== Deleting Minikube cluster =="
  minikube delete
else
  echo "== Stopping Minikube (keeping cluster + image cache) =="
  minikube stop
  echo
  echo "Restart the lab Compose stack if needed:"
  echo "  (cd ../../lab && docker compose --profile flower start)"
fi
