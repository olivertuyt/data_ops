#!/usr/bin/env bash
# Bring up the whole demo: Minikube + Airflow via Helm.
# First run takes ~10 minutes (image pulls); later runs ~2-3 minutes thanks to cache.
# Usage: ./scripts/01_up.sh
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VALUES="$SCRIPT_DIR/../values-demo.yaml"

# Docker Desktop with only ~8GB → keep 6144; more RAM → override via env: MEMORY_MB=8192 ./scripts/01_up.sh
MEMORY_MB="${MEMORY_MB:-6144}"

echo "== 1/3 Starting Minikube (4 CPU / ${MEMORY_MB}MB) =="
minikube start --cpus 4 --memory "$MEMORY_MB"

echo "== 2/3 Adding Helm repo =="
helm repo add apache-airflow https://airflow.apache.org >/dev/null 2>&1 || true
helm repo update >/dev/null

echo "== 3/3 Installing Airflow (chart 1.15.0 / Airflow 2.9.3) — waiting for pods =="
helm upgrade --install airflow apache-airflow/airflow \
  --namespace airflow --create-namespace \
  --version 1.15.0 -f "$VALUES" \
  --wait --timeout 12m

echo
kubectl get pods -n airflow
echo
echo "✅ Stack is ready. Next: ./scripts/02_port_forward.sh"
