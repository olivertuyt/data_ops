#!/usr/bin/env bash
# Self-healing demo: kill the worker pod → K8s recreates it (~15 seconds).
# Compare with Docker Compose: restart-in-place vs pod reschedule.
# Usage: ./scripts/04_demo_self_healing.sh
set -euo pipefail

echo "== Current worker =="
kubectl get pods -n airflow -l component=worker

echo
echo "== Killing worker pod =="
kubectl delete pod -n airflow -l component=worker --wait=false

echo
echo "== Watch K8s recreate the pod (Ctrl+C once the new pod is Running) =="
kubectl get pods -n airflow -l component=worker --watch
