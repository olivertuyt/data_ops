#!/usr/bin/env bash
# Open tunnels to reach the UIs from a browser (ports 8081/5556 so the demo can
# run side by side with the Docker Compose stack on 8080/5555).
# Teaching note: port-forward is a dev/debug tunnel only — production uses a
# LoadBalancer Service / Ingress.
# Usage: ./scripts/02_port_forward.sh
set -euo pipefail

pkill -f "kubectl port-forward svc/airflow" 2>/dev/null || true
sleep 1

kubectl port-forward svc/airflow-webserver 8081:8080 -n airflow >/dev/null 2>&1 &
kubectl port-forward svc/airflow-flower    5556:5555 -n airflow >/dev/null 2>&1 &
sleep 3

echo "✅ Airflow UI : http://localhost:8081  (admin / admin)"
echo "✅ Flower     : http://localhost:5556"
echo
echo "Tunnels run in the background — a pod restart breaks them; rerun this script."
echo "Stop tunnels: pkill -f 'kubectl port-forward svc/airflow'"
