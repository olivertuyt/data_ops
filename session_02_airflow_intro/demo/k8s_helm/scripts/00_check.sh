#!/usr/bin/env bash
# Pre-flight check — run BEFORE the demo session to make sure the machine is ready.
# Usage: ./scripts/00_check.sh
set -euo pipefail

echo "== Checking tools =="
missing=0
for cmd in docker kubectl minikube helm; do
  if command -v "$cmd" >/dev/null 2>&1; then
    echo "  ✅ $cmd"
  else
    echo "  ❌ missing $cmd  → brew install $cmd"
    missing=1
  fi
done
[ "$missing" -eq 1 ] && exit 1

echo "== Checking Docker daemon & resources =="
if ! docker info >/dev/null 2>&1; then
  echo "  ❌ Docker is not running — start Docker Desktop first"
  exit 1
fi
read -r cpus mem_bytes < <(docker info --format '{{.NCPU}} {{.MemTotal}}')
mem_mb=$((mem_bytes / 1024 / 1024))
echo "  Docker Desktop: ${cpus} CPU / ${mem_mb}MB RAM"
if [ "$mem_mb" -lt 7000 ]; then
  echo "  ⚠️  RAM < 7GB — demo will be slow; raise it in Docker Desktop → Settings → Resources"
fi

echo "== Checking for conflicts with the lab Docker Compose stack =="
if docker ps --format '{{.Names}}' | grep -q 'session_02_airflow_intro'; then
  echo "  ⚠️  Session 2 Compose stack is running — with < 12GB RAM, stop it before the K8s demo:"
  echo "      (cd ../../lab && docker compose --profile flower stop)"
else
  echo "  ✅ No Compose stack running"
fi

echo "Done. Next: ./scripts/01_up.sh"
