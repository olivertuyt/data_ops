#!/usr/bin/env bash
# Homework verification script — session_07_security
# Run from session_07_security/: bash homework/verify.sh
set -euo pipefail

PASS=0
FAIL=0

ok()   { echo "[PASS] $*"; PASS=$((PASS+1)); }
fail() { echo "[FAIL] $*"; FAIL=$((FAIL+1)); }

trino_count() {
  docker compose exec -T demo-cli python /scripts/query_trino.py \
    --user "$1" ${2:+--password "$2"} \
    --sql "$3" 2>&1 | grep -E "^[0-9]+" | head -1
}

echo "========================================"
echo "  Session 07 Homework — Verify Script"
echo "========================================"
echo ""

# ─── Part 1: Row-Level Security ──────────────────────────────────────────────
echo "── Part 1: Row-Level Security ───"

# 1a. analyst_uk sees only UK rows
COUNT=$(trino_count analyst_uk "AnalystUK_2026!" "SELECT COUNT(*) FROM delta.gold.customer_analytics")
if [ "$COUNT" -gt 0 ] && [ "$COUNT" -lt 200 ]; then
  ok "analyst_uk sees $COUNT rows (UK only, not full 200)"
else
  fail "analyst_uk sees $COUNT rows — expected <200 and >0 (row filter not applied?)"
fi

# 1b. analyst_us sees only US rows
COUNT_US=$(trino_count analyst_us "AnalystUS_2026!" "SELECT COUNT(*) FROM delta.gold.customer_analytics")
if [ "$COUNT_US" -gt 0 ] && [ "$COUNT_US" -lt 200 ]; then
  ok "analyst_us sees $COUNT_US rows (US only, not full 200)"
else
  fail "analyst_us sees $COUNT_US rows — expected <200 and >0 (row filter not applied?)"
fi

# 1c. UK + US counts add up to total
COUNT_ADMIN=$(trino_count admin "AdminDemo_2026!" "SELECT COUNT(*) FROM delta.gold.customer_analytics")
if [ $((COUNT + COUNT_US)) -eq "$COUNT_ADMIN" ]; then
  ok "UK ($COUNT) + US ($COUNT_US) = admin total ($COUNT_ADMIN)"
else
  fail "UK ($COUNT) + US ($COUNT_US) != admin total ($COUNT_ADMIN)"
fi

# 1d. analyst_uk cannot bypass filter with explicit WHERE region = 'US'
BYPASS=$(trino_count analyst_uk "AnalystUK_2026!" "SELECT COUNT(*) FROM delta.gold.customer_analytics WHERE region = 'US'")
if [ "$BYPASS" -eq 0 ]; then
  ok "analyst_uk cannot bypass row filter (WHERE region='US' returns 0)"
else
  fail "analyst_uk bypassed row filter — WHERE region='US' returned $BYPASS rows"
fi

# 1e. existing analyst column masking still works
if docker compose exec -T demo-cli python /scripts/query_trino.py \
    --user analyst --password 'AnalystDemo_2026!' \
    --sql "SELECT cccd FROM delta.gold.customer_analytics LIMIT 1" \
    2>&1 | grep -q "^None"; then
  ok "analyst column masking intact (cccd = None)"
else
  fail "analyst column masking broken (cccd not masked to None)"
fi

echo ""
# ─── Part 2: Credential Rotation ─────────────────────────────────────────────
echo "── Part 2: Credential Rotation ──"

# 2a. old password rejected (test via external TCP on port 5433, where auth is enforced)
OLD_PW="EtlReader_DemoOnly_2026!"
if ! PGPASSWORD="$OLD_PW" psql -h localhost -p 5433 -U etl_reader -d dataops_source \
    -c "SELECT 1" >/dev/null 2>&1; then
  ok "Old password rejected by Postgres (external TCP)"
else
  fail "Old password still works — rotation not complete"
fi

# 2b. Vault holds a different password than the original
VAULT_PW=$(docker exec \
  -e VAULT_ADDR=http://127.0.0.1:8200 \
  -e VAULT_TOKEN=root-demo-token \
  s07-vault vault kv get -field=password secret/dataops/postgres_source 2>/dev/null)
if [ "$VAULT_PW" != "$OLD_PW" ] && [ -n "$VAULT_PW" ]; then
  ok "Vault password updated (not the original)"
else
  fail "Vault still has old password or is empty"
fi

# 2c. ingest_bronze succeeds with new credential (no restart, no code change)
if docker exec s07-spark-master /opt/spark/bin/spark-submit \
    /jobs/ingest_bronze.py --ds "$(date +%Y-%m-%d)" \
    >/dev/null 2>&1; then
  ok "ingest_bronze succeeded after rotation (zero-downtime confirmed)"
else
  fail "ingest_bronze failed after rotation"
fi

# 2d. Vault secret version > 1 (proves kv patch was used, not just re-read)
VERSION=$(docker exec \
  -e VAULT_ADDR=http://127.0.0.1:8200 \
  -e VAULT_TOKEN=root-demo-token \
  s07-vault vault kv get -format=json secret/dataops/postgres_source 2>/dev/null \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['data']['metadata']['version'])" 2>/dev/null || echo 1)
if [ "$VERSION" -gt 1 ]; then
  ok "Vault KV version is $VERSION (secret was updated, not just read)"
else
  fail "Vault KV version is $VERSION — was kv patch run?"
fi

echo ""
echo "========================================"
echo "  Results: $PASS passed, $FAIL failed"
echo "========================================"
[ "$FAIL" -eq 0 ]
