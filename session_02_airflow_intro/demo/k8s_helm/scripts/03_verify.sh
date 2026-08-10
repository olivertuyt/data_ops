#!/usr/bin/env bash
# Automated verify: health check + admin/admin login + Flower.
# Run after 02_port_forward.sh. Usage: ./scripts/03_verify.sh
set -euo pipefail
BASE="http://localhost:8081"
FAIL=0

echo "== 1/3 Webserver health =="
health=$(curl -sf "$BASE/health" || echo "")
if echo "$health" | grep -q '"scheduler".*"status": "healthy"'; then
  echo "  ✅ scheduler healthy"
else
  echo "  ❌ health check failed: $health"; FAIL=1
fi

echo "== 2/3 UI login (admin/admin) =="
jar=$(mktemp)
csrf=$(curl -s -c "$jar" "$BASE/login/" | grep -o 'name="csrf_token"[^>]*value="[^"]*"' | sed 's/.*value="//;s/"//' || true)
code=$(curl -s -b "$jar" -c "$jar" -o /dev/null -w "%{http_code}" -X POST "$BASE/login/" \
  -H "Referer: $BASE/login/" \
  --data-urlencode "username=admin" --data-urlencode "password=admin" \
  --data-urlencode "csrf_token=$csrf")
home=$(curl -s -b "$jar" -o /dev/null -w "%{http_code}" "$BASE/home")
rm -f "$jar"
if [ "$code" = "302" ] && [ "$home" = "200" ]; then
  echo "  ✅ login OK (POST → 302, /home → 200)"
else
  echo "  ❌ login failed (POST → $code, /home → $home)"; FAIL=1
fi

echo "== 3/3 Flower =="
fcode=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:5556/ || echo "000")
if [ "$fcode" = "200" ]; then
  echo "  ✅ Flower OK"
else
  echo "  ❌ Flower failed (HTTP $fcode)"; FAIL=1
fi

echo
if [ "$FAIL" -eq 0 ]; then
  echo "✅ All checks passed — ready to demo."
else
  echo "❌ Some checks failed — see the Troubleshoot section in the README."
  exit 1
fi
