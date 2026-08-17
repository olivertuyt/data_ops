#!/bin/sh
set -eu

apk add --no-cache jq >/dev/null

TOKEN=$(curl -sS --fail-with-body \
  -u "$POLARIS_CLIENT_ID:$POLARIS_CLIENT_SECRET" \
  -H "Polaris-Realm: $POLARIS_REALM" \
  -d grant_type=client_credentials \
  -d scope=PRINCIPAL_ROLE:ALL \
  "$POLARIS_OAUTH_URI" | jq -er .access_token)

CATALOG_URL="$POLARIS_MANAGEMENT_URI/catalogs/$ICEBERG_WAREHOUSE"
STATUS=$(curl -sS -o /tmp/catalog.json -w '%{http_code}' \
  -H "Authorization: Bearer $TOKEN" \
  -H "Polaris-Realm: $POLARIS_REALM" \
  "$CATALOG_URL")

if [ "$STATUS" = "404" ]; then
  curl -sS --fail-with-body -X POST \
    -H "Authorization: Bearer $TOKEN" \
    -H "Polaris-Realm: $POLARIS_REALM" \
    -H "Content-Type: application/json" \
    "$POLARIS_MANAGEMENT_URI/catalogs" \
    -d "{\"catalog\":{\"name\":\"$ICEBERG_WAREHOUSE\",\"type\":\"INTERNAL\",\"readOnly\":false,\"properties\":{\"default-base-location\":\"s3://$MINIO_BUCKET/warehouse\"},\"storageConfigInfo\":{\"storageType\":\"S3\",\"endpoint\":\"$MINIO_ENDPOINT\",\"endpointInternal\":\"$MINIO_ENDPOINT\",\"pathStyleAccess\":true,\"region\":\"us-east-1\"}}}"
elif [ "$STATUS" != "200" ]; then
  cat /tmp/catalog.json
  exit 1
fi

GRANT_STATUS=$(curl -sS -o /tmp/grant.json -w '%{http_code}' -X PUT \
  -H "Authorization: Bearer $TOKEN" \
  -H "Polaris-Realm: $POLARIS_REALM" \
  -H "Content-Type: application/json" \
  "$POLARIS_MANAGEMENT_URI/catalogs/$ICEBERG_WAREHOUSE/catalog-roles/catalog_admin/grants" \
  -d '{"type":"catalog","privilege":"CATALOG_MANAGE_CONTENT"}')

case "$GRANT_STATUS" in
  200|201|204|409) ;;
  *) cat /tmp/grant.json; exit 1 ;;
esac

