#!/bin/sh
set -eu

apk add --no-cache jq >/dev/null

TOKEN=$(curl -sS --fail-with-body \
  -u "$POLARIS_CLIENT_ID:$POLARIS_CLIENT_SECRET" \
  -H "Polaris-Realm: $POLARIS_REALM" \
  -d grant_type=client_credentials \
  -d scope=PRINCIPAL_ROLE:ALL \
  http://polaris:8181/api/catalog/v1/oauth/tokens | jq -r .access_token)

test -n "$TOKEN"
test "$TOKEN" != "null"

STATUS=$(curl -sS -o /tmp/catalog.json -w '%{http_code}' \
  -H "Authorization: Bearer $TOKEN" \
  -H "Polaris-Realm: $POLARIS_REALM" \
  http://polaris:8181/api/management/v1/catalogs/shopvn_catalog)

if [ "$STATUS" = "404" ]; then
  curl -sS --fail-with-body -X POST \
    -H "Authorization: Bearer $TOKEN" \
    -H "Polaris-Realm: $POLARIS_REALM" \
    -H "Content-Type: application/json" \
    http://polaris:8181/api/management/v1/catalogs \
    -d '{"catalog":{"name":"shopvn_catalog","type":"INTERNAL","readOnly":false,"properties":{"default-base-location":"s3://shopvn-lakehouse/warehouse"},"storageConfigInfo":{"storageType":"S3","endpoint":"http://minio:9000","endpointInternal":"http://minio:9000","pathStyleAccess":true,"region":"us-east-1"}}}'
elif [ "$STATUS" != "200" ]; then
  cat /tmp/catalog.json
  exit 1
fi

GRANT_STATUS=$(curl -sS -o /tmp/grant.json -w '%{http_code}' -X PUT \
  -H "Authorization: Bearer $TOKEN" \
  -H "Polaris-Realm: $POLARIS_REALM" \
  -H "Content-Type: application/json" \
  http://polaris:8181/api/management/v1/catalogs/shopvn_catalog/catalog-roles/catalog_admin/grants \
  -d '{"type":"catalog","privilege":"CATALOG_MANAGE_CONTENT"}')

case "$GRANT_STATUS" in
  200|201|204|409) ;;
  *) cat /tmp/grant.json; exit 1 ;;
esac
