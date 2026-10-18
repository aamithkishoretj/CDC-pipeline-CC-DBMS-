#!/bin/bash
# Registers the Debezium Postgres connector with Kafka Connect.
# PUT /connectors/<name>/config is idempotent: it creates the connector the
# first time and updates it on every later run, so this is safe to re-run.
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
NAME="shopdb-connector"

echo "Waiting for Kafka Connect at ${CONNECT_URL} ..."
until curl -fs "${CONNECT_URL}/connectors" >/dev/null; do
  sleep 3
done

echo "Registering ${NAME} ..."
curl -fs -X PUT -H "Content-Type: application/json" \
     --data @/debezium/shopdb-connector.json \
     "${CONNECT_URL}/connectors/${NAME}/config"
echo

for i in $(seq 1 20); do
  STATUS=$(curl -fs "${CONNECT_URL}/connectors/${NAME}/status" || true)
  if echo "${STATUS}" | grep -q '"state":"RUNNING"' && ! echo "${STATUS}" | grep -q '"state":"FAILED"'; then
    echo "Connector is RUNNING:"
    echo "${STATUS}"
    exit 0
  fi
  sleep 3
done

echo "Connector did not reach RUNNING state:"
echo "${STATUS}"
exit 1
