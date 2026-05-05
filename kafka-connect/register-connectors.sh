#!/bin/sh
# Register all connectors whose configs live in /opt/connectors/*.json
# The connector name is derived from the filename (e.g. jdbc-sink-eligibility.json -> jdbc-sink-eligibility).

CONNECT_URL="http://kafka-connect:8083"
CONFIG_DIR="/opt/connectors"

echo "Waiting for Kafka Connect to be ready..."
while ! curl -s "${CONNECT_URL}/connectors" > /dev/null 2>&1; do
  sleep 5
done
echo "Kafka Connect is ready."

for config_file in "${CONFIG_DIR}"/*.json; do
  connector_name=$(basename "${config_file}" .json)
  echo "Registering connector: ${connector_name} ..."
  curl -s -X PUT "${CONNECT_URL}/connectors/${connector_name}/config" \
    -H "Content-Type: application/json" \
    -d @"${config_file}"
  echo ""
done

echo "All connectors registered."
