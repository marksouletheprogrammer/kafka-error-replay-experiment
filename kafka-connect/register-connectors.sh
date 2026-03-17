#!/bin/bash
# Wait for Kafka Connect to be ready
echo "Waiting for Kafka Connect to be ready..."
while ! curl -s http://kafka-connect:8083/connectors > /dev/null 2>&1; do
  sleep 5
done
echo "Kafka Connect is ready."

# ============================================
# Connector 1: Eligibility
# Topics: cdc.members, cdc.coverage, cdc.enrollment
# ============================================
echo "Registering jdbc-sink-eligibility connector..."
curl -s -X PUT http://kafka-connect:8083/connectors/jdbc-sink-eligibility/config \
  -H "Content-Type: application/json" \
  -d '{
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "tasks.max": "1",
    "topics": "cdc.members,cdc.coverage,cdc.enrollment",
    "connection.url": "jdbc:postgresql://postgres:5432/appdb",
    "connection.user": "postgres",
    "connection.password": "postgres",
    "auto.create": "false",
    "auto.evolve": "false",
    "insert.mode": "upsert",
    "pk.mode": "record_value",
    "pk.fields": "id",
    "table.name.format": "datastore.${topic}",
    "transforms": "stripPrefix",
    "transforms.stripPrefix.type": "org.apache.kafka.connect.transforms.RegexRouter",
    "transforms.stripPrefix.regex": "cdc\\.(.*)",
    "transforms.stripPrefix.replacement": "$1",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081",
    "errors.tolerance": "all",
    "errors.deadletterqueue.topic.name": "dlq-jdbc-sink-eligibility",
    "errors.deadletterqueue.topic.replication.factor": "1",
    "errors.deadletterqueue.context.headers.enable": "true",
    "errors.log.enable": "true",
    "errors.log.include.messages": "false"
  }'
echo ""

# ============================================
# Connector 2: Claims
# Topics: cdc.claims, cdc.claim_lines, cdc.adjustments
# ============================================
echo "Registering jdbc-sink-claims connector..."
curl -s -X PUT http://kafka-connect:8083/connectors/jdbc-sink-claims/config \
  -H "Content-Type: application/json" \
  -d '{
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "tasks.max": "1",
    "topics": "cdc.claims,cdc.claim_lines,cdc.adjustments",
    "connection.url": "jdbc:postgresql://postgres:5432/appdb",
    "connection.user": "postgres",
    "connection.password": "postgres",
    "auto.create": "false",
    "auto.evolve": "false",
    "insert.mode": "upsert",
    "pk.mode": "record_value",
    "pk.fields": "id",
    "table.name.format": "datastore.${topic}",
    "transforms": "stripPrefix",
    "transforms.stripPrefix.type": "org.apache.kafka.connect.transforms.RegexRouter",
    "transforms.stripPrefix.regex": "cdc\\.(.*)",
    "transforms.stripPrefix.replacement": "$1",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081",
    "errors.tolerance": "all",
    "errors.deadletterqueue.topic.name": "dlq-jdbc-sink-claims",
    "errors.deadletterqueue.topic.replication.factor": "1",
    "errors.deadletterqueue.context.headers.enable": "true",
    "errors.log.enable": "true",
    "errors.log.include.messages": "false"
  }'
echo ""

# ============================================
# Connector 3: Providers
# Topics: cdc.providers, cdc.facilities, cdc.networks
# ============================================
echo "Registering jdbc-sink-providers connector..."
curl -s -X PUT http://kafka-connect:8083/connectors/jdbc-sink-providers/config \
  -H "Content-Type: application/json" \
  -d '{
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "tasks.max": "1",
    "topics": "cdc.providers,cdc.facilities,cdc.networks",
    "connection.url": "jdbc:postgresql://postgres:5432/appdb",
    "connection.user": "postgres",
    "connection.password": "postgres",
    "auto.create": "false",
    "auto.evolve": "false",
    "insert.mode": "upsert",
    "pk.mode": "record_value",
    "pk.fields": "id",
    "table.name.format": "datastore.${topic}",
    "transforms": "stripPrefix",
    "transforms.stripPrefix.type": "org.apache.kafka.connect.transforms.RegexRouter",
    "transforms.stripPrefix.regex": "cdc\\.(.*)",
    "transforms.stripPrefix.replacement": "$1",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081",
    "errors.tolerance": "all",
    "errors.deadletterqueue.topic.name": "dlq-jdbc-sink-providers",
    "errors.deadletterqueue.topic.replication.factor": "1",
    "errors.deadletterqueue.context.headers.enable": "true",
    "errors.log.enable": "true",
    "errors.log.include.messages": "false"
  }'
echo ""

echo "All connectors registered."
