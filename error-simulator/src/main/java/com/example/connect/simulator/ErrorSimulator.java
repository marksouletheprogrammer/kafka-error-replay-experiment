package com.example.connect.simulator;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Produces a mix of valid Avro records and intentionally malformed records
 * to Kafka topics. Valid records are processed normally by the JDBC Sink
 * Connector; malformed records trigger deserialization failures and get
 * routed to the DLQ topics.
 */
public class ErrorSimulator {

    private static final Logger log = LoggerFactory.getLogger(ErrorSimulator.class);

    private static final String BOOTSTRAP_SERVERS = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
    private static final String SCHEMA_REGISTRY_URL = System.getenv().getOrDefault("SCHEMA_REGISTRY_URL", "http://localhost:8081");
    private static final int BATCH_SIZE = Integer.parseInt(System.getenv().getOrDefault("BATCH_SIZE", "20"));
    private static final double ERROR_RATE = Double.parseDouble(System.getenv().getOrDefault("ERROR_RATE", "0.3"));
    private static final long INTERVAL_MS = Long.parseLong(System.getenv().getOrDefault("INTERVAL_MS", "5000"));

    // Topic definitions grouped by connector
    private static final Map<String, List<String>> CONNECTOR_TOPICS = Map.of(
        "eligibility", List.of("cdc.members", "cdc.coverage", "cdc.enrollment"),
        "claims", List.of("cdc.claims", "cdc.claim_lines", "cdc.adjustments"),
        "providers", List.of("cdc.providers", "cdc.facilities", "cdc.networks")
    );

    // Avro schemas for each topic
    private static final Map<String, Schema> SCHEMAS = new HashMap<>();

    static {
        SCHEMAS.put("cdc.members", new Schema.Parser().parse(
            "{\"type\":\"record\",\"name\":\"Member\",\"namespace\":\"com.example.connect.cdc\"," +
            "\"fields\":[" +
            "{\"name\":\"id\",\"type\":\"long\"}," +
            "{\"name\":\"first_name\",\"type\":\"string\"}," +
            "{\"name\":\"last_name\",\"type\":\"string\"}," +
            "{\"name\":\"email\",\"type\":\"string\"}," +
            "{\"name\":\"date_of_birth\",\"type\":\"string\"}," +
            "{\"name\":\"source_pos\",\"type\":\"long\"}" +
            "]}"));

        SCHEMAS.put("cdc.coverage", new Schema.Parser().parse(
            "{\"type\":\"record\",\"name\":\"Coverage\",\"namespace\":\"com.example.connect.cdc\"," +
            "\"fields\":[" +
            "{\"name\":\"id\",\"type\":\"long\"}," +
            "{\"name\":\"member_id\",\"type\":\"long\"}," +
            "{\"name\":\"plan_code\",\"type\":\"string\"}," +
            "{\"name\":\"effective_date\",\"type\":\"string\"}," +
            "{\"name\":\"termination_date\",\"type\":[\"null\",\"string\"],\"default\":null}," +
            "{\"name\":\"source_pos\",\"type\":\"long\"}" +
            "]}"));

        SCHEMAS.put("cdc.enrollment", new Schema.Parser().parse(
            "{\"type\":\"record\",\"name\":\"Enrollment\",\"namespace\":\"com.example.connect.cdc\"," +
            "\"fields\":[" +
            "{\"name\":\"id\",\"type\":\"long\"}," +
            "{\"name\":\"member_id\",\"type\":\"long\"}," +
            "{\"name\":\"program\",\"type\":\"string\"}," +
            "{\"name\":\"status\",\"type\":\"string\"}," +
            "{\"name\":\"enrollment_date\",\"type\":\"string\"}," +
            "{\"name\":\"source_pos\",\"type\":\"long\"}" +
            "]}"));

        SCHEMAS.put("cdc.claims", new Schema.Parser().parse(
            "{\"type\":\"record\",\"name\":\"Claim\",\"namespace\":\"com.example.connect.cdc\"," +
            "\"fields\":[" +
            "{\"name\":\"id\",\"type\":\"long\"}," +
            "{\"name\":\"member_id\",\"type\":\"long\"}," +
            "{\"name\":\"provider_id\",\"type\":\"long\"}," +
            "{\"name\":\"claim_date\",\"type\":\"string\"}," +
            "{\"name\":\"total_amount\",\"type\":\"double\"}," +
            "{\"name\":\"status\",\"type\":\"string\"}," +
            "{\"name\":\"source_pos\",\"type\":\"long\"}" +
            "]}"));

        SCHEMAS.put("cdc.claim_lines", new Schema.Parser().parse(
            "{\"type\":\"record\",\"name\":\"ClaimLine\",\"namespace\":\"com.example.connect.cdc\"," +
            "\"fields\":[" +
            "{\"name\":\"id\",\"type\":\"long\"}," +
            "{\"name\":\"claim_id\",\"type\":\"long\"}," +
            "{\"name\":\"line_number\",\"type\":\"int\"}," +
            "{\"name\":\"procedure_code\",\"type\":\"string\"}," +
            "{\"name\":\"amount\",\"type\":\"double\"}," +
            "{\"name\":\"source_pos\",\"type\":\"long\"}" +
            "]}"));

        SCHEMAS.put("cdc.adjustments", new Schema.Parser().parse(
            "{\"type\":\"record\",\"name\":\"Adjustment\",\"namespace\":\"com.example.connect.cdc\"," +
            "\"fields\":[" +
            "{\"name\":\"id\",\"type\":\"long\"}," +
            "{\"name\":\"claim_id\",\"type\":\"long\"}," +
            "{\"name\":\"adjustment_type\",\"type\":\"string\"}," +
            "{\"name\":\"amount\",\"type\":\"double\"}," +
            "{\"name\":\"reason_code\",\"type\":\"string\"}," +
            "{\"name\":\"source_pos\",\"type\":\"long\"}" +
            "]}"));

        SCHEMAS.put("cdc.providers", new Schema.Parser().parse(
            "{\"type\":\"record\",\"name\":\"Provider\",\"namespace\":\"com.example.connect.cdc\"," +
            "\"fields\":[" +
            "{\"name\":\"id\",\"type\":\"long\"}," +
            "{\"name\":\"npi\",\"type\":\"string\"}," +
            "{\"name\":\"name\",\"type\":\"string\"}," +
            "{\"name\":\"specialty\",\"type\":\"string\"}," +
            "{\"name\":\"tax_id\",\"type\":\"string\"}," +
            "{\"name\":\"source_pos\",\"type\":\"long\"}" +
            "]}"));

        SCHEMAS.put("cdc.facilities", new Schema.Parser().parse(
            "{\"type\":\"record\",\"name\":\"Facility\",\"namespace\":\"com.example.connect.cdc\"," +
            "\"fields\":[" +
            "{\"name\":\"id\",\"type\":\"long\"}," +
            "{\"name\":\"name\",\"type\":\"string\"}," +
            "{\"name\":\"facility_type\",\"type\":\"string\"}," +
            "{\"name\":\"address\",\"type\":\"string\"}," +
            "{\"name\":\"state\",\"type\":\"string\"}," +
            "{\"name\":\"source_pos\",\"type\":\"long\"}" +
            "]}"));

        SCHEMAS.put("cdc.networks", new Schema.Parser().parse(
            "{\"type\":\"record\",\"name\":\"Network\",\"namespace\":\"com.example.connect.cdc\"," +
            "\"fields\":[" +
            "{\"name\":\"id\",\"type\":\"long\"}," +
            "{\"name\":\"network_name\",\"type\":\"string\"}," +
            "{\"name\":\"network_type\",\"type\":\"string\"}," +
            "{\"name\":\"region\",\"type\":\"string\"}," +
            "{\"name\":\"effective_date\",\"type\":\"string\"}," +
            "{\"name\":\"source_pos\",\"type\":\"long\"}" +
            "]}"));
    }

    private static long posCounter = System.currentTimeMillis();

    public static void main(String[] args) throws InterruptedException {
        log.info("Starting Error Simulator");
        log.info("Bootstrap Servers: {}", BOOTSTRAP_SERVERS);
        log.info("Schema Registry: {}", SCHEMA_REGISTRY_URL);
        log.info("Batch Size: {}, Error Rate: {}, Interval: {}ms", BATCH_SIZE, ERROR_RATE, INTERVAL_MS);

        // Wait for Schema Registry to be available
        waitForSchemaRegistry();

        // Avro producer for valid records
        Properties avroProps = new Properties();
        avroProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        avroProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        avroProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        avroProps.put("schema.registry.url", SCHEMA_REGISTRY_URL);
        avroProps.put("auto.register.schemas", "true");

        // Raw bytes producer for bad records (triggers deserialization errors)
        Properties rawProps = new Properties();
        rawProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        rawProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        rawProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        try (KafkaProducer<String, GenericRecord> avroProducer = new KafkaProducer<>(avroProps);
             KafkaProducer<String, byte[]> rawProducer = new KafkaProducer<>(rawProps)) {

            List<String> allTopics = CONNECTOR_TOPICS.values().stream()
                .flatMap(Collection::stream)
                .toList();

            while (true) {
                for (int i = 0; i < BATCH_SIZE; i++) {
                    String topic = allTopics.get(ThreadLocalRandom.current().nextInt(allTopics.size()));
                    boolean shouldFail = ThreadLocalRandom.current().nextDouble() < ERROR_RATE;

                    if (shouldFail) {
                        sendBadRecord(rawProducer, topic);
                    } else {
                        sendGoodRecord(avroProducer, topic);
                    }
                }
                log.info("Sent batch of {} messages (error rate: {})", BATCH_SIZE, ERROR_RATE);
                Thread.sleep(INTERVAL_MS);
            }
        }
    }

    private static void waitForSchemaRegistry() throws InterruptedException {
        log.info("Waiting for Schema Registry at {}...", SCHEMA_REGISTRY_URL);
        int attempts = 0;
        while (attempts < 60) {
            try {
                java.net.HttpURLConnection conn = (java.net.HttpURLConnection)
                    new java.net.URL(SCHEMA_REGISTRY_URL).openConnection();
                conn.setRequestMethod("GET");
                conn.setConnectTimeout(2000);
                conn.setReadTimeout(2000);
                if (conn.getResponseCode() == 200) {
                    log.info("Schema Registry is available.");
                    return;
                }
            } catch (Exception e) {
                // ignore
            }
            attempts++;
            Thread.sleep(3000);
        }
        log.warn("Schema Registry not available after retries, proceeding anyway.");
    }

    private static void sendGoodRecord(KafkaProducer<String, GenericRecord> producer, String topic) {
        Schema schema = SCHEMAS.get(topic);
        GenericRecord record = generateRecord(topic, schema);
        String key = String.valueOf(record.get("id"));

        producer.send(new ProducerRecord<>(topic, key, record), (metadata, exception) -> {
            if (exception != null) {
                log.error("Failed to send valid record to {}: {}", topic, exception.getMessage());
            } else {
                log.debug("Sent valid record to {}[{}]@{}", metadata.topic(), metadata.partition(), metadata.offset());
            }
        });
    }

    private static void sendBadRecord(KafkaProducer<String, byte[]> producer, String topic) {
        // Send garbage bytes that cannot be deserialized as Avro
        byte[] garbage = new byte[ThreadLocalRandom.current().nextInt(10, 100)];
        ThreadLocalRandom.current().nextBytes(garbage);
        // Ensure it's not accidentally valid Avro by prepending invalid magic byte
        garbage[0] = (byte) 0xFF;

        String key = "bad-" + ThreadLocalRandom.current().nextInt(10000);

        producer.send(new ProducerRecord<>(topic, key, garbage), (metadata, exception) -> {
            if (exception != null) {
                log.error("Failed to send bad record to {}: {}", topic, exception.getMessage());
            } else {
                log.info("Sent BAD record to {}[{}]@{} (will trigger DLQ)", metadata.topic(), metadata.partition(), metadata.offset());
            }
        });
    }

    private static GenericRecord generateRecord(String topic, Schema schema) {
        GenericRecord record = new GenericData.Record(schema);
        long id = ThreadLocalRandom.current().nextLong(1, 100000);
        long pos = posCounter++;

        switch (topic) {
            case "cdc.members" -> {
                record.put("id", id);
                record.put("first_name", randomFrom("John", "Jane", "Alice", "Bob", "Carol", "Dave"));
                record.put("last_name", randomFrom("Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia"));
                record.put("email", "user" + id + "@example.com");
                record.put("date_of_birth", randomDate(1950, 2005));
                record.put("source_pos", pos);
            }
            case "cdc.coverage" -> {
                record.put("id", id);
                record.put("member_id", ThreadLocalRandom.current().nextLong(1, 100000));
                record.put("plan_code", randomFrom("HMO-100", "PPO-200", "EPO-300", "POS-400"));
                record.put("effective_date", randomDate(2023, 2025));
                record.put("termination_date", null);
                record.put("source_pos", pos);
            }
            case "cdc.enrollment" -> {
                record.put("id", id);
                record.put("member_id", ThreadLocalRandom.current().nextLong(1, 100000));
                record.put("program", randomFrom("Medicare", "Medicaid", "Commercial", "Exchange"));
                record.put("status", randomFrom("ACTIVE", "PENDING", "TERMINATED"));
                record.put("enrollment_date", randomDate(2023, 2025));
                record.put("source_pos", pos);
            }
            case "cdc.claims" -> {
                record.put("id", id);
                record.put("member_id", ThreadLocalRandom.current().nextLong(1, 100000));
                record.put("provider_id", ThreadLocalRandom.current().nextLong(1, 10000));
                record.put("claim_date", randomDate(2024, 2025));
                record.put("total_amount", ThreadLocalRandom.current().nextDouble(50.0, 50000.0));
                record.put("status", randomFrom("SUBMITTED", "ADJUDICATED", "PAID", "DENIED"));
                record.put("source_pos", pos);
            }
            case "cdc.claim_lines" -> {
                record.put("id", id);
                record.put("claim_id", ThreadLocalRandom.current().nextLong(1, 100000));
                record.put("line_number", ThreadLocalRandom.current().nextInt(1, 10));
                record.put("procedure_code", randomFrom("99213", "99214", "99215", "99281", "99282"));
                record.put("amount", ThreadLocalRandom.current().nextDouble(25.0, 5000.0));
                record.put("source_pos", pos);
            }
            case "cdc.adjustments" -> {
                record.put("id", id);
                record.put("claim_id", ThreadLocalRandom.current().nextLong(1, 100000));
                record.put("adjustment_type", randomFrom("PAYMENT", "REVERSAL", "CORRECTION", "WRITEOFF"));
                record.put("amount", ThreadLocalRandom.current().nextDouble(-5000.0, 5000.0));
                record.put("reason_code", randomFrom("CO-45", "CO-97", "PR-1", "OA-23"));
                record.put("source_pos", pos);
            }
            case "cdc.providers" -> {
                record.put("id", id);
                record.put("npi", String.format("%010d", ThreadLocalRandom.current().nextLong(1000000000L)));
                record.put("name", "Dr. " + randomFrom("Smith", "Patel", "Lee", "Garcia", "Chen"));
                record.put("specialty", randomFrom("Cardiology", "Orthopedics", "Oncology", "Pediatrics", "Internal Medicine"));
                record.put("tax_id", String.format("%09d", ThreadLocalRandom.current().nextLong(100000000L)));
                record.put("source_pos", pos);
            }
            case "cdc.facilities" -> {
                record.put("id", id);
                record.put("name", randomFrom("Metro General", "Riverside Medical", "City Hospital", "Valley Health"));
                record.put("facility_type", randomFrom("HOSPITAL", "CLINIC", "LAB", "PHARMACY"));
                record.put("address", ThreadLocalRandom.current().nextInt(100, 9999) + " Main St");
                record.put("state", randomFrom("MN", "WI", "IA", "ND", "SD"));
                record.put("source_pos", pos);
            }
            case "cdc.networks" -> {
                record.put("id", id);
                record.put("network_name", randomFrom("BlueSelect", "GoldPlus", "SilverCare", "PlatinumPrime"));
                record.put("network_type", randomFrom("HMO", "PPO", "EPO", "POS"));
                record.put("region", randomFrom("Twin Cities", "Greater MN", "Western WI", "National"));
                record.put("effective_date", randomDate(2023, 2025));
                record.put("source_pos", pos);
            }
        }
        return record;
    }

    private static String randomFrom(String... options) {
        return options[ThreadLocalRandom.current().nextInt(options.length)];
    }

    private static String randomDate(int startYear, int endYear) {
        int year = ThreadLocalRandom.current().nextInt(startYear, endYear + 1);
        int month = ThreadLocalRandom.current().nextInt(1, 13);
        int day = ThreadLocalRandom.current().nextInt(1, 29);
        return LocalDate.of(year, month, day).toString();
    }
}
