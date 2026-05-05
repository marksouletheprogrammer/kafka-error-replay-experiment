package com.example.connect.simulator;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Produces a mix of three record classes to demonstrate the full DLQ + replay flow:
 *
 * <ol>
 *   <li><b>Good records</b> &mdash; well-formed Avro records sent to the {@code cdc.*}
 *       source topics. The primary Debezium JDBC sinks consume these and upsert into
 *       {@code datastore.*}.</li>
 *   <li><b>Unrecoverable bad records</b> &mdash; random garbage bytes sent to
 *       {@code cdc.*}. The primary sinks fail to deserialize them and route them to
 *       {@code dlq-jdbc-sink-*}. The replay sink fails on them as well and sends them
 *       to {@code dlq-replay-terminal} (terminal failure, manual triage).</li>
 *   <li><b>Recoverable DLQ records</b> &mdash; well-formed Avro records published
 *       <em>directly</em> to a {@code dlq-jdbc-sink-*} topic with synthetic
 *       {@code __connect.errors.*} headers. These simulate a record that originally
 *       failed for a transient downstream reason (DB outage, FK lag, etc.). The
 *       {@code jdbc-sink-replay} connector consumes them, the
 *       {@code ReplayHeaderToTopic} SMT routes them back to {@code datastore.<table>}
 *       using the headers, and the upsert succeeds &mdash; demonstrating successful
 *       replay end-to-end. Their {@code id} is offset by
 *       {@link #RECOVERABLE_ID_OFFSET} so replayed rows are immediately
 *       identifiable in the destination tables
 *       ({@code WHERE id >= 9_000_000_000_000}).</li>
 * </ol>
 *
 * <p>Rates are layered: {@code ERROR_RATE} chooses good vs. failing; among failing
 * records, {@code RECOVERABLE_RATE} chooses recoverable-DLQ vs. unrecoverable. With the
 * defaults ({@code ERROR_RATE=0.3}, {@code RECOVERABLE_RATE=0.1}) the mix is roughly
 * 70% good, 27% unrecoverable, 3% recoverable.
 */
public class ErrorSimulator {

    private static final Logger log = LoggerFactory.getLogger(ErrorSimulator.class);

    private static final String BOOTSTRAP_SERVERS = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
    private static final String SCHEMA_REGISTRY_URL = System.getenv().getOrDefault("SCHEMA_REGISTRY_URL", "http://localhost:8081");
    private static final int BATCH_SIZE = Integer.parseInt(System.getenv().getOrDefault("BATCH_SIZE", "20"));
    private static final double ERROR_RATE = Double.parseDouble(System.getenv().getOrDefault("ERROR_RATE", "0.3"));
    private static final long INTERVAL_MS = Long.parseLong(System.getenv().getOrDefault("INTERVAL_MS", "5000"));
    private static final String ENVELOPE_FORMAT = System.getenv().getOrDefault("ENVELOPE_FORMAT", "flat");
    /**
     * Of the records selected to fail (per {@link #ERROR_RATE}), the fraction emitted
     * as recoverable-DLQ records (well-formed Avro published directly to a DLQ topic).
     * The remainder are unrecoverable garbage bytes sent to the primary {@code cdc.*}
     * topics. Default {@code 0.1}.
     */
    private static final double RECOVERABLE_RATE = Double.parseDouble(System.getenv().getOrDefault("RECOVERABLE_RATE", "0.1"));

    /**
     * Offset added to the {@code id} of every recoverable-DLQ record so the resulting
     * row in {@code datastore.<table>} is trivially identifiable as having come through
     * the replay path.
     *
     * <p>Normal records use ids in {@code [1, 100_000)}; replayed rows therefore land
     * at {@code >= 9_000_000_000_000} (well within the {@code bigint} range used by
     * every datastore PK column). Filter with:
     * <pre>{@code SELECT * FROM datastore.claims WHERE id >= 9000000000000;}</pre>
     */
    private static final long RECOVERABLE_ID_OFFSET = 9_000_000_000_000L;

    // Topic definitions grouped by connector
    private static final Map<String, List<String>> CONNECTOR_TOPICS = Map.of(
        "eligibility", List.of("cdc.members", "cdc.coverage", "cdc.enrollment"),
        "claims", List.of("cdc.claims", "cdc.claim_lines", "cdc.adjustments"),
        "providers", List.of("cdc.providers", "cdc.facilities", "cdc.networks")
    );

    /**
     * Reverse lookup: a {@code cdc.<table>} source topic to its owning connector group
     * (e.g. {@code cdc.members} -> {@code eligibility}). Used to derive both the DLQ
     * topic name ({@code dlq-jdbc-sink-<group>}) and the original connector name
     * ({@code jdbc-sink-<group>}) when emitting recoverable-DLQ records, so the
     * synthetic Connect error headers match what Kafka Connect would have written for
     * a real failure on that topic.
     */
    private static final Map<String, String> TOPIC_TO_CONNECTOR = buildTopicToConnector();

    private static Map<String, String> buildTopicToConnector() {
        Map<String, String> m = new HashMap<>();
        CONNECTOR_TOPICS.forEach((group, topics) -> topics.forEach(t -> m.put(t, group)));
        return Map.copyOf(m);
    }

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
        log.info("Batch Size: {}, Error Rate: {}, Recoverable Rate: {}, Interval: {}ms",
            BATCH_SIZE, ERROR_RATE, RECOVERABLE_RATE, INTERVAL_MS);
        log.info("Envelope Format: {}", ENVELOPE_FORMAT);

        // Wait for Schema Registry to be available
        waitForSchemaRegistry();

        // Initialize envelope strategy
        EnvelopeStrategy strategy = createStrategy(ENVELOPE_FORMAT);
        strategy.initialize(SCHEMAS);

        // The good and raw producers publish to the per-table cdc.* topics, where each
        // topic carries exactly one schema, so the default TopicNameStrategy works fine
        // and the JDBC sinks consume them with no extra config.
        Properties goodProps = new Properties();
        goodProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        goodProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        goodProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        goodProps.put("schema.registry.url", SCHEMA_REGISTRY_URL);
        goodProps.put("auto.register.schemas", "true");

        // Raw bytes producer for bad records (triggers deserialization errors on the VALUE only).
        // Same Avro key serializer as the good producer so keys stay consistent on the topic.
        Properties rawProps = new Properties();
        rawProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        rawProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        rawProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        rawProps.put("schema.registry.url", SCHEMA_REGISTRY_URL);
        rawProps.put("auto.register.schemas", "true");

        // Dedicated producer for the recoverable-DLQ path. We need TopicRecordNameStrategy
        // here because this producer publishes records of *different* envelope schemas
        // (e.g. Member / Coverage / Enrollment) to the *same* DLQ topic
        // (dlq-jdbc-sink-eligibility). Under the default TopicNameStrategy those would
        // collide on a single "<topic>-value" (or "<topic>-key") subject and SR would
        // reject the second envelope as BACKWARD-incompatible. TopicRecordNameStrategy
        // gives each record type its own subject so all three coexist on one topic.
        //
        // We deliberately keep this off goodProducer/rawProducer because the cdc.*
        // topics are single-schema and the primary JDBC sinks consume them with the
        // default TopicNameStrategy; mixing strategies there breaks the primary path
        // (the sink looks up "cdc.<table>-key" and gets a 40401 from SR).
        Properties dlqProps = new Properties();
        dlqProps.putAll(goodProps);
        dlqProps.put("key.subject.name.strategy", "io.confluent.kafka.serializers.subject.TopicRecordNameStrategy");
        dlqProps.put("value.subject.name.strategy", "io.confluent.kafka.serializers.subject.TopicRecordNameStrategy");

        try (KafkaProducer<Object, Object> goodProducer = new KafkaProducer<>(goodProps);
             KafkaProducer<Object, byte[]> rawProducer = new KafkaProducer<>(rawProps);
             KafkaProducer<Object, Object> dlqProducer = new KafkaProducer<>(dlqProps)) {

            List<String> allTopics = CONNECTOR_TOPICS.values().stream()
                .flatMap(Collection::stream)
                .toList();

            while (true) {
                for (int i = 0; i < BATCH_SIZE; i++) {
                    String topic = allTopics.get(ThreadLocalRandom.current().nextInt(allTopics.size()));
                    // Three-way branch:
                    //   p < (1 - ERROR_RATE)                              -> good record         (~70% default)
                    //   p < (1 - ERROR_RATE) + ERROR_RATE*(1-RECOVERABLE) -> unrecoverable bytes  (~27% default)
                    //   else                                              -> recoverable DLQ      (~ 3% default)
                    double p = ThreadLocalRandom.current().nextDouble();
                    double goodCutoff = 1.0 - ERROR_RATE;
                    double unrecoverableCutoff = goodCutoff + ERROR_RATE * (1.0 - RECOVERABLE_RATE);

                    if (p < goodCutoff) {
                        sendGoodRecord(goodProducer, topic, strategy);
                    } else if (p < unrecoverableCutoff) {
                        sendBadRecord(rawProducer, topic, strategy);
                    } else {
                        sendRecoverableDlqRecord(dlqProducer, topic, strategy);
                    }
                }
                log.info("Sent batch of {} messages (error rate: {}, recoverable rate: {}, format: {})",
                    BATCH_SIZE, ERROR_RATE, RECOVERABLE_RATE, ENVELOPE_FORMAT);
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

    private static void sendGoodRecord(KafkaProducer<Object, Object> producer, String topic, EnvelopeStrategy strategy) {
        Schema schema = SCHEMAS.get(topic);
        GenericRecord record = generateRecord(topic, schema);
        ProducerRecord<Object, Object> producerRecord = strategy.createRecord(topic, record);

        producer.send(producerRecord, (metadata, exception) -> {
            if (exception != null) {
                log.error("Failed to send valid record to {}: {}", topic, exception.getMessage());
            } else {
                log.debug("Sent valid record to {}[{}]@{}", metadata.topic(), metadata.partition(), metadata.offset());
            }
        });
    }

    /**
     * Emits a well-formed Avro record directly to a DLQ topic with synthetic Connect
     * error headers, simulating a record that the primary sink rejected for a transient
     * downstream reason (e.g. DB outage, foreign-key lag) rather than a deserialization
     * failure.
     *
     * <p>The headers attached are exactly the subset of {@code __connect.errors.*}
     * headers that {@link com.medica.connect.smt.ReplayHeaderToTopic} (configured on
     * {@code jdbc-sink-replay}) reads to reroute the record back to its original
     * destination table:
     * <ul>
     *   <li>{@code __connect.errors.topic} &mdash; the original {@code cdc.<table>}
     *       source topic. Used by the SMT to compute the destination as
     *       {@code <schema>.<sourceTopic>}.</li>
     *   <li>{@code __connect.errors.connector.name} &mdash; the original
     *       {@code jdbc-sink-<group>} connector. Used by the SMT to look up the
     *       Postgres schema in its {@code connector.schema.map}.</li>
     * </ul>
     * A few additional headers ({@code task.id}, {@code exception.class.name},
     * {@code exception.message}, {@code timestamp}) are included for fidelity with what
     * Kafka Connect would actually write — the SMT ignores them but they show up in
     * kafka-ui / DLQman so the record looks like a real DLQ entry.
     *
     * <p>The downstream contract: {@code jdbc-sink-replay} consumes from
     * {@code dlq-jdbc-sink-*}, the SMT rewrites the topic to e.g.
     * {@code datastore.cdc.members}, the {@code RegexRouter} strips the {@code cdc.}
     * prefix to {@code datastore.members}, and the Debezium sink upserts into that
     * table — making this record observably "replayed".
     *
     * <p>The record's {@code id} is shifted by {@link #RECOVERABLE_ID_OFFSET}
     * <em>before</em> the envelope is built, so both the Avro key (e.g. the Debezium
     * structured {@code {id: long}}) and the value's {@code after.id} carry the
     * sentinel. This makes replayed rows visually obvious and easy to filter:
     * {@code WHERE id >= 9_000_000_000_000}.
     */
    private static void sendRecoverableDlqRecord(KafkaProducer<Object, Object> producer, String sourceTopic, EnvelopeStrategy strategy) {
        String group = TOPIC_TO_CONNECTOR.get(sourceTopic);
        if (group == null) {
            log.warn("No connector mapping for topic '{}'; skipping recoverable DLQ record", sourceTopic);
            return;
        }
        String dlqTopic = "dlq-jdbc-sink-" + group;
        String connectorName = "jdbc-sink-" + group;

        Schema schema = SCHEMAS.get(sourceTopic);
        GenericRecord record = generateRecord(sourceTopic, schema);
        // Stamp the id with the sentinel offset BEFORE handing the record to the
        // envelope strategy. This way the Avro key (which strategies derive from id)
        // and the value's id field both carry the sentinel, so the row in Postgres is
        // unambiguously identifiable as a replayed record.
        long replayedId = ((Long) record.get("id")) + RECOVERABLE_ID_OFFSET;
        record.put("id", replayedId);
        // Build the full envelope (key + value) as if it were going to the source topic,
        // then re-target the ProducerRecord at the DLQ topic. Key/value bytes match what
        // the primary sink would have seen, so the replay sink decodes them identically.
        ProducerRecord<Object, Object> envelope = strategy.createRecord(sourceTopic, record);

        ProducerRecord<Object, Object> dlqRecord = new ProducerRecord<>(
            dlqTopic, null, envelope.key(), envelope.value());
        Headers headers = dlqRecord.headers();
        headers.add("__connect.errors.topic", sourceTopic.getBytes(StandardCharsets.UTF_8));
        headers.add("__connect.errors.connector.name", connectorName.getBytes(StandardCharsets.UTF_8));
        headers.add("__connect.errors.task.id", "0".getBytes(StandardCharsets.UTF_8));
        headers.add("__connect.errors.exception.class.name",
            "org.apache.kafka.connect.errors.RetriableException".getBytes(StandardCharsets.UTF_8));
        headers.add("__connect.errors.exception.message",
            "Simulated transient failure (recoverable)".getBytes(StandardCharsets.UTF_8));
        headers.add("__connect.errors.timestamp",
            Long.toString(System.currentTimeMillis()).getBytes(StandardCharsets.UTF_8));

        producer.send(dlqRecord, (metadata, exception) -> {
            if (exception != null) {
                log.error("Failed to send recoverable DLQ record to {}: {}", dlqTopic, exception.getMessage());
            } else {
                log.info("Sent RECOVERABLE record to {}[{}]@{} (replays as {}, id={})",
                    metadata.topic(), metadata.partition(), metadata.offset(), sourceTopic, replayedId);
            }
        });
    }

    private static void sendBadRecord(KafkaProducer<Object, byte[]> producer, String topic, EnvelopeStrategy strategy) {
        // Send garbage bytes that cannot be deserialized as Avro
        byte[] garbage = new byte[ThreadLocalRandom.current().nextInt(10, 100)];
        ThreadLocalRandom.current().nextBytes(garbage);
        // Ensure it's not accidentally valid Avro by prepending invalid magic byte
        garbage[0] = (byte) 0xFF;

        // Build a valid key via the active strategy so key serialization stays consistent
        // with good records on this topic. Only the VALUE is malformed.
        long id = ThreadLocalRandom.current().nextLong(1, 100000);
        Object key = strategy.buildKey(topic, id);

        producer.send(new ProducerRecord<>(topic, key, garbage), (metadata, exception) -> {
            if (exception != null) {
                log.error("Failed to send bad record to {}: {}", topic, exception.getMessage());
            } else {
                log.info("Sent BAD record to {}[{}]@{} (will trigger DLQ)", metadata.topic(), metadata.partition(), metadata.offset());
            }
        });
    }

    private static EnvelopeStrategy createStrategy(String format) {
        return switch (format.toLowerCase()) {
            case "debezium" -> new DebeziumEnvelopeStrategy();
            case "goldengate" -> new GoldenGateEnvelopeStrategy();
            default -> new FlatEnvelopeStrategy();
        };
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
