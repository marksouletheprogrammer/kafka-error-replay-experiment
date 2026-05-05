package com.example.connect.simulator;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Map;

/**
 * Strategy for wrapping data records in different envelope formats.
 */
public interface EnvelopeStrategy {

    /**
     * Initialize envelope and key schemas from the base data schemas.
     */
    void initialize(Map<String, Schema> dataSchemas);

    /**
     * Create a ProducerRecord with the appropriate key and envelope-wrapped value.
     * Keys are always Avro-serialized (via KafkaAvroSerializer); the exact Avro shape
     * depends on the strategy.
     */
    ProducerRecord<Object, Object> createRecord(String topic, GenericRecord dataRecord);

    /**
     * Build a key for the given topic and id, in the same form used by {@link #createRecord}.
     * Used to attach valid keys to malformed (DLQ-bound) records so key serialization stays
     * consistent within a topic.
     */
    Object buildKey(String topic, long id);
}
