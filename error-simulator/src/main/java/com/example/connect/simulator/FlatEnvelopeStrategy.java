package com.example.connect.simulator;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Map;

/**
 * No envelope — sends the raw Avro data record with a string key.
 * This is the original/default behavior.
 */
public class FlatEnvelopeStrategy implements EnvelopeStrategy {

    @Override
    public void initialize(Map<String, Schema> dataSchemas) {
        // no envelope schemas needed
    }

    @Override
    public ProducerRecord<Object, Object> createRecord(String topic, GenericRecord dataRecord) {
        Object key = buildKey(topic, (long) dataRecord.get("id"));
        return new ProducerRecord<>(topic, key, dataRecord);
    }

    @Override
    public Object buildKey(String topic, long id) {
        // Avro primitive long key: KafkaAvroSerializer will register schema "long".
        return Long.valueOf(id);
    }
}
