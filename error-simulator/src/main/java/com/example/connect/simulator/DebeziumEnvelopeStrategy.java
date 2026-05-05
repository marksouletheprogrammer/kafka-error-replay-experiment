package com.example.connect.simulator;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Wraps data records in the standard Debezium change-event envelope.
 * Key is a structured Avro record containing the primary key field.
 */
public class DebeziumEnvelopeStrategy implements EnvelopeStrategy {

    private final Map<String, Schema> envelopeSchemas = new HashMap<>();
    private final Map<String, Schema> keySchemas = new HashMap<>();
    private final Map<String, Schema> sourceSchemas = new HashMap<>();

    @Override
    public void initialize(Map<String, Schema> dataSchemas) {
        for (Map.Entry<String, Schema> entry : dataSchemas.entrySet()) {
            String topic = entry.getKey();
            Schema dataSchema = entry.getValue();
            String namespace = "com.example.connect.cdc." + extractTableName(topic);

            // Key schema: structured record with the PK field
            Schema keySchema = SchemaBuilder.record("Key")
                .namespace(namespace)
                .fields()
                .requiredLong("id")
                .endRecord();
            keySchemas.put(topic, keySchema);

            // Source metadata schema
            Schema sourceSchema = SchemaBuilder.record("Source")
                .namespace(namespace)
                .fields()
                .requiredString("version")
                .requiredString("connector")
                .requiredString("name")
                .requiredLong("ts_ms")
                .requiredString("db")
                .requiredString("schema")
                .requiredString("table")
                .endRecord();
            sourceSchemas.put(topic, sourceSchema);

            // Debezium envelope schema
            Schema envelopeSchema = SchemaBuilder.record("Envelope")
                .namespace(namespace)
                .fields()
                .name("before").type().unionOf().nullType().and().type(dataSchema).endUnion().nullDefault()
                .name("after").type().unionOf().nullType().and().type(dataSchema).endUnion().nullDefault()
                .name("source").type(sourceSchema).noDefault()
                .requiredString("op")
                .name("ts_ms").type().unionOf().nullType().and().longType().endUnion().nullDefault()
                .endRecord();
            envelopeSchemas.put(topic, envelopeSchema);
        }
    }

    @Override
    public ProducerRecord<Object, Object> createRecord(String topic, GenericRecord dataRecord) {
        Schema envelopeSchema = envelopeSchemas.get(topic);
        Schema sourceSchema = sourceSchemas.get(topic);

        // Build structured key
        GenericRecord key = (GenericRecord) buildKey(topic, (long) dataRecord.get("id"));

        // Build source metadata
        long now = Instant.now().toEpochMilli();
        GenericRecord source = new GenericData.Record(sourceSchema);
        source.put("version", "2.7");
        source.put("connector", "simulator");
        source.put("name", "cdc");
        source.put("ts_ms", now);
        source.put("db", "appdb");
        source.put("schema", "datastore");
        source.put("table", extractTableName(topic));

        // Build envelope
        GenericRecord envelope = new GenericData.Record(envelopeSchema);
        envelope.put("before", null);
        envelope.put("after", dataRecord);
        envelope.put("source", source);
        envelope.put("op", "c");
        envelope.put("ts_ms", now);

        return new ProducerRecord<>(topic, key, envelope);
    }

    @Override
    public Object buildKey(String topic, long id) {
        Schema keySchema = keySchemas.get(topic);
        GenericRecord key = new GenericData.Record(keySchema);
        key.put("id", id);
        return key;
    }

    private static String extractTableName(String topic) {
        int dot = topic.lastIndexOf('.');
        return dot >= 0 ? topic.substring(dot + 1) : topic;
    }
}
