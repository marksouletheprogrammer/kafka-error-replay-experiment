package com.example.connect.simulator;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Wraps data records in the Oracle GoldenGate envelope format.
 * Key is a plain Avro string (not a structured record). Uses I/U/D for op_type.
 * Data fields use native Avro types (same rules as Debezium).
 */
public class GoldenGateEnvelopeStrategy implements EnvelopeStrategy {

    private static final DateTimeFormatter TS_FORMAT =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS").withZone(ZoneOffset.UTC);

    private final Map<String, Schema> envelopeSchemas = new HashMap<>();
    private long posCounter = 0;

    @Override
    public void initialize(Map<String, Schema> dataSchemas) {
        for (Map.Entry<String, Schema> entry : dataSchemas.entrySet()) {
            String topic = entry.getKey();
            Schema dataSchema = entry.getValue();
            String namespace = "com.example.connect.cdc." + extractTableName(topic);

            Schema envelopeSchema = SchemaBuilder.record("GoldenGateEnvelope")
                .namespace(namespace)
                .fields()
                .requiredString("table")
                .requiredString("op_type")
                .requiredString("op_ts")
                .requiredString("current_ts")
                .requiredString("pos")
                .name("primary_keys").type().array().items().stringType().noDefault()
                .name("before").type().unionOf().nullType().and().type(dataSchema).endUnion().nullDefault()
                .name("after").type().unionOf().nullType().and().type(dataSchema).endUnion().nullDefault()
                .endRecord();
            envelopeSchemas.put(topic, envelopeSchema);
        }
    }

    @Override
    public ProducerRecord<Object, Object> createRecord(String topic, GenericRecord dataRecord) {
        Schema envelopeSchema = envelopeSchemas.get(topic);
        String tableName = "DATASTORE." + extractTableName(topic);
        Instant now = Instant.now();

        // Build primary_keys array
        Schema pkArraySchema = envelopeSchema.getField("primary_keys").schema();
        GenericData.Array<String> primaryKeys = new GenericData.Array<>(pkArraySchema, List.of("id"));

        // Build envelope
        GenericRecord envelope = new GenericData.Record(envelopeSchema);
        envelope.put("table", tableName);
        envelope.put("op_type", "I");
        envelope.put("op_ts", TS_FORMAT.format(now));
        envelope.put("current_ts", TS_FORMAT.format(now));
        envelope.put("pos", String.format("%020d", posCounter++));
        envelope.put("primary_keys", primaryKeys);
        envelope.put("before", null);
        envelope.put("after", dataRecord);

        // GoldenGate key is an Avro primitive string (KafkaAvroSerializer infers "string" schema).
        Object key = buildKey(topic, (long) dataRecord.get("id"));
        return new ProducerRecord<>(topic, key, envelope);
    }

    @Override
    public Object buildKey(String topic, long id) {
        // Java String -> Avro primitive string on the wire.
        return Long.toString(id);
    }

    private static String extractTableName(String topic) {
        int dot = topic.lastIndexOf('.');
        return dot >= 0 ? topic.substring(dot + 1) : topic;
    }
}
