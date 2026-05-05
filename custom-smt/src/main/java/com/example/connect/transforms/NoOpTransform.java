package com.example.connect.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Map;

/**
 * A no-op Single Message Transform that passes records through unchanged.
 * Placeholder for a future DLQ-header-to-value transform.
 */
public class NoOpTransform<R extends ConnectRecord<R>> implements Transformation<R> {

    @Override
    public void configure(Map<String, ?> configs) {
        // no configuration needed
    }

    @Override
    public R apply(R record) {
        return record;
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public void close() {
        // nothing to clean up
    }
}
