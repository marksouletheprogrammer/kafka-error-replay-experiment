package com.example.connect.replay.service;

import com.example.connect.replay.model.ErrorRecord;
import com.example.connect.replay.repository.ErrorRecordRepository;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

@Service
public class ReplayEngine {

    private static final Logger log = LoggerFactory.getLogger(ReplayEngine.class);

    @Value("${replay.max-replay-count:3}")
    private int maxReplayCount;

    private final ErrorRecordRepository repository;
    private final KafkaTemplate<byte[], byte[]> kafkaTemplate;
    private final JdbcTemplate jdbcTemplate;

    public ReplayEngine(ErrorRecordRepository repository,
                        KafkaTemplate<byte[], byte[]> kafkaTemplate,
                        JdbcTemplate jdbcTemplate) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Replay a single error record by ID.
     * @param id the error record UUID
     * @param mode "re-produce" or "direct-to-db"
     * @return the updated error record
     */
    public ErrorRecord replay(UUID id, String mode) {
        ErrorRecord record = repository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("Error record not found: " + id));

        if ("RESOLVED".equals(record.getStatus()) || "ABANDONED".equals(record.getStatus())) {
            throw new IllegalStateException("Cannot replay record in status: " + record.getStatus());
        }

        if (record.getReplayCount() >= maxReplayCount) {
            record.setStatus("ABANDONED");
            repository.save(record);
            log.warn("Record {} exceeded max replay count ({}), marking ABANDONED", id, maxReplayCount);
            throw new IllegalStateException("Record exceeded max replay count: " + maxReplayCount);
        }

        record.setStatus("REPLAYING");
        record.setReplayCount(record.getReplayCount() + 1);
        record.setLastReplayedAt(OffsetDateTime.now());
        repository.save(record);

        try {
            if ("direct-to-db".equalsIgnoreCase(mode)) {
                replayDirectToDb(record);
            } else {
                replayReproduce(record);
            }

            record.setStatus("RESOLVED");
            record.setResolvedAt(OffsetDateTime.now());
            repository.save(record);
            log.info("Record {} replayed successfully via {} mode", id, mode);

        } catch (Exception e) {
            log.error("Replay failed for record {}: {}", id, e.getMessage(), e);
            // Revert to PENDING so it can be retried
            if (record.getReplayCount() >= maxReplayCount) {
                record.setStatus("ABANDONED");
                log.warn("Record {} reached max replay count after failure, marking ABANDONED", id);
            } else {
                record.setStatus("PENDING");
            }
            repository.save(record);
            throw new RuntimeException("Replay failed: " + e.getMessage(), e);
        }

        return record;
    }

    /**
     * Bulk replay by filter criteria.
     */
    public List<ErrorRecord> replayBulk(String status, String connectorName, String topic, String mode) {
        List<ErrorRecord> records;

        if (connectorName != null && status != null) {
            records = repository.findByStatusAndConnectorName(status, connectorName);
        } else if (topic != null && status != null) {
            records = repository.findByStatusAndOriginalTopic(status, topic);
        } else if (status != null) {
            records = repository.findByStatus(status);
        } else if (connectorName != null) {
            records = repository.findByConnectorName(connectorName);
        } else {
            records = repository.findByStatus("PENDING");
        }

        log.info("Bulk replay: found {} records matching criteria", records.size());

        for (ErrorRecord record : records) {
            try {
                replay(record.getId(), mode);
            } catch (Exception e) {
                log.warn("Bulk replay: failed for record {}: {}", record.getId(), e.getMessage());
            }
        }

        return records;
    }

    /**
     * Mark a record as ABANDONED.
     */
    public ErrorRecord abandon(UUID id) {
        ErrorRecord record = repository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("Error record not found: " + id));

        record.setStatus("ABANDONED");
        return repository.save(record);
    }

    private void replayReproduce(ErrorRecord record) {
        String targetTopic = record.getOriginalTopic();
        if (targetTopic == null || targetTopic.isBlank()) {
            throw new IllegalStateException("Cannot re-produce: original topic is unknown");
        }

        ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(
                targetTopic, record.getMessageKey(), record.getMessagePayload());

        // Add a header marking this as a replayed message
        producerRecord.headers().add(new RecordHeader("X-Replay-Id",
                record.getId().toString().getBytes(StandardCharsets.UTF_8)));
        producerRecord.headers().add(new RecordHeader("X-Replay-Count",
                String.valueOf(record.getReplayCount()).getBytes(StandardCharsets.UTF_8)));

        kafkaTemplate.send(producerRecord);
        log.info("Re-produced record {} to topic {}", record.getId(), targetTopic);
    }

    private void replayDirectToDb(ErrorRecord record) {
        // For direct-to-DB mode, we attempt to insert the record directly.
        // In a real implementation, we'd deserialize the payload and map to the correct table.
        // For this prototype, we demonstrate the concept with a simple insert.
        String originalTopic = record.getOriginalTopic();
        if (originalTopic == null) {
            throw new IllegalStateException("Cannot direct-to-db: original topic is unknown");
        }

        // Map topic to datastore table
        String tableName = mapTopicToTable(originalTopic);
        if (tableName == null) {
            throw new IllegalStateException("Cannot map topic to table: " + originalTopic);
        }

        // For this prototype, direct-to-db replay is a placeholder that logs the action.
        // A real implementation would deserialize the Avro payload and perform the upsert.
        log.info("Direct-to-DB replay for record {} -> table {} (prototype: payload stored but not deserialized)",
                record.getId(), tableName);

        // Mark as resolved since we've "processed" it
        // In production, this would actually write to the DB and could fail
    }

    private String mapTopicToTable(String topic) {
        return switch (topic) {
            case "cdc.members" -> "datastore.members";
            case "cdc.coverage" -> "datastore.coverage";
            case "cdc.enrollment" -> "datastore.enrollment";
            case "cdc.claims" -> "datastore.claims";
            case "cdc.claim_lines" -> "datastore.claim_lines";
            case "cdc.adjustments" -> "datastore.adjustments";
            case "cdc.providers" -> "datastore.providers";
            case "cdc.facilities" -> "datastore.facilities";
            case "cdc.networks" -> "datastore.networks";
            default -> null;
        };
    }
}
