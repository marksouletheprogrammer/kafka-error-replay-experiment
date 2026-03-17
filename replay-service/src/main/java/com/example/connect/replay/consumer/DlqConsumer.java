package com.example.connect.replay.consumer;

import com.example.connect.replay.model.ErrorRecord;
import com.example.connect.replay.repository.ErrorRecordRepository;
import com.example.connect.replay.service.ErrorClassifier;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Component
public class DlqConsumer {

    private static final Logger log = LoggerFactory.getLogger(DlqConsumer.class);

    private static final String HEADER_TOPIC = "__connect.errors.topic";
    private static final String HEADER_PARTITION = "__connect.errors.partition";
    private static final String HEADER_OFFSET = "__connect.errors.offset";
    private static final String HEADER_EXCEPTION_CLASS = "__connect.errors.exception.class.name";
    private static final String HEADER_EXCEPTION_MESSAGE = "__connect.errors.exception.message";

    private final ErrorRecordRepository repository;
    private final ErrorClassifier classifier;

    public DlqConsumer(ErrorRecordRepository repository, ErrorClassifier classifier) {
        this.repository = repository;
        this.classifier = classifier;
    }

    @KafkaListener(topics = {"dlq-jdbc-sink-eligibility", "dlq-jdbc-sink-claims", "dlq-jdbc-sink-providers"},
                   groupId = "replay-service")
    public void consume(ConsumerRecord<byte[], byte[]> record) {
        log.info("Received DLQ message from topic={} partition={} offset={}",
                record.topic(), record.partition(), record.offset());

        ErrorRecord errorRecord = new ErrorRecord();

        // Derive connector name from DLQ topic name
        String connectorName = deriveConnectorName(record.topic());
        errorRecord.setConnectorName(connectorName);

        // Parse Connect error headers
        errorRecord.setOriginalTopic(getHeader(record, HEADER_TOPIC));
        errorRecord.setOriginalPartition(parseIntHeader(record, HEADER_PARTITION));
        errorRecord.setOriginalOffset(parseLongHeader(record, HEADER_OFFSET));

        String exceptionClass = getHeader(record, HEADER_EXCEPTION_CLASS);
        String exceptionMessage = getHeader(record, HEADER_EXCEPTION_MESSAGE);

        String fullErrorMessage = buildErrorMessage(exceptionClass, exceptionMessage);
        errorRecord.setErrorCode(classifier.classify(exceptionClass, fullErrorMessage));
        errorRecord.setErrorMessage(fullErrorMessage);

        // Store original key and payload
        errorRecord.setMessageKey(record.key());
        errorRecord.setMessagePayload(record.value());

        errorRecord.setStatus("PENDING");

        ErrorRecord saved = repository.save(errorRecord);
        log.info("Stored error record id={} connector={} originalTopic={} errorCode={}",
                saved.getId(), connectorName, saved.getOriginalTopic(), saved.getErrorCode());
    }

    private String deriveConnectorName(String dlqTopic) {
        // dlq-jdbc-sink-eligibility -> jdbc-sink-eligibility
        if (dlqTopic != null && dlqTopic.startsWith("dlq-")) {
            return dlqTopic.substring(4);
        }
        return dlqTopic;
    }

    private String getHeader(ConsumerRecord<byte[], byte[]> record, String key) {
        Header header = record.headers().lastHeader(key);
        if (header != null && header.value() != null) {
            return new String(header.value(), StandardCharsets.UTF_8);
        }
        return null;
    }

    private Integer parseIntHeader(ConsumerRecord<byte[], byte[]> record, String key) {
        String val = getHeader(record, key);
        if (val != null) {
            try {
                return Integer.parseInt(val);
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }

    private Long parseLongHeader(ConsumerRecord<byte[], byte[]> record, String key) {
        String val = getHeader(record, key);
        if (val != null) {
            try {
                return Long.parseLong(val);
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }

    private String buildErrorMessage(String exceptionClass, String exceptionMessage) {
        StringBuilder sb = new StringBuilder();
        if (exceptionClass != null) {
            sb.append(exceptionClass);
        }
        if (exceptionMessage != null) {
            if (sb.length() > 0) sb.append(": ");
            sb.append(exceptionMessage);
        }
        return sb.length() > 0 ? sb.toString() : "Unknown error";
    }
}
