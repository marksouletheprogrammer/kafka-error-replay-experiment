package com.example.connect.replay.service;

import org.springframework.stereotype.Component;

/**
 * Classifies error types based on the exception class name from
 * Kafka Connect DLQ headers.
 */
@Component
public class ErrorClassifier {

    public String classify(String exceptionClassName, String errorMessage) {
        String combined = ((exceptionClassName != null ? exceptionClassName : "") + " " +
                           (errorMessage != null ? errorMessage : "")).toLowerCase();

        if (combined.isBlank()) {
            return "UNKNOWN";
        }

        if (combined.contains("deserializ") || combined.contains("serializ")) {
            return "DESERIALIZATION";
        }
        if (combined.contains("convert") || combined.contains("avro")) {
            return "CONVERTER";
        }
        if (combined.contains("transform") || combined.contains("smt")) {
            return "SMT";
        }
        if (combined.contains("schema")) {
            return "SCHEMA_MISMATCH";
        }

        return "UNKNOWN";
    }
}
