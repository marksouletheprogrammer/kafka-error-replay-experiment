package com.example.connect.replay.controller;

import com.example.connect.replay.model.ErrorRecord;
import com.example.connect.replay.repository.ErrorRecordRepository;
import com.example.connect.replay.service.ReplayEngine;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/errors")
public class ErrorController {

    private final ErrorRecordRepository repository;
    private final ReplayEngine replayEngine;

    public ErrorController(ErrorRecordRepository repository, ReplayEngine replayEngine) {
        this.repository = repository;
        this.replayEngine = replayEngine;
    }

    @GetMapping
    public ResponseEntity<List<ErrorRecord>> listErrors(
            @RequestParam(required = false) String status,
            @RequestParam(required = false) String connector,
            @RequestParam(required = false) String topic) {

        List<ErrorRecord> results;

        if (status != null && connector != null) {
            results = repository.findByStatusAndConnectorName(status, connector);
        } else if (status != null && topic != null) {
            results = repository.findByStatusAndOriginalTopic(status, topic);
        } else if (status != null) {
            results = repository.findByStatus(status);
        } else if (connector != null) {
            results = repository.findByConnectorName(connector);
        } else if (topic != null) {
            results = repository.findByOriginalTopic(topic);
        } else {
            results = repository.findAll();
        }

        return ResponseEntity.ok(results);
    }

    @GetMapping("/{id}")
    public ResponseEntity<ErrorRecord> getError(@PathVariable UUID id) {
        return repository.findById(id)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @PostMapping("/{id}/replay")
    public ResponseEntity<?> replayOne(
            @PathVariable UUID id,
            @RequestParam(defaultValue = "re-produce") String mode) {
        try {
            ErrorRecord result = replayEngine.replay(id, mode);
            return ResponseEntity.ok(result);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.notFound().build();
        } catch (IllegalStateException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        } catch (RuntimeException e) {
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/replay")
    public ResponseEntity<?> replayBulk(
            @RequestParam(defaultValue = "PENDING") String status,
            @RequestParam(required = false) String connector,
            @RequestParam(required = false) String topic,
            @RequestParam(defaultValue = "re-produce") String mode) {
        try {
            List<ErrorRecord> results = replayEngine.replayBulk(status, connector, topic, mode);
            return ResponseEntity.ok(Map.of(
                    "message", "Bulk replay initiated",
                    "count", results.size()
            ));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/{id}/abandon")
    public ResponseEntity<?> abandon(@PathVariable UUID id) {
        try {
            ErrorRecord result = replayEngine.abandon(id);
            return ResponseEntity.ok(result);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.notFound().build();
        }
    }

    @GetMapping("/stats")
    public ResponseEntity<Map<String, Object>> stats() {
        Map<String, Object> stats = new LinkedHashMap<>();

        // Counts by status
        Map<String, Long> byStatus = repository.countByStatus().stream()
                .collect(Collectors.toMap(
                        row -> (String) row[0],
                        row -> (Long) row[1],
                        (a, b) -> a,
                        LinkedHashMap::new));
        stats.put("byStatus", byStatus);

        // Counts by error code
        Map<String, Long> byErrorCode = repository.countByErrorCode().stream()
                .collect(Collectors.toMap(
                        row -> row[0] != null ? (String) row[0] : "UNKNOWN",
                        row -> (Long) row[1],
                        (a, b) -> a,
                        LinkedHashMap::new));
        stats.put("byErrorCode", byErrorCode);

        // Counts by connector
        Map<String, Long> byConnector = repository.countByConnector().stream()
                .collect(Collectors.toMap(
                        row -> (String) row[0],
                        row -> (Long) row[1],
                        (a, b) -> a,
                        LinkedHashMap::new));
        stats.put("byConnector", byConnector);

        // Total
        stats.put("total", repository.count());

        return ResponseEntity.ok(stats);
    }
}
