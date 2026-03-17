package com.example.connect.replay.model;

import jakarta.persistence.*;
import java.time.OffsetDateTime;
import java.util.UUID;

@Entity
@Table(name = "error_records", schema = "replay")
public class ErrorRecord {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "connector_name", nullable = false)
    private String connectorName;

    @Column(name = "original_topic", nullable = false)
    private String originalTopic;

    @Column(name = "original_partition")
    private Integer originalPartition;

    @Column(name = "original_offset")
    private Long originalOffset;

    @Column(name = "error_code")
    private String errorCode;

    @Column(name = "error_message", columnDefinition = "TEXT")
    private String errorMessage;

    @Column(name = "message_key")
    private byte[] messageKey;

    @Column(name = "message_payload")
    private byte[] messagePayload;

    @Column(name = "source_position")
    private Long sourcePosition;

    @Column(name = "replay_count", nullable = false)
    private int replayCount = 0;

    @Column(name = "status", nullable = false)
    private String status = "PENDING";

    @Column(name = "created_at", nullable = false)
    private OffsetDateTime createdAt = OffsetDateTime.now();

    @Column(name = "last_replayed_at")
    private OffsetDateTime lastReplayedAt;

    @Column(name = "resolved_at")
    private OffsetDateTime resolvedAt;

    public ErrorRecord() {}

    public UUID getId() { return id; }
    public void setId(UUID id) { this.id = id; }

    public String getConnectorName() { return connectorName; }
    public void setConnectorName(String connectorName) { this.connectorName = connectorName; }

    public String getOriginalTopic() { return originalTopic; }
    public void setOriginalTopic(String originalTopic) { this.originalTopic = originalTopic; }

    public Integer getOriginalPartition() { return originalPartition; }
    public void setOriginalPartition(Integer originalPartition) { this.originalPartition = originalPartition; }

    public Long getOriginalOffset() { return originalOffset; }
    public void setOriginalOffset(Long originalOffset) { this.originalOffset = originalOffset; }

    public String getErrorCode() { return errorCode; }
    public void setErrorCode(String errorCode) { this.errorCode = errorCode; }

    public String getErrorMessage() { return errorMessage; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }

    public byte[] getMessageKey() { return messageKey; }
    public void setMessageKey(byte[] messageKey) { this.messageKey = messageKey; }

    public byte[] getMessagePayload() { return messagePayload; }
    public void setMessagePayload(byte[] messagePayload) { this.messagePayload = messagePayload; }

    public Long getSourcePosition() { return sourcePosition; }
    public void setSourcePosition(Long sourcePosition) { this.sourcePosition = sourcePosition; }

    public int getReplayCount() { return replayCount; }
    public void setReplayCount(int replayCount) { this.replayCount = replayCount; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public OffsetDateTime getCreatedAt() { return createdAt; }
    public void setCreatedAt(OffsetDateTime createdAt) { this.createdAt = createdAt; }

    public OffsetDateTime getLastReplayedAt() { return lastReplayedAt; }
    public void setLastReplayedAt(OffsetDateTime lastReplayedAt) { this.lastReplayedAt = lastReplayedAt; }

    public OffsetDateTime getResolvedAt() { return resolvedAt; }
    public void setResolvedAt(OffsetDateTime resolvedAt) { this.resolvedAt = resolvedAt; }
}
