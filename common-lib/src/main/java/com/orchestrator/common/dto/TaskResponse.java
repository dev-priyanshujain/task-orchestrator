package com.orchestrator.common.dto;

import com.orchestrator.common.model.TaskStatus;
import com.orchestrator.common.model.TaskType;
import lombok.*;

import java.time.Instant;
import java.util.UUID;

/**
 * Outbound response DTO for task queries.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TaskResponse {
    private UUID taskId;
    private String idempotencyKey;
    private TaskType type;
    private String payload;
    private TaskStatus status;
    private int retryCount;
    private int maxRetries;
    private Instant scheduledAt;
    private Instant createdAt;
    private String lockedBy;
    private Instant lockedUntil;
    private Instant lastHeartbeatAt;
    private String failureReason;
}
