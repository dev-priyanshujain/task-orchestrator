package com.orchestrator.common.dto;

import com.orchestrator.common.model.TaskType;
import lombok.*;

import java.time.Instant;
import java.util.UUID;

/**
 * Event published to Kafka when a task needs to be executed.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TaskEvent {
    private UUID taskId;
    private String idempotencyKey;
    private TaskType type;
    private String payload;
    private int retryCount;
    private int maxRetries;
    private Instant scheduledAt;
}
