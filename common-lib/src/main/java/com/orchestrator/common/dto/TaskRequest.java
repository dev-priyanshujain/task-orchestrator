package com.orchestrator.common.dto;

import com.orchestrator.common.model.TaskType;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.*;

import java.time.Instant;

/**
 * Inbound request DTO for creating a new task.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TaskRequest {

    @NotBlank(message = "Idempotency key is required")
    private String idempotencyKey;

    @NotNull(message = "Task type is required")
    private TaskType type;

    private String payload;

    private Instant scheduledAt;

    @Builder.Default
    private int maxRetries = 5;
}
