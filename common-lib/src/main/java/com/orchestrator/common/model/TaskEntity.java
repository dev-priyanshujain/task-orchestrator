package com.orchestrator.common.model;

import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;
import java.util.UUID;

/**
 * Core task entity persisted to PostgreSQL.
 * Tracks full lifecycle including locking, heartbeat, and retry state.
 */
@Entity
@Table(name = "tasks", indexes = {
        @Index(name = "idx_tasks_status", columnList = "status"),
        @Index(name = "idx_tasks_idempotency_key", columnList = "idempotencyKey", unique = true),
        @Index(name = "idx_tasks_scheduled_at", columnList = "scheduledAt"),
        @Index(name = "idx_tasks_last_heartbeat", columnList = "lastHeartbeatAt")
})
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TaskEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(updatable = false, nullable = false)
    private UUID taskId;

    @Column(nullable = false, unique = true)
    private String idempotencyKey;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private TaskType type;

    @Column(columnDefinition = "TEXT")
    private String payload;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private TaskStatus status;

    @Column(nullable = false)
    @Builder.Default
    private int retryCount = 0;

    @Column(nullable = false)
    @Builder.Default
    private int maxRetries = 5;

    private Instant scheduledAt;

    @Column(nullable = false, updatable = false)
    private Instant createdAt;

    private String lockedBy;

    private Instant lockedUntil;

    private Instant lastHeartbeatAt;

    @Column(columnDefinition = "TEXT")
    private String failureReason;

    @Version
    private Long version;

    @PrePersist
    protected void onCreate() {
        if (createdAt == null) {
            createdAt = Instant.now();
        }
        if (status == null) {
            status = TaskStatus.PENDING;
        }
    }
}
