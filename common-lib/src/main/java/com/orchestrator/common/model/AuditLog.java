package com.orchestrator.common.model;

import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;
import java.util.UUID;

/**
 * Audit trail entity recording every task state transition.
 * Persisted to the audit_log table for compliance and debugging.
 */
@Entity
@Table(name = "audit_log", indexes = {
        @Index(name = "idx_audit_task_id", columnList = "taskId"),
        @Index(name = "idx_audit_timestamp", columnList = "timestamp")
})
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class AuditLog {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(updatable = false, nullable = false)
    private UUID id;

    @Column(nullable = false)
    private UUID taskId;

    @Enumerated(EnumType.STRING)
    @Column(length = 20)
    private TaskStatus fromStatus;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private TaskStatus toStatus;

    @Column(nullable = false, length = 50)
    private String actor;

    @Column(columnDefinition = "TEXT")
    private String reason;

    @Column(nullable = false)
    private Instant timestamp;

    @PrePersist
    protected void onCreate() {
        if (timestamp == null) {
            timestamp = Instant.now();
        }
    }
}
