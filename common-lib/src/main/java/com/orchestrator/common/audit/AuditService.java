package com.orchestrator.common.audit;

import com.orchestrator.common.model.AuditLog;
import com.orchestrator.common.model.TaskStatus;
import com.orchestrator.common.repository.AuditLogRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.UUID;

/**
 * Service for recording task state transitions to the audit_log table.
 * Every status change across scheduler, worker, and reaper is captured here.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class AuditService {

    private final AuditLogRepository auditLogRepository;

    /**
     * Logs a task state transition.
     *
     * @param taskId     the task UUID
     * @param fromStatus previous status (null for initial creation)
     * @param toStatus   new status
     * @param actor      service name performing the transition (e.g. "scheduler-service")
     * @param reason     optional description of why the transition occurred
     */
    public void log(UUID taskId, TaskStatus fromStatus, TaskStatus toStatus, String actor, String reason) {
        AuditLog entry = AuditLog.builder()
                .taskId(taskId)
                .fromStatus(fromStatus)
                .toStatus(toStatus)
                .actor(actor)
                .reason(reason)
                .build();

        java.util.Objects.requireNonNull(entry, "Audit record cannot be null");
        auditLogRepository.save(entry);
        log.debug("Audit: taskId={} {} → {} by {} ({})", taskId, fromStatus, toStatus, actor, reason);
    }
}
