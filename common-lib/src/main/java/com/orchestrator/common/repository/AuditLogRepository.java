package com.orchestrator.common.repository;

import com.orchestrator.common.model.AuditLog;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

/**
 * Repository for audit log persistence and queries.
 */
@Repository
public interface AuditLogRepository extends JpaRepository<AuditLog, UUID> {

    List<AuditLog> findByTaskIdOrderByTimestampAsc(UUID taskId);
}
