package com.orchestrator.worker.repository;

import com.orchestrator.common.model.TaskEntity;
import com.orchestrator.common.model.TaskStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Repository
public interface WorkerTaskRepository extends JpaRepository<TaskEntity, UUID> {

    /**
     * Conditional update: PENDING → RUNNING.
     * Only succeeds if the task is still PENDING (prevents race conditions).
     * Returns the number of updated rows (0 = another worker beat us).
     */
    @Modifying(clearAutomatically = true)
    @Query("UPDATE TaskEntity t SET t.status = 'RUNNING', t.lockedBy = :workerId, " +
           "t.lockedUntil = :lockedUntil, t.lastHeartbeatAt = :now " +
           "WHERE t.taskId = :taskId AND t.status = 'PENDING'")
    int claimTask(@Param("taskId") UUID taskId,
                  @Param("workerId") String workerId,
                  @Param("lockedUntil") Instant lockedUntil,
                  @Param("now") Instant now);

    /**
     * Update heartbeat timestamp and extend lock.
     */
    @Modifying(clearAutomatically = true)
    @Query("UPDATE TaskEntity t SET t.lastHeartbeatAt = :now, t.lockedUntil = :lockedUntil " +
           "WHERE t.taskId = :taskId AND t.lockedBy = :workerId")
    int updateHeartbeat(@Param("taskId") UUID taskId,
                        @Param("workerId") String workerId,
                        @Param("lockedUntil") Instant lockedUntil,
                        @Param("now") Instant now);

    /**
     * Finds tasks stuck in RUNNING with expired heartbeats (for Reaper).
     */
    List<TaskEntity> findByStatusAndLastHeartbeatAtBefore(TaskStatus status, Instant threshold);
}
