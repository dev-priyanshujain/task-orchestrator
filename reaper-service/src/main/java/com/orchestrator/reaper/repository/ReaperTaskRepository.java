package com.orchestrator.reaper.repository;

import com.orchestrator.common.model.TaskEntity;
import com.orchestrator.common.model.TaskStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Repository
public interface ReaperTaskRepository extends JpaRepository<TaskEntity, UUID> {

    /**
     * Finds tasks stuck in RUNNING where heartbeat has expired.
     * These are likely from crashed workers whose Redis lock TTL has also expired.
     */
    @Query("SELECT t FROM TaskEntity t WHERE t.status = :status AND t.lastHeartbeatAt < :threshold")
    List<TaskEntity> findStuckRunningTasks(@Param("status") TaskStatus status,
                                           @Param("threshold") Instant threshold);

    /**
     * Finds PENDING tasks that have been stuck (scheduledAt is old and no worker picked them up).
     * This catches tasks that were re-queued but never consumed.
     */
    @Query("SELECT t FROM TaskEntity t WHERE t.status = :status AND t.scheduledAt < :threshold")
    List<TaskEntity> findStalePendingTasks(@Param("status") TaskStatus status,
                                           @Param("threshold") Instant threshold);
}
