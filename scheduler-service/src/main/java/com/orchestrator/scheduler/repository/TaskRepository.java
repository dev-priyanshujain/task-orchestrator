package com.orchestrator.scheduler.repository;

import com.orchestrator.common.model.TaskEntity;
import com.orchestrator.common.model.TaskStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface TaskRepository extends JpaRepository<TaskEntity, UUID> {

    Optional<TaskEntity> findByIdempotencyKey(String idempotencyKey);

    List<TaskEntity> findByStatus(TaskStatus status);
}
