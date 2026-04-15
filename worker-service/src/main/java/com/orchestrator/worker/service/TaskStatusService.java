package com.orchestrator.worker.service;

import com.orchestrator.common.model.TaskStatus;
import com.orchestrator.worker.repository.WorkerTaskRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
@RequiredArgsConstructor
public class TaskStatusService {

    private final WorkerTaskRepository taskRepository;

    @Transactional
    public void updateTaskStatus(UUID taskId, TaskStatus status, String failureReason) {
        java.util.Objects.requireNonNull(taskId, "taskId cannot be null");
        java.util.Objects.requireNonNull(status, "status cannot be null");
        taskRepository.findById(taskId).ifPresent(task -> {
            task.setStatus(status);
            task.setFailureReason(failureReason);
            if (status == TaskStatus.SUCCESS || status == TaskStatus.DEAD) {
                task.setLockedBy(null);
                task.setLockedUntil(null);
            }
            taskRepository.save(task);
        });
    }

    @Transactional
    public void updateTaskForRetry(UUID taskId, int retryCount, String failureReason) {
        java.util.Objects.requireNonNull(taskId, "taskId cannot be null");
        taskRepository.findById(taskId).ifPresent(task -> {
            task.setRetryCount(retryCount);
            task.setStatus(TaskStatus.PENDING);
            task.setFailureReason(failureReason);
            task.setLockedBy(null);
            task.setLockedUntil(null);
            task.setLastHeartbeatAt(null);
            taskRepository.save(task);
        });
    }
}
