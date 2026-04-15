package com.orchestrator.scheduler.service;

import com.orchestrator.common.audit.AuditService;
import com.orchestrator.common.dto.TaskEvent;
import com.orchestrator.common.dto.TaskRequest;
import com.orchestrator.common.dto.TaskResponse;
import com.orchestrator.common.metrics.MetricsService;
import com.orchestrator.common.model.TaskEntity;
import com.orchestrator.common.model.TaskStatus;
import com.orchestrator.common.model.TaskType;
import com.orchestrator.common.util.EncryptionUtil;
import com.orchestrator.scheduler.repository.TaskRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class TaskService {

    private final TaskRepository taskRepository;
    private final KafkaTemplate<String, TaskEvent> kafkaTemplate;
    private final StringRedisTemplate redisTemplate;
    private final MetricsService metricsService;
    private final AuditService auditService;

    @Value("${app.kafka.topic.tasks}")
    private String tasksTopic;

    @Value("${app.idempotency.ttl-seconds}")
    private long idempotencyTtlSeconds;

    @Value("${app.encryption.key}")
    private String encryptionKey;

    private static final String ACTOR = "scheduler-service";

    /**
     * Creates a new task with idempotency check.
     * Flow: Check Redis dedup → Persist to DB → Publish to Kafka → Cache idempotency key.
     */
    @Transactional
    public TaskResponse createTask(TaskRequest request) {
        String idempotencyKey = request.getIdempotencyKey();
        String redisKey = "idemp:" + idempotencyKey;

        // Step 1: Check Redis for idempotency — return existing task if duplicate
        String existingTaskId = redisTemplate.opsForValue().get(redisKey);
        if (existingTaskId != null) {
            log.info("Duplicate request detected for idempotencyKey={}, returning existing taskId={}",
                    idempotencyKey, existingTaskId);
            TaskEntity existing = taskRepository.findById(UUID.fromString(existingTaskId))
                    .orElseThrow(() -> new RuntimeException("Task not found for cached idempotencyKey"));
            return toResponse(existing);
        }

        // Step 2: Also check DB (in case Redis entry expired but task exists)
        var dbExisting = taskRepository.findByIdempotencyKey(idempotencyKey);
        if (dbExisting.isPresent()) {
            log.info("Task already exists in DB for idempotencyKey={}", idempotencyKey);
            return toResponse(dbExisting.get());
        }

        // Step 3: Encrypt payload for PAYMENT tasks
        String payload = request.getPayload();
        if (request.getType() == TaskType.PAYMENT && payload != null) {
            payload = EncryptionUtil.encrypt(payload, encryptionKey);
            log.info("Encrypted PAYMENT payload for idempotencyKey={}", idempotencyKey);
        }

        // Step 4: Persist task with PENDING status
        TaskEntity task = TaskEntity.builder()
                .idempotencyKey(idempotencyKey)
                .type(request.getType())
                .payload(payload)
                .status(TaskStatus.PENDING)
                .maxRetries(request.getMaxRetries())
                .scheduledAt(request.getScheduledAt() != null ? request.getScheduledAt() : Instant.now())
                .build();

        try {
            task = taskRepository.save(task);
        } catch (DataIntegrityViolationException ex) {
            // Race condition: another request with the same idempotencyKey inserted between
            // our Redis check and this save. Fall back to DB lookup.
            log.info("Concurrent duplicate detected for idempotencyKey={}, falling back to DB lookup",
                    idempotencyKey);
            return taskRepository.findByIdempotencyKey(idempotencyKey)
                    .map(this::toResponse)
                    .orElseThrow(() -> new RuntimeException(
                            "Task disappeared after constraint violation for idempotencyKey: " + idempotencyKey));
        }

        try {
            MDC.put("taskId", task.getTaskId().toString());
            MDC.put("taskType", task.getType().name());
            MDC.put("status", "PENDING");

            log.info("Task created: taskId={}, type={}, status=PENDING", task.getTaskId(), task.getType());

            // Audit: null → PENDING
            auditService.log(task.getTaskId(), null, TaskStatus.PENDING, ACTOR, "Task created");

            // Metrics
            metricsService.incrementTaskCreated(task.getType().name());

            // Step 5: Publish event to Kafka only if due soon (within 1 minute)
            // Otherwise, let the ReaperService pick it up when it's due
            Instant soonThreshold = Instant.now().plus(Duration.ofMinutes(1));
            if (task.getScheduledAt().isBefore(soonThreshold)) {
                TaskEvent event = TaskEvent.builder()
                        .taskId(task.getTaskId())
                        .idempotencyKey(task.getIdempotencyKey())
                        .type(task.getType())
                        .payload(task.getPayload())
                        .retryCount(task.getRetryCount())
                        .maxRetries(task.getMaxRetries())
                        .scheduledAt(task.getScheduledAt())
                        .build();

                final TaskEvent finalEvent = event;
                final TaskEntity finalTask = task;
                org.springframework.transaction.support.TransactionSynchronizationManager.registerSynchronization(
                        new org.springframework.transaction.support.TransactionSynchronization() {
                            @Override
                            public void afterCommit() {
                                kafkaTemplate.send(tasksTopic, finalTask.getTaskId().toString(), finalEvent);
                                log.info("Task event published to Kafka after transaction commit (imminent task): taskId={}", finalTask.getTaskId());
                            }
                        }
                );
            } else {
                log.info("Task scheduled for future (>{}), skipping initial Kafka publish: taskId={}", 
                        task.getScheduledAt(), task.getTaskId());
            }

            // Step 6: Cache idempotency key in Redis
            redisTemplate.opsForValue().set(redisKey, task.getTaskId().toString(),
                    Duration.ofSeconds(idempotencyTtlSeconds));

            return toResponse(task);
        } finally {
            MDC.clear();
        }
    }

    public TaskResponse getTask(UUID taskId) {
        TaskEntity task = taskRepository.findById(taskId)
                .orElseThrow(() -> new RuntimeException("Task not found: " + taskId));
        return toResponse(task);
    }

    @Transactional
    public void cancelTask(UUID taskId) {
        TaskEntity task = taskRepository.findById(taskId)
                .orElseThrow(() -> new RuntimeException("Task not found: " + taskId));

        if (task.getStatus() != TaskStatus.PENDING) {
            throw new IllegalStateException("Only PENDING tasks can be cancelled. Current status: " + task.getStatus());
        }

        TaskStatus fromStatus = task.getStatus();
        task.setStatus(TaskStatus.DEAD);
        taskRepository.save(task);

        auditService.log(taskId, fromStatus, TaskStatus.DEAD, ACTOR, "Task cancelled by user");
        log.info("Task cancelled: taskId={}", taskId);
    }

    public List<TaskResponse> getTasksByStatus(TaskStatus status) {
        return taskRepository.findByStatus(status).stream()
                .map(this::toResponse)
                .toList();
    }

    public List<TaskResponse> getAllTasks() {
        return taskRepository.findAll().stream()
                .map(this::toResponse)
                .toList();
    }

    private TaskResponse toResponse(TaskEntity entity) {
        return TaskResponse.builder()
                .taskId(entity.getTaskId())
                .idempotencyKey(entity.getIdempotencyKey())
                .type(entity.getType())
                .payload(entity.getPayload())
                .status(entity.getStatus())
                .retryCount(entity.getRetryCount())
                .maxRetries(entity.getMaxRetries())
                .scheduledAt(entity.getScheduledAt())
                .createdAt(entity.getCreatedAt())
                .lockedBy(entity.getLockedBy())
                .lockedUntil(entity.getLockedUntil())
                .lastHeartbeatAt(entity.getLastHeartbeatAt())
                .failureReason(entity.getFailureReason())
                .build();
    }
}
