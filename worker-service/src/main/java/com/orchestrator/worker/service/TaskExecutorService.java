package com.orchestrator.worker.service;

import com.orchestrator.common.audit.AuditService;
import com.orchestrator.common.dto.TaskEvent;
import com.orchestrator.common.metrics.MetricsService;
import com.orchestrator.common.model.TaskEntity;
import com.orchestrator.common.model.TaskStatus;
import com.orchestrator.common.model.TaskType;
import com.orchestrator.common.util.EncryptionUtil;
import com.orchestrator.worker.repository.WorkerTaskRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.*;

/**
 * Core execution engine.
 *
 * Flow per task:
 * 1. Acquire Redis lock
 * 2. Conditional DB update: PENDING → RUNNING
 * 3. Start heartbeat thread (renew lock TTL + DB heartbeat)
 * 4. Execute business logic
 * 5. Update DB status → SUCCESS or FAILED
 * 6. Commit Kafka offset (handled by caller)
 * 7. Release Redis lock safely
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class TaskExecutorService {

    private final LockService lockService;
    private final WorkerTaskRepository taskRepository;
    private final KafkaTemplate<String, TaskEvent> kafkaTemplate;
    private final StringRedisTemplate redisTemplate;
    private final MetricsService metricsService;
    private final AuditService auditService;
    private final TaskStatusService taskStatusService;

    @Value("${app.worker.id}")
    private String workerId;

    @Value("${app.lock.ttl-ms}")
    private long lockTtlMs;

    @Value("${app.lock.heartbeat-interval-ms}")
    private long heartbeatIntervalMs;

    @Value("${app.kafka.topic.tasks}")
    private String tasksTopic;

    @Value("${app.kafka.topic.dlq}")
    private String dlqTopic;

    @Value("${app.retry.backoff-multiplier}")
    private int backoffMultiplier;

    @Value("${app.retry.initial-delay-ms}")
    private long initialDelayMs;

    @Value("${app.retry.max-delay-ms}")
    private long maxDelayMs;

    @Value("${app.encryption.key}")
    private String encryptionKey;

    private static final String ACTOR = "worker-service";

    private final ScheduledExecutorService heartbeatExecutor = createHeartbeatExecutor();

    private ScheduledExecutorService createHeartbeatExecutor() {
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(10, r -> {
            Thread t = new Thread(r, "heartbeat");
            t.setDaemon(true);
            return t;
        });
        executor.setRemoveOnCancelPolicy(true);
        return executor;
    }

    @jakarta.annotation.PreDestroy
    public void shutdown() {
        log.info("Shutting down heartbeat executor");
        heartbeatExecutor.shutdown();
        try {
            if (!heartbeatExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                heartbeatExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            heartbeatExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Executes a single task event end-to-end.
     *
     * @return true if the task was executed (or skipped due to lock),
     *         allowing the consumer to commit the offset.
     */
    public boolean execute(TaskEvent event) {
        String taskId = event.getTaskId().toString();

        try {
            MDC.put("taskId", taskId);
            MDC.put("taskType", event.getType().name());
            MDC.put("workerId", workerId);

            // Step 0: Execution idempotency — skip if already successfully executed
            String execKey = "exec:" + taskId;
            if (Boolean.TRUE.equals(redisTemplate.hasKey(execKey))) {
                log.info("Skipping already-executed task (idempotency guard): taskId={}", taskId);
                return true;
            }

            // Step 1: Acquire Redis lock
            if (!lockService.acquireLock(taskId, workerId)) {
                log.info("Skipping task (lock held by another worker): taskId={}", taskId);
                return true; // Another worker is handling it — safe to commit offset
            }

            ScheduledFuture<?> heartbeatFuture = null;
            try {
                // Step 2: Conditional DB update PENDING → RUNNING
                Instant lockedUntil = Instant.now().plusMillis(lockTtlMs);
                int updated = taskRepository.claimTask(
                        event.getTaskId(), workerId, lockedUntil, Instant.now());

                if (updated == 0) {
                    log.info("Task already claimed by another worker: taskId={}", taskId);
                    lockService.releaseLock(taskId, workerId);
                    return true;
                }

                MDC.put("status", "RUNNING");
                log.info("Task claimed: taskId={}, workerId={}", taskId, workerId);

                // Audit: PENDING → RUNNING
                auditService.log(event.getTaskId(), TaskStatus.PENDING, TaskStatus.RUNNING,
                        ACTOR, "Claimed by " + workerId);

                // Step 3: Start heartbeat thread
                Thread currentThread = Thread.currentThread();
                heartbeatFuture = startHeartbeat(event.getTaskId(), taskId, currentThread);

                // Step 4: Execute business logic (with timing)
                long startTime = System.currentTimeMillis();
                executeBusinessLogic(event);
                long durationMs = System.currentTimeMillis() - startTime;

                // Step 5: Mark SUCCESS + set execution idempotency key
                taskStatusService.updateTaskStatus(event.getTaskId(), TaskStatus.SUCCESS, null);
                redisTemplate.opsForValue().set(execKey, "1", Duration.ofDays(1));

                // Audit: RUNNING → SUCCESS
                auditService.log(event.getTaskId(), TaskStatus.RUNNING, TaskStatus.SUCCESS,
                        ACTOR, "Completed in " + durationMs + "ms");

                // Metrics
                MDC.put("status", "SUCCESS");
                metricsService.incrementTaskCompleted(event.getType().name(), "SUCCESS");
                metricsService.recordExecutionTime(event.getType().name(), durationMs);

                log.info("Task completed successfully: taskId={}, duration={}ms", taskId, durationMs);

                return true;

            } catch (Exception ex) {
                if (Thread.interrupted()) {
                    log.error("Task execution aborted — lock lost: taskId={}", taskId);
                } else {
                    log.error("Task execution failed: taskId={}, error={}", taskId, ex.getMessage(), ex);
                    handleFailure(event, ex);
                }
                return true;

            } finally {
                // Stop heartbeat
                if (heartbeatFuture != null) {
                    heartbeatFuture.cancel(false);
                }
                // Step 7: Release lock safely
                lockService.releaseLock(taskId, workerId);
            }
        } finally {
            MDC.clear();
        }
    }

    /**
     * Starts a background heartbeat that renews both Redis lock TTL and DB heartbeat timestamp.
     * If the lock cannot be renewed, the execution thread is interrupted.
     */
    private ScheduledFuture<?> startHeartbeat(UUID taskUuid, String taskId, Thread executionThread) {
        return heartbeatExecutor.scheduleAtFixedRate(() -> {
            try {
                boolean renewed = lockService.renewLock(taskId, workerId);
                if (renewed) {
                    Instant newLockedUntil = Instant.now().plusMillis(lockTtlMs);
                    taskRepository.updateHeartbeat(taskUuid, workerId, newLockedUntil, Instant.now());
                    log.debug("Heartbeat sent: taskId={}", taskId);
                } else {
                    log.error("CRITICAL: Heartbeat failed — lock lost! Interrupting task: taskId={}", taskId);
                    executionThread.interrupt();
                }
            } catch (Exception ex) {
                log.error("Heartbeat error: taskId={}, error={}", taskId, ex.getMessage());
            }
        }, heartbeatIntervalMs, heartbeatIntervalMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Dispatches business logic based on task type.
     * Decrypts PAYMENT payloads before processing.
     */
    private void executeBusinessLogic(TaskEvent event) {
        log.info("Executing task: taskId={}, type={}", event.getTaskId(), event.getType());

        // Decrypt payload for PAYMENT tasks
        String payload = event.getPayload();
        if (event.getType() == TaskType.PAYMENT && payload != null) {
            try {
                payload = EncryptionUtil.decrypt(payload, encryptionKey);
                log.info("Decrypted PAYMENT payload for taskId={}", event.getTaskId());
            } catch (Exception ex) {
                log.error("Payload decryption failed for taskId={} — aborting execution", event.getTaskId());
                throw new RuntimeException("Decryption stalled: check encryption key consistency", ex);
            }
        }

        // Simulate work based on type
        switch (event.getType()) {
            case EMAIL -> {
                log.info("Sending email for taskId={}", event.getTaskId());
                simulateWork(2000);
            }
            case PAYMENT -> {
                log.info("Processing payment for taskId={} (decrypted payload length={})",
                        event.getTaskId(), payload != null ? payload.length() : 0);
                simulateWork(5000);
            }
            case REPORT -> {
                log.info("Generating report for taskId={}", event.getTaskId());
                simulateWork(10000);
            }
        }

        log.info("Task execution completed: taskId={}, type={}", event.getTaskId(), event.getType());
    }

    private void simulateWork(long durationMs) {
        try {
            Thread.sleep(durationMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Task interrupted", e);
        }
    }

    /**
     * Handles task failure: increments retry count, re-publishes with backoff, or sends to DLQ.
     */
    private void handleFailure(TaskEvent event, Exception ex) {
        int newRetryCount = event.getRetryCount() + 1;
        String reason = ex.getMessage() != null ? ex.getMessage() : ex.getClass().getSimpleName();

        if (newRetryCount > event.getMaxRetries()) {
            // Exhausted retries → send to DLQ and mark DEAD
            log.warn("Task exhausted retries, sending to DLQ: taskId={}, retries={}",
                    event.getTaskId(), newRetryCount);
            taskStatusService.updateTaskStatus(event.getTaskId(), TaskStatus.DEAD, reason);
            kafkaTemplate.send(dlqTopic, event.getTaskId().toString(), event);

            // Audit: RUNNING → DEAD
            auditService.log(event.getTaskId(), TaskStatus.RUNNING, TaskStatus.DEAD,
                    ACTOR, "Retries exhausted: " + reason);

            // Metrics
            metricsService.incrementTaskCompleted(event.getType().name(), "DEAD");
            metricsService.incrementDlq(event.getType().name());
        } else {
            // Retry with exponential backoff
            long delay = calculateBackoff(newRetryCount);
            log.info("Scheduling retry: taskId={}, attempt={}/{}, backoff={}ms",
                    event.getTaskId(), newRetryCount, event.getMaxRetries(), delay);

            taskStatusService.updateTaskForRetry(event.getTaskId(), newRetryCount, reason);

            // Audit: RUNNING → PENDING (retry)
            auditService.log(event.getTaskId(), TaskStatus.RUNNING, TaskStatus.PENDING,
                    ACTOR, "Retry " + newRetryCount + "/" + event.getMaxRetries() + ": " + reason);

            // Metrics
            metricsService.incrementRetry(event.getType().name());

            // Re-publish with updated retry count after delay
            TaskEvent retryEvent = TaskEvent.builder()
                    .taskId(event.getTaskId())
                    .idempotencyKey(event.getIdempotencyKey())
                    .type(event.getType())
                    .payload(event.getPayload())
                    .retryCount(newRetryCount)
                    .maxRetries(event.getMaxRetries())
                    .scheduledAt(Instant.now().plusMillis(delay))
                    .build();

            kafkaTemplate.send(tasksTopic, event.getTaskId().toString(), retryEvent);
        }
    }

    /**
     * Exponential backoff: initial * multiplier^(attempt-1), capped at maxDelay.
     * Example with defaults: 1s → 5s → 25s → 125s → 625s (capped at 10min)
     */
    private long calculateBackoff(int attempt) {
        long delay = (long) (initialDelayMs * Math.pow(backoffMultiplier, attempt - 1));
        return Math.min(delay, maxDelayMs);
    }


}
