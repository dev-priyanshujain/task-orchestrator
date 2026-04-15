package com.orchestrator.reaper.service;

import com.orchestrator.common.audit.AuditService;
import com.orchestrator.common.dto.TaskEvent;
import com.orchestrator.common.metrics.MetricsService;
import com.orchestrator.common.model.TaskEntity;
import com.orchestrator.common.model.TaskStatus;
import com.orchestrator.reaper.repository.ReaperTaskRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Scheduled service that scans for stuck tasks and recovers them.
 *
 * Features:
 * - Redis-based leader election (only one reaper instance runs at a time)
 * - Recovers RUNNING tasks with expired heartbeats
 * - Re-queues eligible tasks to Kafka or marks them DEAD if retries exhausted
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class ReaperService {

    private final ReaperTaskRepository taskRepository;
    private final KafkaTemplate<String, TaskEvent> kafkaTemplate;
    private final StringRedisTemplate redisTemplate;
    private final MetricsService metricsService;
    private final AuditService auditService;

    @Value("${app.reaper.heartbeat-threshold-ms}")
    private long heartbeatThresholdMs;

    @Value("${app.reaper.leader-ttl-ms}")
    private long leaderTtlMs;

    @Value("${app.reaper.instance-id}")
    private String instanceId;

    @Value("${app.kafka.topic.tasks}")
    private String tasksTopic;

    @Value("${app.kafka.topic.dlq}")
    private String dlqTopic;

    private static final String LEADER_KEY = "reaper:leader";
    private static final String ACTOR = "reaper-service";

    /**
     * Runs on a fixed delay. First acquires leader lock, then scans for stuck tasks.
     */
    @Scheduled(fixedDelayString = "${app.reaper.scan-interval-ms}")
    public void reap() {
        // Leader election — only one reaper instance processes at a time
        java.util.Objects.requireNonNull(instanceId, "instanceId cannot be null");
        Duration ttl = Duration.ofMillis(leaderTtlMs);
        java.util.Objects.requireNonNull(ttl, "ttl cannot be null");

        Boolean acquired = redisTemplate.opsForValue()
                .setIfAbsent(LEADER_KEY, instanceId, ttl);
        
        if (acquired == null) acquired = false;

        if (!Boolean.TRUE.equals(acquired)) {
            // Check if we are already the leader (re-entrant is not native, checking value)
            String currentLeader = redisTemplate.opsForValue().get(LEADER_KEY);
            if (!instanceId.equals(currentLeader)) {
                log.debug("Reaper skipping — another instance is leader: {}", currentLeader);
                return;
            }
        }

        // We are leader — start watchdog to extend lock while processing
        ScheduledExecutorService watchdog = Executors.newSingleThreadScheduledExecutor();
        watchdog.scheduleAtFixedRate(() -> {
            try {
                Duration ttl = Duration.ofMillis(leaderTtlMs);
                java.util.Objects.requireNonNull(ttl, "ttl cannot be null");
                redisTemplate.expire(LEADER_KEY, ttl);
                log.debug("Reaper leader lock extended");
            } catch (Exception e) {
                log.warn("Failed to extend reaper leader lock");
            }
        }, leaderTtlMs / 2, leaderTtlMs / 2, java.util.concurrent.TimeUnit.MILLISECONDS);

        log.info("Reaper scan started (leader={})", instanceId);

        try {
            recoverStuckRunningTasks();
            recoverStalePendingTasks();
        } catch (Exception ex) {
            log.error("Reaper scan failed: {}", ex.getMessage(), ex);
        } finally {
            watchdog.shutdown();
            // Release lock to allow other instances to take over gracefully
            redisTemplate.delete(LEADER_KEY);
            log.info("Reaper scan completed, leader lock released");
        }
    }

    /**
     * Finds RUNNING tasks with expired heartbeats and either re-queues or kills them.
     */
    @Transactional
    void recoverStuckRunningTasks() {
        Instant threshold = Instant.now().minusMillis(heartbeatThresholdMs);

        List<TaskEntity> stuckTasks = taskRepository.findStuckRunningTasks(TaskStatus.RUNNING, threshold);

        if (stuckTasks.isEmpty()) {
            log.debug("Reaper found no stuck tasks");
            return;
        }

        log.info("Reaper found {} stuck task(s)", stuckTasks.size());

        for (TaskEntity task : stuckTasks) {
            try {
                MDC.put("taskId", task.getTaskId().toString());
                MDC.put("taskType", task.getType().name());

                if (task.getRetryCount() < task.getMaxRetries()) {
                    dispatchTask(task, true, "worker heartbeat expired");
                } else {
                    killTask(task);
                }
            } catch (IllegalArgumentException | IllegalStateException ex) {
                log.error("Reaper failed to recover task: taskId={}, error={}",
                        task.getTaskId(), ex.getMessage(), ex);
            } finally {
                MDC.clear();
            }
        }
    }

    /**
     * Resets a stuck task and re-publishes it to Kafka.
     * If incrementRetry is true, retryCount is increased and reason is set.
     */
    private void dispatchTask(TaskEntity task, boolean incrementRetry, String reason) {
        if (incrementRetry) {
            task.setRetryCount(task.getRetryCount() + 1);
        }
        
        task.setStatus(TaskStatus.PENDING);
        task.setLockedBy(null);
        task.setLockedUntil(null);
        task.setLastHeartbeatAt(null);
        if (reason != null) {
            task.setFailureReason(reason);
        }
        taskRepository.save(task);

        TaskEvent event = TaskEvent.builder()
                .taskId(task.getTaskId())
                .idempotencyKey(task.getIdempotencyKey())
                .type(task.getType())
                .payload(task.getPayload())
                .retryCount(task.getRetryCount())
                .maxRetries(task.getMaxRetries())
                .scheduledAt(Instant.now())
                .build();

        final TaskEvent finalEvent = event;
        final TaskEntity finalTask = task;
                org.springframework.transaction.support.TransactionSynchronizationManager.registerSynchronization(
                        new org.springframework.transaction.support.TransactionSynchronization() {
                            @Override
                            public void afterCommit() {
                                java.util.Objects.requireNonNull(tasksTopic, "tasksTopic cannot be null");
                                java.util.UUID tid = finalTask.getTaskId();
                                java.util.Objects.requireNonNull(tid, "taskId cannot be null");
                                kafkaTemplate.send(tasksTopic, tid.toString(), finalEvent);
                                log.info("Reaper dispatched task: taskId={}, retry={}", tid, incrementRetry);
                            }
                        }
                );

        if (incrementRetry) {
            // Audit: RUNNING → PENDING
            auditService.log(task.getTaskId(), TaskStatus.RUNNING, TaskStatus.PENDING,
                    ACTOR, "Recovered — " + reason + " (retry " + task.getRetryCount() + "/" + task.getMaxRetries() + ")");
            metricsService.incrementReaperRecovered();
        } else {
            log.info("Reaper scheduled due PENDING task: taskId={}", task.getTaskId());
        }
    }

    /**
     * Marks a task as DEAD and publishes to the DLQ.
     */
    private void killTask(TaskEntity task) {
        task.setStatus(TaskStatus.DEAD);
        task.setLockedBy(null);
        task.setLockedUntil(null);
        task.setFailureReason("Marked DEAD by reaper — retries exhausted after heartbeat expiry");
        taskRepository.save(task);

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
                                java.util.Objects.requireNonNull(dlqTopic, "dlqTopic cannot be null");
                                java.util.UUID tid = finalTask.getTaskId();
                                java.util.Objects.requireNonNull(tid, "taskId cannot be null");
                                kafkaTemplate.send(dlqTopic, tid.toString(), finalEvent);
                                log.info("Reaper sent task to DLQ: taskId={}", tid);
                            }
                        }
                );

        // Audit: RUNNING → DEAD
        auditService.log(task.getTaskId(), TaskStatus.RUNNING, TaskStatus.DEAD,
                ACTOR, "Retries exhausted after heartbeat expiry");

        // Metrics
        metricsService.incrementReaperKilled();

        log.warn("Reaper killed task (retries exhausted): taskId={}, retries={}",
                task.getTaskId(), task.getRetryCount());
    }
    @Transactional
    void recoverStalePendingTasks() {
        // Find tasks whose scheduled time has passed but are still PENDING
        Instant threshold = Instant.now();
        List<TaskEntity> staleTasks = taskRepository.findStalePendingTasks(TaskStatus.PENDING, threshold);

        if (staleTasks.isEmpty()) return;

        log.info("Reaper found {} due PENDING task(s)", staleTasks.size());
        for (TaskEntity task : staleTasks) {
            dispatchTask(task, false, null);
        }
    }
}
