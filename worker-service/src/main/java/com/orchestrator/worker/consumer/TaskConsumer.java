package com.orchestrator.worker.consumer;

import com.orchestrator.common.dto.TaskEvent;
import com.orchestrator.worker.service.TaskExecutorService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;

/**
 * Kafka consumer that pulls task events and delegates to the executor.
 *
 * Manual acknowledgment ensures offsets are only committed AFTER
 * successful task execution (at-least-once delivery guarantee).
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class TaskConsumer {

    private final TaskExecutorService taskExecutorService;

    @KafkaListener(
            topics = "${app.kafka.topic.tasks}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(TaskEvent event, Acknowledgment ack) {
        log.info("Received task event: taskId={}, type={}, retryCount={}",
                event.getTaskId(), event.getType(), event.getRetryCount());

        // Respect scheduledAt — avoid partition blocking. 
        // If the task is not yet due, acknowledge and skip it.
        // The ReaperService will re-publish it when it is actually due.
        if (event.getScheduledAt() != null && event.getScheduledAt().isAfter(Instant.now())) {
            log.info("Task not yet due (scheduledAt={}), skipping and acknowledging to avoid partition blocking: taskId={}",
                    event.getScheduledAt(), event.getTaskId());
            ack.acknowledge();
            return;
        }

        try {
            boolean processed = taskExecutorService.execute(event);

            if (processed) {
                // Commit offset only after successful processing
                ack.acknowledge();
                log.debug("Kafka offset committed for taskId={}", event.getTaskId());
            }
        } catch (Exception ex) {
            log.error("Unrecoverable error processing task: taskId={}, error={}",
                    event.getTaskId(), ex.getMessage(), ex);
            // Acknowledge to avoid infinite redelivery of poison messages
            // The task has already been handled by the failure path in executor
            ack.acknowledge();
        }
    }
}
