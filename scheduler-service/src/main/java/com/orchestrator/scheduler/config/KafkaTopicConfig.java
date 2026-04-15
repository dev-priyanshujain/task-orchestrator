package com.orchestrator.scheduler.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declarative Kafka topic creation.
 *
 * Topics are created on startup if they don't exist.
 * Partition count is tuned for horizontal scaling:
 * - task-events: 12 partitions (allows up to 12 concurrent consumers)
 * - task-events-dlq: 3 partitions (lower throughput expected)
 */
@Configuration
public class KafkaTopicConfig {

    @Value("${app.kafka.topic.tasks}")
    private String tasksTopic;

    @Value("${app.kafka.topic.dlq:task-events-dlq}")
    private String dlqTopic;

    @Value("${app.kafka.topic.partitions:12}")
    private int partitions;

    @Value("${app.kafka.topic.replication-factor:1}")
    private int replicationFactor;

    @Bean
    public NewTopic taskEventsTopic() {
        java.util.Objects.requireNonNull(tasksTopic, "tasksTopic cannot be null");
        return TopicBuilder.name(tasksTopic)
                .partitions(partitions)
                .replicas(replicationFactor)
                .config("retention.ms", "604800000")  // 7 days
                .config("min.insync.replicas", "1")
                .build();
    }

    @Bean
    public NewTopic dlqTopic() {
        java.util.Objects.requireNonNull(dlqTopic, "dlqTopic cannot be null");
        return TopicBuilder.name(dlqTopic)
                .partitions(3)
                .replicas(replicationFactor)
                .config("retention.ms", "2592000000")  // 30 days
                .build();
    }
}
