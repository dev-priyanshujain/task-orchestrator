package com.orchestrator.worker.config;

import com.orchestrator.common.dto.TaskEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka consumer configuration tuned for throughput and fast rebalancing.
 *
 * Tuning rationale:
 * - concurrency = partition count: each thread handles one partition
 * - fetch.min.bytes=1KB: don't wait for large batches on low traffic
 * - fetch.max.wait.ms=500: upper bound on fetch delay
 * - session.timeout.ms=10s: fast detection of dead consumers
 * - heartbeat.interval.ms=3s: must be < session.timeout / 3
 * - max.poll.records=20: balance between throughput and rebalance latency
 * - max.poll.interval.ms=300s: allow long-running REPORT tasks
 */
@Configuration
@EnableKafka
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Value("${app.kafka.concurrency:6}")
    private int concurrency;

    @Bean
    public ConsumerFactory<String, TaskEvent> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // Throughput tuning
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 20);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1024);          // 1 KB
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);         // 500ms

        // Fast rebalancing
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000);      // 10s
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3000);    // 3s

        JsonDeserializer<TaskEvent> deserializer = new JsonDeserializer<>(TaskEvent.class);
        deserializer.addTrustedPackages("com.orchestrator.common.dto");
        deserializer.setUseTypeMapperForKey(false);

        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), deserializer);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, TaskEvent> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, TaskEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        // Manual acknowledgment mode — offsets committed only after ack.acknowledge()
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        // Concurrency matches partition count for maximum parallelism
        factory.setConcurrency(concurrency);
        return factory;
    }
}
