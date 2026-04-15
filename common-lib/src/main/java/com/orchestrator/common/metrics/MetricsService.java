package com.orchestrator.common.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Service;

import java.time.Duration;

/**
 * Shared metrics helper providing application-level counters and timers.
 * 
 * All services (scheduler, worker, reaper) inject this bean to record
 * task throughput, execution latency, retry counts, and reaper activity.
 */
@Service
public class MetricsService {

    private final MeterRegistry registry;

    public MetricsService(MeterRegistry registry) {
        this.registry = registry;
    }

    // ── Scheduler metrics ──────────────────────────────────────

    public void incrementTaskCreated(String taskType) {
        Counter.builder("tasks.created")
                .description("Number of tasks created")
                .tag("type", taskType)
                .register(registry)
                .increment();
    }

    // ── Worker metrics ─────────────────────────────────────────

    public void incrementTaskCompleted(String taskType, String status) {
        Counter.builder("tasks.completed")
                .description("Number of tasks completed")
                .tag("type", taskType)
                .tag("status", status)
                .register(registry)
                .increment();
    }

    public void recordExecutionTime(String taskType, long durationMs) {
        Timer.builder("tasks.execution.duration")
                .description("Task execution duration")
                .tag("type", taskType)
                .register(registry)
                .record(Duration.ofMillis(durationMs));
    }

    public void incrementRetry(String taskType) {
        Counter.builder("tasks.retries")
                .description("Number of task retries")
                .tag("type", taskType)
                .register(registry)
                .increment();
    }

    public void incrementDlq(String taskType) {
        Counter.builder("tasks.dlq")
                .description("Tasks sent to dead-letter queue")
                .tag("type", taskType)
                .register(registry)
                .increment();
    }

    // ── Reaper metrics ─────────────────────────────────────────

    public void incrementReaperRecovered() {
        Counter.builder("reaper.tasks.recovered")
                .description("Tasks recovered by reaper")
                .register(registry)
                .increment();
    }

    public void incrementReaperKilled() {
        Counter.builder("reaper.tasks.killed")
                .description("Tasks killed by reaper (retries exhausted)")
                .register(registry)
                .increment();
    }
}
