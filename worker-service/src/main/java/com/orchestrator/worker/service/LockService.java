package com.orchestrator.worker.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Collections;

/**
 * Redis-based distributed lock service.
 *
 * Acquire: SET task:{taskId} {workerId} NX PX {ttlMs}
 * Release: Lua script compare-and-delete (only the lock owner can release)
 * Renew:   Lua script compare-and-extend TTL
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class LockService {

    private final StringRedisTemplate redisTemplate;

    @Value("${app.lock.ttl-ms}")
    private long lockTtlMs;

    private static final String LOCK_PREFIX = "lock:task:";

    /**
     * Compare-and-delete Lua script.
     * Only deletes the key if the stored value matches the expected owner.
     * Returns 1 on success, 0 if the lock is owned by someone else.
     */
    private static final String UNLOCK_SCRIPT =
            "if redis.call('get', KEYS[1]) == ARGV[1] then " +
            "  return redis.call('del', KEYS[1]) " +
            "else " +
            "  return 0 " +
            "end";

    /**
     * Compare-and-extend Lua script.
     * Only extends TTL if the stored value matches the expected owner.
     * Returns 1 on success, 0 if the lock is owned by someone else.
     */
    private static final String RENEW_SCRIPT =
            "if redis.call('get', KEYS[1]) == ARGV[1] then " +
            "  return redis.call('pexpire', KEYS[1], ARGV[2]) " +
            "else " +
            "  return 0 " +
            "end";

    /**
     * Attempts to acquire a distributed lock for the given task.
     *
     * @return true if lock was acquired, false if already held by another worker
     */
    public boolean acquireLock(String taskId, String workerId) {
        String key = LOCK_PREFIX + taskId;
        Boolean result = redisTemplate.opsForValue()
                .setIfAbsent(key, workerId, Duration.ofMillis(lockTtlMs));

        boolean acquired = Boolean.TRUE.equals(result);
        if (acquired) {
            log.debug("Lock acquired: taskId={}, workerId={}, ttl={}ms", taskId, workerId, lockTtlMs);
        } else {
            log.debug("Lock already held: taskId={}", taskId);
        }
        return acquired;
    }

    /**
     * Safely releases a lock — only if this worker is the current owner.
     * Uses a Lua script to atomically compare-and-delete.
     *
     * @return true if the lock was released, false if owned by someone else
     */
    public boolean releaseLock(String taskId, String workerId) {
        String key = LOCK_PREFIX + taskId;
        DefaultRedisScript<Long> script = new DefaultRedisScript<>(UNLOCK_SCRIPT, Long.class);

        Long result = redisTemplate.execute(script, Collections.singletonList(key), workerId);
        boolean released = result != null && result == 1L;

        if (released) {
            log.debug("Lock released: taskId={}, workerId={}", taskId, workerId);
        } else {
            log.warn("Failed to release lock (not owner): taskId={}, workerId={}", taskId, workerId);
        }
        return released;
    }

    /**
     * Renews the lock TTL — only if this worker is the current owner.
     * Used by the heartbeat thread to keep the lock alive during long-running tasks.
     *
     * @return true if TTL was extended, false if lock expired or owned by someone else
     */
    public boolean renewLock(String taskId, String workerId) {
        String key = LOCK_PREFIX + taskId;
        DefaultRedisScript<Long> script = new DefaultRedisScript<>(RENEW_SCRIPT, Long.class);

        Long result = redisTemplate.execute(script, Collections.singletonList(key),
                workerId, String.valueOf(lockTtlMs));
        boolean renewed = result != null && result == 1L;

        if (renewed) {
            log.debug("Lock renewed: taskId={}, workerId={}, ttl={}ms", taskId, workerId, lockTtlMs);
        } else {
            log.warn("Failed to renew lock (not owner or expired): taskId={}, workerId={}", taskId, workerId);
        }
        return renewed;
    }
}
