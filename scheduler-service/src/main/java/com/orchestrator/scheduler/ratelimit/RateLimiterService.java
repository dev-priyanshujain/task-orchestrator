package com.orchestrator.scheduler.ratelimit;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;

/**
 * Redis-based sliding window rate limiter using a Lua script for atomicity.
 *
 * Algorithm: Fixed-window counter per client key.
 * - INCR the counter on each request
 * - SET TTL on first request in the window
 * - If count > maxRequests, reject
 *
 * This is simpler than token bucket but effective for API rate limiting.
 */
@Service
@Slf4j
public class RateLimiterService {

    private final StringRedisTemplate redisTemplate;
    private final int maxRequests;
    private final int windowSeconds;
    @SuppressWarnings("rawtypes")
    private final DefaultRedisScript<List> rateLimitScript;

    public RateLimiterService(
            StringRedisTemplate redisTemplate,
            @Value("${app.rate-limit.max-requests:100}") int maxRequests,
            @Value("${app.rate-limit.window-seconds:60}") int windowSeconds) {
        this.redisTemplate = redisTemplate;
        this.maxRequests = maxRequests;
        this.windowSeconds = windowSeconds;

        // Lua script: atomic INCR + conditional EXPIRE
        // Returns: [current_count, ttl_remaining]
        String script =
                "local key = KEYS[1] " +
                "local limit = tonumber(ARGV[1]) " +
                "local window = tonumber(ARGV[2]) " +
                "local current = redis.call('INCR', key) " +
                "if current == 1 then " +
                "  redis.call('EXPIRE', key, window) " +
                "end " +
                "local ttl = redis.call('TTL', key) " +
                "return {current, ttl}";

        this.rateLimitScript = new DefaultRedisScript<>();
        this.rateLimitScript.setScriptText(script);
        this.rateLimitScript.setResultType(List.class);
    }

    /**
     * Check if a request from the given client key is allowed.
     *
     * @param clientKey unique identifier (e.g., IP address, API key)
     * @return result containing allowed flag, remaining quota, and retry-after
     */
    public RateLimitResult isAllowed(String clientKey) {
        String redisKey = "ratelimit:" + clientKey;

        try {
            java.util.Objects.requireNonNull(rateLimitScript, "rateLimitScript cannot be null");
            List<String> keys = Collections.singletonList(redisKey);
            java.util.Objects.requireNonNull(keys, "keys cannot be null");
            String limitStr = String.valueOf(maxRequests);
            String windowStr = String.valueOf(windowSeconds);
            java.util.Objects.requireNonNull(limitStr, "limitStr cannot be null");
            java.util.Objects.requireNonNull(windowStr, "windowStr cannot be null");

            List<?> result = redisTemplate.execute(
                    (org.springframework.data.redis.core.script.RedisScript<List>) rateLimitScript,
                    keys,
                    limitStr,
                    windowStr
            );

            if (result == null || result.size() < 2) {
                // Redis unavailable — fail open (allow request)
                log.warn("Rate limiter returned null, failing open for key={}", clientKey);
                return new RateLimitResult(true, maxRequests, 0);
            }

            long currentCount = ((Number) result.get(0)).longValue();
            long ttl = ((Number) result.get(1)).longValue();
            int remaining = Math.max(0, (int) (maxRequests - currentCount));

            if (currentCount > maxRequests) {
                log.info("Rate limit exceeded: key={}, count={}, limit={}, retryAfter={}s",
                        clientKey, currentCount, maxRequests, ttl);
                return new RateLimitResult(false, 0, (int) ttl);
            }

            return new RateLimitResult(true, remaining, 0);

        } catch (Exception ex) {
            // Redis failure — fail open to avoid blocking all traffic
            log.error("Rate limiter error, failing open: key={}, error={}", clientKey, ex.getMessage());
            return new RateLimitResult(true, maxRequests, 0);
        }
    }

    /**
     * Result of a rate limit check.
     */
    public record RateLimitResult(boolean allowed, int remaining, int retryAfterSeconds) {}
}
