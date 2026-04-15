package com.orchestrator.scheduler.security;

import io.jsonwebtoken.JwtException;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.List;

/**
 * Authentication filter supporting two strategies:
 * 1. Bearer JWT token in the Authorization header
 * 2. Static API key in the X-API-Key header
 *
 * If neither is present or valid, the request continues unauthenticated
 * and Spring Security will return 401 for protected endpoints.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class JwtAuthFilter extends OncePerRequestFilter {

    private final JwtUtil jwtUtil;

    @Value("${app.security.api-key}")
    private String apiKey;

    @Override
    protected void doFilterInternal(@org.springframework.lang.NonNull HttpServletRequest request,
                                    @org.springframework.lang.NonNull HttpServletResponse response,
                                    @org.springframework.lang.NonNull FilterChain filterChain) throws ServletException, IOException {

        String authHeader = request.getHeader("Authorization");
        String apiKeyHeader = request.getHeader("X-API-Key");

        // Strategy 1: Bearer JWT token
        if (authHeader != null && authHeader.startsWith("Bearer ")) {
            String token = authHeader.substring(7);
            try {
                String subject = jwtUtil.getSubject(token);
                setAuthentication(subject);
                log.debug("JWT auth successful for subject={}", subject);
            } catch (JwtException ex) {
                log.warn("JWT validation failed: {}", ex.getMessage());
            }
        }
        // Strategy 2: Static API key
        else if (apiKeyHeader != null && apiKeyHeader.equals(apiKey)) {
            setAuthentication("api-key-client");
            log.debug("API key auth successful");
        }

        filterChain.doFilter(request, response);
    }

    private void setAuthentication(String principal) {
        UsernamePasswordAuthenticationToken auth =
                new UsernamePasswordAuthenticationToken(principal, null, List.of());
        SecurityContextHolder.getContext().setAuthentication(auth);
    }
}
