package com.orchestrator.scheduler.security;

import io.jsonwebtoken.*;
import io.jsonwebtoken.security.Keys;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.crypto.SecretKey;
import java.util.Base64;
import java.util.Date;

/**
 * JWT utility for generating and validating HMAC-SHA256 tokens.
 */
@Component
public class JwtUtil {

    private final SecretKey key;
    private final long expirationMs;

    public JwtUtil(
            @Value("${app.security.jwt.secret}") String base64Secret,
            @Value("${app.security.jwt.expiration-ms}") long expirationMs) {
        this.key = Keys.hmacShaKeyFor(Base64.getDecoder().decode(base64Secret));
        this.expirationMs = expirationMs;
    }

    /**
     * Generates a signed JWT for the given subject (e.g. client name or user ID).
     */
    public String generateToken(String subject) {
        Date now = new Date();
        return Jwts.builder()
                .subject(subject)
                .issuedAt(now)
                .expiration(new Date(now.getTime() + expirationMs))
                .signWith(key)
                .compact();
    }

    /**
     * Validates the token and extracts claims.
     * Throws JwtException on any validation failure (expired, tampered, etc.).
     */
    public Claims validateToken(String token) {
        return Jwts.parser()
                .verifyWith(key)
                .build()
                .parseSignedClaims(token)
                .getPayload();
    }

    /**
     * Extracts the subject from a valid token.
     */
    public String getSubject(String token) {
        return validateToken(token).getSubject();
    }
}
