# Task Orchestrator

A distributed task scheduling and execution system built with **Java 21**, **Spring Boot 3**, **Kafka**, **PostgreSQL**, and **Redis**.

## Core Features
- **Reliable Execution**: Effectively-once task processing using distributed locks and idempotency guards.
- **Failover**: Automatic recovery of stuck tasks via a Reaper service.
- **Security**: AES-256 payload encryption for sensitive data (Payments) and JWT/API-key authentication.
- **Scalability**: Horizontally scalable workers with partition-aware scheduling.

## Prerequisites
- Java 21
- Docker & Docker Compose
- Maven

## Getting Started

1. **Start Infrastructure**:
   ```bash
   cd infra && docker compose up -d
   ```

2. **Build and Run**:
   ```bash
   mvn clean package
   java -jar scheduler-service/target/scheduler-service-1.0.0.jar
   java -jar worker-service/target/worker-service-1.0.0.jar
   java -jar reaper-service/target/reaper-service-1.0.0.jar
   ```

## API Example

**Create a Task**:
```http
POST /tasks
X-API-Key: <your-api-key>
Content-Type: application/json

{
  "idempotencyKey": "order-123",
  "type": "PAYMENT",
  "payload": "{\"amount\": 99.99}",
  "maxRetries": 3
}
```

## License
MIT
