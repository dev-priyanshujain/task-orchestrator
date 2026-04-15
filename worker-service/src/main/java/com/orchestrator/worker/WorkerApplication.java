package com.orchestrator.worker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EntityScan(basePackages = "com.orchestrator.common.model")
@ComponentScan(basePackages = {"com.orchestrator.worker", "com.orchestrator.common"})
@EnableJpaRepositories(basePackages = {"com.orchestrator.worker.repository", "com.orchestrator.common.repository"})
@EnableScheduling
public class WorkerApplication {

    public static void main(String[] args) {
        SpringApplication.run(WorkerApplication.class, args);
    }
}
