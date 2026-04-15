package com.orchestrator.scheduler;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@SpringBootApplication
@EntityScan(basePackages = "com.orchestrator.common.model")
@ComponentScan(basePackages = {"com.orchestrator.scheduler", "com.orchestrator.common"})
@EnableJpaRepositories(basePackages = {"com.orchestrator.scheduler.repository", "com.orchestrator.common.repository"})
public class SchedulerApplication {

    public static void main(String[] args) {
        SpringApplication.run(SchedulerApplication.class, args);
    }
}

