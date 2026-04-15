package com.orchestrator.reaper;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EntityScan(basePackages = "com.orchestrator.common.model")
@ComponentScan(basePackages = {"com.orchestrator.reaper", "com.orchestrator.common"})
@EnableJpaRepositories(basePackages = {"com.orchestrator.reaper.repository", "com.orchestrator.common.repository"})
public class ReaperApplication {

    public static void main(String[] args) {
        SpringApplication.run(ReaperApplication.class, args);
    }
}
