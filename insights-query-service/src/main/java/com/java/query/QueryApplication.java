package com.java.query;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Entry point for the Insights Query Service (CQRS Read Path).
 * Consumes enriched aggregate events into a local OLAP store (Pinot stand-in)
 * and serves campaign insight APIs with tenant-scoped access control.
 *
 * <p>{@code @EnableScheduling} activates the periodic reconciliation jobs
 * ({@link com.java.query.reconciliation.ReconciliationJob}).
 */
@EnableKafka
@EnableScheduling
@SpringBootApplication
public class QueryApplication {
    public static void main(String[] args) {
        SpringApplication.run(QueryApplication.class, args);
    }
}

