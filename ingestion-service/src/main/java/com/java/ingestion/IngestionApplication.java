package com.java.ingestion;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Entry point for the Ingestion Service (CQRS Write Path).
 * Terminates client HTTP/JSON, enforces multi-tenant context, validates the
 * schema, and produces partitioned events onto the Kafka raw stream.
 */
@SpringBootApplication
public class IngestionApplication {
    public static void main(String[] args) {
        SpringApplication.run(IngestionApplication.class, args);
    }
}

