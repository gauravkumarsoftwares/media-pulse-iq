package com.java.processing;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

/**
 * Entry point for the Stream Processing Engine (Flink-style consumer).
 * Consumes the raw event stream, deduplicates, performs sessionized
 * click-to-basket attribution joins, and emits enriched events to the
 * aggregates topic for the serving layer.
 */
@EnableKafka
@SpringBootApplication
public class ProcessingApplication {
    public static void main(String[] args) {
        SpringApplication.run(ProcessingApplication.class, args);
    }
}

