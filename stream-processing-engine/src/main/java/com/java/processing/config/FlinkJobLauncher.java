package com.java.processing.config;

import com.java.processing.job.FlinkStreamingJob;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Spring {@link ApplicationRunner} that starts the Flink DataStream job in a
 * daemon background thread after the application context is fully initialised.
 *
 * <p>Active only when {@code platform.flink.enabled=true}. In all other profiles
 * (local / dev) the Spring-Kafka {@link com.java.processing.consumer.EventConsumer}
 * acts as the stream-processing fallback.
 *
 * <p>The Flink job runs on a dedicated thread ({@code flink-job-thread}) so that
 * the Spring Boot main thread (and its health/metrics endpoints) remain responsive
 * throughout the job's lifetime. If the job terminates unexpectedly, the error is
 * logged but the Spring Boot process stays alive to surface the failure via the
 * {@code /actuator/health} endpoint.
 */
@Component
@ConditionalOnProperty(name = "platform.flink.enabled", havingValue = "true")
@RequiredArgsConstructor
@Slf4j
public class FlinkJobLauncher implements ApplicationRunner {

    private final FlinkStreamingJob flinkStreamingJob;

    @Override
    public void run(ApplicationArguments args) {
        log.info("Starting Flink DataStream job on daemon thread …");

        Thread jobThread = new Thread(() -> {
            try {
                flinkStreamingJob.execute();
            } catch (Exception ex) {
                log.error("Flink job terminated with an error — check Flink dashboard or logs", ex);
            }
        }, "flink-job-thread");

        jobThread.setDaemon(true);
        jobThread.start();

        log.info("Flink job thread launched. "
                + "Spring Boot will continue serving health/metrics endpoints.");
    }
}

