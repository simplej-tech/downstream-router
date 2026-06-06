package com.example.downstream.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.annotation.EnableAsync;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * Wires the {@code @Async} executor for the application. A virtual-thread-per-task executor fits the
 * downstream call shape (I/O-bound, mostly waiting on HTTP) and avoids the carrier-thread-pool sizing
 * that {@link java.util.concurrent.ThreadPoolExecutor} would force.
 *
 * <p>Profile-gated off ({@code "!single-thread-async"}) so integration tests can substitute a
 * single-thread executor for deterministic call counts. The IT activates the
 * {@code single-thread-async} profile and provides its own {@link AsyncConfigurer}.
 */
@Configuration
@EnableAsync
@Profile("!single-thread-async")
public class KafkaAppConfig implements AsyncConfigurer {

    @Override
    public Executor getAsyncExecutor() {
        return Executors.newVirtualThreadPerTaskExecutor();
    }
}
