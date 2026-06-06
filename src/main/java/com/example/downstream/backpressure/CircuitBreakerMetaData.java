package com.example.downstream.backpressure;

import java.util.List;

/**
 * Maps a circuit breaker to the domain it guards and the {@code @KafkaListener} ids whose consumption
 * should be paused while it is open.
 */
public record CircuitBreakerMetaData(String name, String domain, List<String> consumerIds) {}
