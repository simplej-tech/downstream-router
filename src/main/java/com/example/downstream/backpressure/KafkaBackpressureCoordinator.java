package com.example.downstream.backpressure;

/**
 * Translates circuit breaker state changes into Kafka consumer backpressure: pause the gated
 * listeners when a circuit opens, resume them when it recovers.
 */
public interface KafkaBackpressureCoordinator {

    void onCircuitOpened(CircuitBreakerMetaData metadata);

    void onCircuitRecovered(CircuitBreakerMetaData metadata);
}
