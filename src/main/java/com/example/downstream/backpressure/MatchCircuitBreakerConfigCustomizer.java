package com.example.downstream.backpressure;

import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.common.circuitbreaker.configuration.CircuitBreakerConfigCustomizer;

/**
 * Applies {@link CircuitBreakerErrorClassifier#shouldRecordForMatch} to the {@code match} breaker so
 * only genuine downstream failures (5xx / non-Api) count toward opening it.
 */
public class MatchCircuitBreakerConfigCustomizer implements CircuitBreakerConfigCustomizer {

    private final CircuitBreakerErrorClassifier classifier;

    public MatchCircuitBreakerConfigCustomizer(CircuitBreakerErrorClassifier classifier) {
        this.classifier = classifier;
    }

    @Override
    public String name() {
        return "match";
    }

    @Override
    public void customize(CircuitBreakerConfig.Builder builder) {
        builder.recordException(classifier::shouldRecordForMatch);
    }
}
