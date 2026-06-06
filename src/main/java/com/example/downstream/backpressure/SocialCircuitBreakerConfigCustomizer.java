package com.example.downstream.backpressure;

import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.common.circuitbreaker.configuration.CircuitBreakerConfigCustomizer;

/**
 * Applies {@link CircuitBreakerErrorClassifier#shouldRecordForSocial} to the {@code social} breaker so
 * only genuine downstream failures (5xx / non-Api) count toward opening it.
 */
public class SocialCircuitBreakerConfigCustomizer implements CircuitBreakerConfigCustomizer {

    private final CircuitBreakerErrorClassifier classifier;

    public SocialCircuitBreakerConfigCustomizer(CircuitBreakerErrorClassifier classifier) {
        this.classifier = classifier;
    }

    @Override
    public String name() {
        return "social";
    }

    @Override
    public void customize(CircuitBreakerConfig.Builder builder) {
        builder.recordException(classifier::shouldRecordForSocial);
    }
}
