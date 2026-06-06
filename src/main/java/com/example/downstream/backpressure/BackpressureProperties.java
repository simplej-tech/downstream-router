package com.example.downstream.backpressure;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.util.List;

@ConfigurationProperties("app.backpressure")
public record BackpressureProperties(
        @DefaultValue("false") boolean enabled,
        @DefaultValue("true") boolean resumeOnHalfOpen,
        @DefaultValue List<CircuitBreakerMetaData> circuits) {}
