package com.example.downstream.backpressure;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.circuitbreaker.event.CircuitBreakerOnCallNotPermittedEvent;
import io.github.resilience4j.circuitbreaker.event.CircuitBreakerOnErrorEvent;
import io.github.resilience4j.circuitbreaker.event.CircuitBreakerOnStateTransitionEvent;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Subscribes to the lifecycle of every circuit breaker named in the backpressure metadata and drives
 * the {@link KafkaBackpressureCoordinator}. Pauses on OPEN/FORCED_OPEN; resumes on CLOSED and (when
 * {@code resumeOnHalfOpen}) HALF_OPEN — the latter is required so a paused consumer can produce the
 * probe calls that decide whether the breaker closes or re-opens.
 */
public class CircuitBreakerStateListener {

    private static final Logger log = LoggerFactory.getLogger(CircuitBreakerStateListener.class);

    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final KafkaBackpressureCoordinator coordinator;
    private final CircuitBreakerMetaDataRegistry metaRegistry;
    private final BackpressureProperties properties;

    public CircuitBreakerStateListener(CircuitBreakerRegistry circuitBreakerRegistry,
                                       KafkaBackpressureCoordinator coordinator,
                                       CircuitBreakerMetaDataRegistry metaRegistry,
                                       BackpressureProperties properties) {
        this.circuitBreakerRegistry = circuitBreakerRegistry;
        this.coordinator = coordinator;
        this.metaRegistry = metaRegistry;
        this.properties = properties;
    }

    @PostConstruct
    public void register() {
        circuitBreakerRegistry.getAllCircuitBreakers().forEach(this::attach);
        circuitBreakerRegistry.getEventPublisher()
                .onEntryAdded(event -> attach(event.getAddedEntry()));
    }

    private void attach(CircuitBreaker circuitBreaker) {
        if (metaRegistry.byName(circuitBreaker.getName()) == null) {
            return;
        }
        circuitBreaker.getEventPublisher()
                .onStateTransition(this::onStateTransition)
                .onError(this::onError)
                .onCallNotPermitted(this::onCallNotPermitted);
    }

    private void onStateTransition(CircuitBreakerOnStateTransitionEvent event) {
        CircuitBreakerMetaData meta = metaRegistry.byName(event.getCircuitBreakerName());
        if (meta == null) {
            return;
        }
        CircuitBreaker.State to = event.getStateTransition().getToState();
        switch (to) {
            case OPEN, FORCED_OPEN -> coordinator.onCircuitOpened(meta);
            case HALF_OPEN -> {
                if (properties.resumeOnHalfOpen()) {
                    coordinator.onCircuitRecovered(meta);
                }
            }
            case CLOSED -> coordinator.onCircuitRecovered(meta);
            default -> { }
        }
    }

    private void onError(CircuitBreakerOnErrorEvent event) {
        log.debug("cb={} recorded failure: {}", event.getCircuitBreakerName(), event.getThrowable().toString());
    }

    private void onCallNotPermitted(CircuitBreakerOnCallNotPermittedEvent event) {
        log.debug("cb={} rejected call (circuit open)", event.getCircuitBreakerName());
    }
}
