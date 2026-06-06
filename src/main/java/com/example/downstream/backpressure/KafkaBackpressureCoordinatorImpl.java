package com.example.downstream.backpressure;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;

/**
 * Pauses/resumes Kafka listener containers in response to circuit state. A listener resumes only when
 * none of the circuits gating it is still open (a listener can depend on more than one downstream).
 */
public class KafkaBackpressureCoordinatorImpl implements KafkaBackpressureCoordinator {

    private static final Logger log = LoggerFactory.getLogger(KafkaBackpressureCoordinatorImpl.class);

    private final KafkaListenerEndpointRegistry listenerRegistry;
    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final CircuitBreakerMetaDataRegistry metaRegistry;

    public KafkaBackpressureCoordinatorImpl(KafkaListenerEndpointRegistry listenerRegistry,
                                            CircuitBreakerRegistry circuitBreakerRegistry,
                                            CircuitBreakerMetaDataRegistry metaRegistry) {
        this.listenerRegistry = listenerRegistry;
        this.circuitBreakerRegistry = circuitBreakerRegistry;
        this.metaRegistry = metaRegistry;
    }

    @Override
    public void onCircuitOpened(CircuitBreakerMetaData metadata) {
        for (String consumerId : metadata.consumerIds()) {
            pause(consumerId, metadata.name());
        }
    }

    @Override
    public void onCircuitRecovered(CircuitBreakerMetaData metadata) {
        for (String consumerId : metadata.consumerIds()) {
            if (noOtherOpenCircuit(consumerId)) {
                resume(consumerId, metadata.name());
            }
        }
    }

    private boolean noOtherOpenCircuit(String consumerId) {
        return metaRegistry.all().stream()
                .filter(meta -> meta.consumerIds().contains(consumerId))
                .map(meta -> circuitBreakerRegistry.circuitBreaker(meta.name()).getState())
                .noneMatch(state -> state == CircuitBreaker.State.OPEN || state == CircuitBreaker.State.FORCED_OPEN);
    }

    private void pause(String consumerId, String cbName) {
        MessageListenerContainer container = listenerRegistry.getListenerContainer(consumerId);
        if (container == null) {
            log.warn("backpressure: no listener container '{}' to pause (cb={})", consumerId, cbName);
            return;
        }
        if (!container.isContainerPaused()) {
            container.pause();
            log.info("backpressure: paused listener '{}' (cb={} open)", consumerId, cbName);
        }
    }

    private void resume(String consumerId, String cbName) {
        MessageListenerContainer container = listenerRegistry.getListenerContainer(consumerId);
        if (container == null) {
            log.warn("backpressure: no listener container '{}' to resume (cb={})", consumerId, cbName);
            return;
        }
        if (container.isContainerPaused()) {
            container.resume();
            log.info("backpressure: resumed listener '{}' (cb={} recovered)", consumerId, cbName);
        }
    }
}
