package com.example.downstream.backpressure;

import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.common.circuitbreaker.configuration.CircuitBreakerConfigCustomizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;

@Configuration
@EnableConfigurationProperties(BackpressureProperties.class)
@ConditionalOnProperty(prefix = "app.backpressure", name = "enabled", havingValue = "true")
public class BackpressureConfig {

    @Autowired
    private KafkaListenerEndpointRegistry listenerRegistry;

    @Autowired
    private BackpressureProperties properties;

    @Bean
    public CircuitBreakerMetaDataRegistry circuitBreakerMetaDataRegistry() {
        return new CircuitBreakerMetaDataRegistry(properties.circuits());
    }

    @Bean
    public CircuitBreakerErrorClassifier circuitBreakerErrorClassifier() {
        return new CircuitBreakerErrorClassifier();
    }

    @Bean
    public CircuitBreakerConfigCustomizer matchCircuitBreakerConfigCustomizer(CircuitBreakerErrorClassifier classifier) {
        return new MatchCircuitBreakerConfigCustomizer(classifier);
    }

    @Bean
    public CircuitBreakerConfigCustomizer socialCircuitBreakerConfigCustomizer(CircuitBreakerErrorClassifier classifier) {
        return new SocialCircuitBreakerConfigCustomizer(classifier);
    }

    // CircuitBreakerRegistry is taken as a method parameter (not an @Autowired field) on purpose:
    // this config also declares the CircuitBreakerConfigCustomizer beans the registry is built from,
    // so field-injecting the registry here would create a bean creation cycle.
    @Bean
    public KafkaBackpressureCoordinator kafkaBackpressureCoordinator(CircuitBreakerRegistry circuitBreakerRegistry,
                                                                     CircuitBreakerMetaDataRegistry metaRegistry) {
        return new KafkaBackpressureCoordinatorImpl(listenerRegistry, circuitBreakerRegistry, metaRegistry);
    }

    @Bean
    public CircuitBreakerStateListener circuitBreakerStateListener(CircuitBreakerRegistry circuitBreakerRegistry,
                                                                   KafkaBackpressureCoordinator coordinator,
                                                                   CircuitBreakerMetaDataRegistry metaRegistry) {
        return new CircuitBreakerStateListener(circuitBreakerRegistry, coordinator, metaRegistry, properties);
    }
}
