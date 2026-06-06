package com.example.downstream.it;

import com.example.bio.ProcessRequest;
import com.example.downstream.StandardDownstreamApplication;
import com.example.downstream.it.Scenarios;
import com.example.downstream.it.stubs.DownstreamStubs;
import com.example.downstream.model.RequestMessage;
import com.example.match.MatchRequest;
import com.example.social.SocialRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Exercises the standard-downstream-processor — single listener gated by THREE breakers
 * (bio + match + social). Drives the bio downstream to fail to validate that:
 *   - bio's distinct recording path (ES ResponseException → BioQueryException → yml record-exceptions),
 *   - the AND backpressure logic (one open breaker pauses the shared listener),
 *   - the other two breakers stay CLOSED while bio cycles,
 *   - recovery (bio HALF_OPEN probe succeeds → CLOSED → listener resumes).
 * Encryption stays on via a stub data-key provider.
 */
@SpringBootTest(
        classes = {StandardDownstreamApplication.class, TestEncryptionConfig.class,
                StandardDownstreamProcessorCircuitBreakerIT.SingleThreadAsyncConfig.class},
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {
                "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
                "spring.profiles.include=single-thread-async",
                "kafka.admin.enabled=false",
                "resilience4j.circuitbreaker.instances.bio.sliding-window-type=COUNT_BASED",
                "resilience4j.circuitbreaker.instances.bio.sliding-window-size=4",
                "resilience4j.circuitbreaker.instances.bio.minimum-number-of-calls=4",
                "resilience4j.circuitbreaker.instances.bio.failure-rate-threshold=50",
                "resilience4j.circuitbreaker.instances.bio.wait-duration-in-open-state=1s",
                "resilience4j.circuitbreaker.instances.bio.permitted-number-of-calls-in-half-open-state=2",
                "resilience4j.circuitbreaker.instances.bio.automatic-transition-from-open-to-half-open-enabled=true",
                "resilience4j.circuitbreaker.instances.match.sliding-window-type=COUNT_BASED",
                "resilience4j.circuitbreaker.instances.match.sliding-window-size=4",
                "resilience4j.circuitbreaker.instances.match.minimum-number-of-calls=4",
                "resilience4j.circuitbreaker.instances.match.failure-rate-threshold=50",
                "resilience4j.circuitbreaker.instances.match.wait-duration-in-open-state=1s",
                "resilience4j.circuitbreaker.instances.match.permitted-number-of-calls-in-half-open-state=2",
                "resilience4j.circuitbreaker.instances.match.automatic-transition-from-open-to-half-open-enabled=true",
                "resilience4j.circuitbreaker.instances.social.sliding-window-type=COUNT_BASED",
                "resilience4j.circuitbreaker.instances.social.sliding-window-size=4",
                "resilience4j.circuitbreaker.instances.social.minimum-number-of-calls=4",
                "resilience4j.circuitbreaker.instances.social.failure-rate-threshold=50",
                "resilience4j.circuitbreaker.instances.social.wait-duration-in-open-state=1s",
                "resilience4j.circuitbreaker.instances.social.permitted-number-of-calls-in-half-open-state=2",
                "resilience4j.circuitbreaker.instances.social.automatic-transition-from-open-to-half-open-enabled=true"
                // Async executor determinism: prod's KafkaAppConfig uses a virtual-thread-per-task
                // executor (unbounded concurrency). For tests we activate the single-thread-async
                // profile and substitute SingleThreadAsyncConfig below so the bio/match/social counts
                // are reproducible.
        }
)
@EmbeddedKafka(
        topics = {"standard-downstream"},
        partitions = 1,
        brokerProperties = {
                "transaction.state.log.replication.factor=1",
                "transaction.state.log.min.isr=1",
                "offsets.topic.replication.factor=1"
        })
class StandardDownstreamProcessorCircuitBreakerIT {

    private static final WireMockServer WIREMOCK = new WireMockServer(options().dynamicPort());
    private static final DownstreamStubs STUBS;

    static {
        WIREMOCK.start();
        STUBS = new DownstreamStubs(WIREMOCK, new ObjectMapper());
    }

    @AfterAll
    static void stopWiremock() {
        WIREMOCK.stop();
    }

    @DynamicPropertySource
    static void downstreamProps(DynamicPropertyRegistry registry) {
        registry.add("app.bio.host", () -> "localhost");
        registry.add("app.bio.port", WIREMOCK::port);
        registry.add("app.bio.scheme", () -> "http");
        registry.add("app.match.base-url", () -> "http://localhost:" + WIREMOCK.port());
        registry.add("app.social.base-url", () -> "http://localhost:" + WIREMOCK.port());
    }

    @Autowired
    private KafkaTemplate<String, byte[]> kafkaTemplate;

    @Autowired
    private CircuitBreakerRegistry circuitBreakerRegistry;

    @Autowired
    private KafkaListenerEndpointRegistry listenerRegistry;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void bioFailingOpensBioBreakerAndPausesListener_othersUnaffected_thenRecovers() throws Exception {
        CircuitBreaker bio = circuitBreakerRegistry.circuitBreaker("bio");
        CircuitBreaker match = circuitBreakerRegistry.circuitBreaker("match");
        CircuitBreaker social = circuitBreakerRegistry.circuitBreaker("social");

        // Phase 1 — all three downstreams healthy: breakers closed, listener running.
        // Await on social (the LAST call) to confirm full processing of warm-1.
        Scenarios.allHealthy(STUBS);
        publish("warm-1");
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> STUBS.social.verifyCalled(1));
        assertThat(bio.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
        assertThat(match.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
        assertThat(social.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
        assertThat(containerPaused()).isFalse();
        STUBS.bio.verifyCalled(1);
        STUBS.match.verifyCalled(1);
        STUBS.bio.verifyCalledFor(expectedBioRequest("warm-1"));
        STUBS.match.verifyCalledFor(expectedMatchRequest("warm-1"));
        STUBS.social.verifyCalledFor(expectedSocialRequest("warm-1"));

        // Phase 2 — bio fails: bio CB opens, listener pauses. Publish exactly the minimum number
        // of messages that trips the CB (3, because phase-1's warm-1 success is still in the window:
        // [ok, fail, fail, fail] = 4 calls, 75% > 50% ⇒ CB OPEN on the 3rd failure). Publishing more
        // than the minimum produces a race between (a) the listener polling additional messages and
        // (b) the CB-open transition triggering backpressure pause; the trailing record can stay
        // uncommitted on the topic and replay during phase 3.
        //
        // Without a fallback, each failing bio call throws straight out of processRecord — match +
        // social are never reached for any of the 3 messages.
        Scenarios.bioFails(STUBS);
        for (int i = 0; i < 3; i++) {
            publish("fail-" + i);
        }
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(bio.getState()).isEqualTo(CircuitBreaker.State.OPEN));
        assertThat(match.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
        assertThat(social.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> STUBS.bio.verifyCalled(3));
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(containerPaused()).isTrue());
        STUBS.match.verifyNotCalled();
        STUBS.social.verifyNotCalled();
        STUBS.bio.verifyCalledFor(expectedBioRequest("fail-0"));
        STUBS.bio.verifyCalledFor(expectedBioRequest("fail-1"));
        STUBS.bio.verifyCalledFor(expectedBioRequest("fail-2"));

        // Phase 3 — bio healthy again. Publish exactly permitted-half-open=2 probe messages: both
        // pass the HALF_OPEN gate and succeed, so CB → CLOSED on the 2nd. With async drain in phase 2,
        // no queued messages exist to act as probes, so we drive them ourselves.
        Scenarios.allHealthy(STUBS);
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(containerPaused()).isFalse());
        publish("probe-0");
        publish("probe-1");
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                assertThat(bio.getState()).isEqualTo(CircuitBreaker.State.CLOSED));
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> STUBS.social.verifyCalled(2));
        STUBS.bio.verifyCalled(2);
        STUBS.match.verifyCalled(2);
        STUBS.bio.verifyCalledFor(expectedBioRequest("probe-0"));
        STUBS.bio.verifyCalledFor(expectedBioRequest("probe-1"));
    }

    private boolean containerPaused() {
        return listenerRegistry.getListenerContainer("standard-downstream-processor").isContainerPaused();
    }

    private void publish(String id) throws Exception {
        RequestMessage message = new RequestMessage(id, "standard", "payload-" + id);
        byte[] payload = objectMapper.writeValueAsBytes(message);
        // The producer factory now has a transaction-id-prefix (so kafkaTransactionManager can wrap
        // the listener), which makes bare send() outside a transaction throw. Wrap in a local txn.
        kafkaTemplate.executeInTransaction(t -> {
            try {
                return t.send("standard-downstream", id, payload).get(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private static ProcessRequest expectedBioRequest(String id) {
        return new ProcessRequest(id, "standard", "payload-" + id);
    }

    private static MatchRequest expectedMatchRequest(String id) {
        return new MatchRequest(id, "payload-" + id);
    }

    private static SocialRequest expectedSocialRequest(String id) {
        return new SocialRequest(id, "payload-" + id);
    }

    /**
     * Test-only AsyncConfigurer: a single-thread executor so the @Async dispatches run serially and
     * the CB-record sequence is deterministic. Active only when the {@code single-thread-async}
     * profile is on (the IT sets {@code spring.profiles.include} above). Prod's
     * {@link com.example.downstream.config.KafkaAppConfig} is profile-gated off in that case.
     */
    @TestConfiguration(proxyBeanMethods = false)
    @EnableAsync
    @Profile("single-thread-async")
    static class SingleThreadAsyncConfig implements AsyncConfigurer {
        @Override
        public Executor getAsyncExecutor() {
            return Executors.newSingleThreadExecutor();
        }
    }
}
