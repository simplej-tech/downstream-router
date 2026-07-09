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
import org.springframework.test.context.TestPropertySource;

import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Runs the same bio-failure circuit-breaker scenario as {@link StandardDownstreamProcessorCircuitBreakerIT}
 * BOTH with the social lookup enabled and disabled, proving the {@code app.social.enabled} toggle
 * doesn't change the breaker/backpressure behavior — only whether social is the last downstream call.
 *
 * <p><b>Why two subclasses instead of a {@code @ParameterizedTest}:</b> the toggle is a context-level
 * property read at bean construction ({@code @Value} in {@code AsyncProcessor}), so it's fixed for a
 * loaded {@code ApplicationContext}; exercising both states needs two contexts. The shared
 * {@code @SpringBootTest}/{@code @EmbeddedKafka} config and the whole scenario live on the abstract
 * {@link Base}; each concrete subclass adds only its {@code app.social.enabled} via
 * {@code @TestPropertySource} (reliably honored on a subclass of an annotated base) and runs the
 * scenario with the matching boolean. Each subclass owns a separate {@link WireMockServer}, so the two
 * cached contexts never share mock state.
 */
class StandardDownstreamProcessorSocialToggleCircuitBreakerIT {

    @SpringBootTest(
            classes = {StandardDownstreamApplication.class, TestEncryptionConfig.class,
                    StandardDownstreamProcessorSocialToggleCircuitBreakerIT.SingleThreadAsyncConfig.class},
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
                    // app.social.enabled is NOT set here — each subclass sets it via @TestPropertySource.
                    //
                    // Async executor determinism: prod's KafkaAppConfig uses a virtual-thread-per-task
                    // executor. For tests we activate single-thread-async + SingleThreadAsyncConfig so
                    // the bio/match/social counts are reproducible.
            })
    @EmbeddedKafka(
            topics = {"standard-downstream", "downstream-results", "downstream-dlt"},
            partitions = 1,
            brokerProperties = {
                    "transaction.state.log.replication.factor=1",
                    "transaction.state.log.min.isr=1",
                    "offsets.topic.replication.factor=1"
            })
    abstract static class Base {

        @Autowired
        private KafkaTemplate<String, byte[]> kafkaTemplate;

        @Autowired
        private CircuitBreakerRegistry circuitBreakerRegistry;

        @Autowired
        private KafkaListenerEndpointRegistry listenerRegistry;

        @Autowired
        private ObjectMapper objectMapper;

        /** Each subclass returns its own stubs (backed by its own WireMock) so contexts don't share state. */
        protected abstract DownstreamStubs stubs();

        /** Point the three downstream clients at the given WireMock; called from each subclass's @DynamicPropertySource. */
        protected static void registerDownstreamUrls(DynamicPropertyRegistry registry, WireMockServer wm) {
            registry.add("app.bio.host", () -> "localhost");
            registry.add("app.bio.port", wm::port);
            registry.add("app.bio.scheme", () -> "http");
            registry.add("app.match.base-url", () -> "http://localhost:" + wm.port());
            registry.add("app.social.base-url", () -> "http://localhost:" + wm.port());
        }

        /**
         * Three-phase scenario. {@code socialEnabled} drives only the social-specific assertions and
         * the completion barrier (social is the last call when on, match when off); the breaker /
         * backpressure behavior is asserted identically in both states.
         */
        protected void runScenario(boolean socialEnabled) throws Exception {
            DownstreamStubs stubs = stubs();
            CircuitBreaker bio = circuitBreakerRegistry.circuitBreaker("bio");
            CircuitBreaker match = circuitBreakerRegistry.circuitBreaker("match");
            CircuitBreaker social = circuitBreakerRegistry.circuitBreaker("social");

            // Phase 1 — downstreams healthy: breakers closed, listener running.
            Scenarios.allHealthy(stubs);
            publish("warm-1");
            awaitProcessed(stubs, socialEnabled, 1);
            assertThat(bio.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
            assertThat(match.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
            assertThat(social.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
            assertThat(containerPaused()).isFalse();
            stubs.bio.verifyCalled(1);
            stubs.match.verifyCalled(1);
            stubs.bio.verifyCalledFor(expectedBioRequest("warm-1"));
            stubs.match.verifyCalledFor(expectedMatchRequest("warm-1"));
            if (socialEnabled) {
                stubs.social.verifyCalledFor(expectedSocialRequest("warm-1"));
            } else {
                stubs.social.verifyNotCalled();
            }

            // Phase 2 — bio fails: bio CB opens, listener pauses. Publish exactly the minimum number
            // of messages that trips the CB (3, because phase-1's warm-1 success is still in the window:
            // [ok, fail, fail, fail] = 4 calls, 75% > 50% ⇒ CB OPEN on the 3rd failure). Without a
            // fallback, each failing bio call throws straight out of processRecord — match + social are
            // never reached, so this phase is identical regardless of the social toggle.
            Scenarios.bioFails(stubs);
            for (int i = 0; i < 3; i++) {
                publish("fail-" + i);
            }
            await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                    assertThat(bio.getState()).isEqualTo(CircuitBreaker.State.OPEN));
            assertThat(match.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
            assertThat(social.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
            await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> stubs.bio.verifyCalled(3));
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(containerPaused()).isTrue());
            stubs.match.verifyNotCalled();
            stubs.social.verifyNotCalled();
            stubs.bio.verifyCalledFor(expectedBioRequest("fail-0"));
            stubs.bio.verifyCalledFor(expectedBioRequest("fail-1"));
            stubs.bio.verifyCalledFor(expectedBioRequest("fail-2"));

            // Phase 3 — bio healthy again. Publish exactly permitted-half-open=2 probe messages: both
            // pass the HALF_OPEN gate and succeed, so CB → CLOSED on the 2nd.
            Scenarios.allHealthy(stubs);
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(containerPaused()).isFalse());
            publish("probe-0");
            publish("probe-1");
            await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                    assertThat(bio.getState()).isEqualTo(CircuitBreaker.State.CLOSED));
            awaitProcessed(stubs, socialEnabled, 2);
            stubs.bio.verifyCalled(2);
            stubs.match.verifyCalled(2);
            stubs.bio.verifyCalledFor(expectedBioRequest("probe-0"));
            stubs.bio.verifyCalledFor(expectedBioRequest("probe-1"));
            if (!socialEnabled) {
                stubs.social.verifyNotCalled();
            }
        }

        /**
         * Waits for full processing of {@code count} cumulative messages by awaiting the LAST downstream
         * call: social when enabled, else match (the last call once social is gated off).
         */
        private void awaitProcessed(DownstreamStubs stubs, boolean socialEnabled, int count) {
            if (socialEnabled) {
                await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> stubs.social.verifyCalled(count));
            } else {
                await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> stubs.match.verifyCalled(count));
            }
        }

        private boolean containerPaused() {
            return listenerRegistry.getListenerContainer("standard-downstream-processor").isContainerPaused();
        }

        private void publish(String id) throws Exception {
            RequestMessage message = new RequestMessage(id, "standard", "payload-" + id);
            byte[] payload = objectMapper.writeValueAsBytes(message);
            // Producer has a transaction-id-prefix, so a bare send() outside a txn throws. Wrap it.
            kafkaTemplate.executeInTransaction(t -> {
                try {
                    return t.send("standard-downstream", id, payload).get(10, TimeUnit.SECONDS);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }

        protected static ProcessRequest expectedBioRequest(String id) {
            return new ProcessRequest(id, "standard", "payload-" + id);
        }

        protected static MatchRequest expectedMatchRequest(String id) {
            return new MatchRequest(id, "payload-" + id);
        }

        protected static SocialRequest expectedSocialRequest(String id) {
            return new SocialRequest(id, "payload-" + id);
        }
    }

    @TestPropertySource(properties = "app.social.enabled=true")
    static class WithSocialEnabled extends Base {
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
            registerDownstreamUrls(registry, WIREMOCK);
        }

        @Override
        protected DownstreamStubs stubs() {
            return STUBS;
        }

        @Test
        void bioFailingOpensBioBreakerAndPausesListener_thenRecovers_socialEnabled() throws Exception {
            runScenario(true);
        }
    }

    @TestPropertySource(properties = "app.social.enabled=false")
    static class WithSocialDisabled extends Base {
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
            registerDownstreamUrls(registry, WIREMOCK);
        }

        @Override
        protected DownstreamStubs stubs() {
            return STUBS;
        }

        @Test
        void bioFailingOpensBioBreakerAndPausesListener_thenRecovers_socialDisabled() throws Exception {
            runScenario(false);
        }
    }

    /**
     * Test-only AsyncConfigurer: a single-thread executor so the @Async dispatches run serially and
     * the CB-record sequence is deterministic. Active only when the {@code single-thread-async}
     * profile is on. Prod's {@link com.example.downstream.config.KafkaAppConfig} is profile-gated off.
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
