package com.example.downstream.it;

import com.example.downstream.StandardDownstreamApplication;
import com.example.downstream.it.mockserver.MockDownstreamStubs;
import com.example.downstream.it.mockserver.MockScenarios;
import com.example.downstream.model.RequestMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.mockserver.client.MockServerClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MockServerContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * MockServer-backed twin of {@link RateLimitThrottlingIT} — same throughput assertion, HTTP mock
 * swapped (WireMock → MockServer). Kept separate so the WireMock IT is untouched. MockServer runs
 * out-of-process in a Testcontainers container (MockServer 5.15 can't embed cleanly in this Spring
 * Boot JVM); requires Docker.
 *
 * <p>Proves that activating {@code kafka-rate-limit-enabled} makes the real Guava limiter actually
 * throttle the live flow end-to-end (EmbeddedKafka → listener → async processor → downstream calls).
 * Only the BIO limiter is throttled (4 permits/sec); match/social are effectively unlimited so the
 * bio limiter is the sole constraint. bio is the first downstream call per message and its
 * acquisitions are serialized by the shared limiter, so social (the last call) lands at the same
 * throttled cadence.
 *
 * <p><b>Warm-up to defeat Guava's burst.</b> {@code RateLimiter.create(rate)} banks up to
 * {@code rate * 1.0s} permits while idle (4 at rate 4), so the first ~4 messages pass for free. We
 * publish {@code WARMUP + N} messages as one continuous stream; the WARMUP prefix (>= burst capacity)
 * drains the banked permits, then the limiter is in steady state. We measure ONLY the window between
 * the WARMUP-th and (WARMUP+N)-th social call — an honest floor of {@code N/rate}. FLOOR only, never
 * a tight ceiling; 0.8 tolerance absorbs low-side jitter.
 *
 * <p><b>The one deviation from the WireMock twin:</b> WireMock exposes a per-request journal with
 * receive timestamps ({@code LoggedRequest.getLoggedDate()}), so it measures the window retrospectively
 * after all messages land. MockServer's {@code retrieveRecordedRequests} returns matched requests
 * <em>without</em> per-request timestamps, so instead we stamp wall-clock time here as the social
 * call count crosses WARMUP and WARMUP+N (poll-bounded to ±10ms, negligible against the ~2s window).
 */
@SpringBootTest(
        classes = {StandardDownstreamApplication.class, TestEncryptionConfig.class},
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {
                "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
                "spring.profiles.include=kafka-rate-limit-enabled",
                "kafka.admin.enabled=false",
                "kafka.rate-limit.bio=4.0",
                "kafka.rate-limit.match=1000.0",
                "kafka.rate-limit.social=1000.0"
        })
@EmbeddedKafka(
        topics = {"standard-downstream"},
        partitions = 1,
        brokerProperties = {
                "transaction.state.log.replication.factor=1",
                "transaction.state.log.min.isr=1",
                "offsets.topic.replication.factor=1"
        })
class RateLimitThrottlingMockServerIT {

    private static final int N = 8;
    private static final int WARMUP = 6;   // >= Guava SmoothBursty burst capacity (rate * 1.0s = 4), padded
    private static final double BIO_RATE = 4.0;

    private static final MockServerContainer MOCKSERVER =
            new MockServerContainer(DockerImageName.parse("mockserver/mockserver:mockserver-5.15.0"));
    private static final MockDownstreamStubs STUBS;

    static {
        MOCKSERVER.start();
        STUBS = new MockDownstreamStubs(
                new MockServerClient(MOCKSERVER.getHost(), MOCKSERVER.getServerPort()), new ObjectMapper());
    }

    @AfterAll
    static void stopMockServer() {
        MOCKSERVER.stop();
    }

    @DynamicPropertySource
    static void downstreamProps(DynamicPropertyRegistry registry) {
        registry.add("app.bio.host", MOCKSERVER::getHost);
        registry.add("app.bio.port", MOCKSERVER::getServerPort);
        registry.add("app.bio.scheme", () -> "http");
        registry.add("app.match.base-url", () -> "http://" + MOCKSERVER.getHost() + ":" + MOCKSERVER.getServerPort());
        registry.add("app.social.base-url", () -> "http://" + MOCKSERVER.getHost() + ":" + MOCKSERVER.getServerPort());
    }

    @Autowired
    private KafkaTemplate<String, byte[]> kafkaTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void enablingProfile_throttlesProcessingToTheConfiguredBioRate() throws Exception {
        MockScenarios.allHealthy(STUBS);

        int total = WARMUP + N;
        // One continuous stream: WARMUP prefix drains the burst, the rest flow at the steady rate.
        for (int i = 0; i < total; i++) {
            publish("msg-" + i);
        }

        // Steady-state window: stamp wall-clock as social (the last downstream call) crosses the
        // WARMUP-th and (WARMUP+N)-th call. Poll fast so the stamp is close to the actual crossing.
        await().atMost(Duration.ofSeconds(40)).pollInterval(Duration.ofMillis(10))
                .until(() -> STUBS.social.callCount() >= WARMUP);
        long windowStartNanos = System.nanoTime();

        await().atMost(Duration.ofSeconds(40)).pollInterval(Duration.ofMillis(10))
                .until(() -> STUBS.social.callCount() >= total);
        long windowEndNanos = System.nanoTime();

        double elapsedSeconds = (windowEndNanos - windowStartNanos) / 1_000_000_000.0;

        double floorSeconds = N / BIO_RATE * 0.8;
        assertThat(elapsedSeconds)
                .as("steady-state processing of %d messages at bio %.1f/s should take at least %.2fs", N, BIO_RATE, floorSeconds)
                .isGreaterThanOrEqualTo(floorSeconds);

        STUBS.bio.verifyCalled(total);
    }

    private void publish(String id) throws Exception {
        RequestMessage message = new RequestMessage(id, "standard", "payload-" + id);
        byte[] payload = objectMapper.writeValueAsBytes(message);
        kafkaTemplate.executeInTransaction(t -> {
            try {
                return t.send("standard-downstream", id, payload).get(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }
}
