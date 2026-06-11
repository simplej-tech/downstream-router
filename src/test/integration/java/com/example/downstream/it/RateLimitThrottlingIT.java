package com.example.downstream.it;

import com.example.downstream.StandardDownstreamApplication;
import com.example.downstream.it.stubs.DownstreamStubs;
import com.example.downstream.it.stubs.SocialStubs;
import com.example.downstream.model.RequestMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.verification.LoggedRequest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Layer 3 — behavioral throughput IT. Proves that activating {@code kafka-rate-limit-enabled} makes
 * the real Guava limiter actually throttle the live flow end-to-end (EmbeddedKafka → listener →
 * async processor → downstream calls).
 *
 * <p>Only the BIO limiter is throttled (4 permits/sec); match/social are set effectively unlimited so
 * the bio limiter is the sole constraint. bio is the first downstream call per message and its
 * acquisitions are serialized by the shared limiter (true even under virtual-thread concurrency — all
 * threads block on the same limiter), so social (the last call) lands at the same throttled cadence.
 *
 * <p><b>Warm-up to defeat Guava's burst.</b> {@code RateLimiter.create(rate)} banks up to
 * {@code rate * 1.0s} permits while idle (4 at rate 4), so the first ~4 messages pass for free. We
 * publish {@code WARMUP + N} messages as one continuous stream; the WARMUP prefix (>= burst capacity)
 * drains the banked permits, then the limiter is in steady state. We measure ONLY the window between
 * the completion of message {@code #WARMUP} and {@code #(WARMUP+N)} — using WireMock's per-request
 * journal timestamps for the social endpoint — so the floor is an honest {@code N/rate}. FLOOR only,
 * never a tight ceiling; 0.8 tolerance absorbs low-side jitter.
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
class RateLimitThrottlingIT {

    private static final int N = 8;
    private static final int WARMUP = 6;   // >= Guava SmoothBursty burst capacity (rate * 1.0s = 4), padded
    private static final double BIO_RATE = 4.0;

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
    private ObjectMapper objectMapper;

    @Test
    void enablingProfile_throttlesProcessingToTheConfiguredBioRate() throws Exception {
        Scenarios.allHealthy(STUBS);

        int total = WARMUP + N;
        // One continuous stream: WARMUP prefix drains the burst, the rest flow at the steady rate.
        for (int i = 0; i < total; i++) {
            publish("msg-" + i);
        }
        await().atMost(Duration.ofSeconds(40)).untilAsserted(() -> STUBS.social.verifyCalled(total));

        // Precise per-call timestamps from WireMock's journal (social = last downstream call).
        List<LoggedRequest> social =
                new ArrayList<>(WIREMOCK.findAll(postRequestedFor(urlEqualTo(SocialStubs.PATH))));
        social.sort(Comparator.comparing(LoggedRequest::getLoggedDate));

        // Steady-state window: completion of msg #WARMUP .. #(WARMUP+N).
        double elapsedSeconds = (social.get(WARMUP + N - 1).getLoggedDate().getTime()
                - social.get(WARMUP - 1).getLoggedDate().getTime()) / 1000.0;

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
