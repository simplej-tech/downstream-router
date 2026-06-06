package com.example.downstream;

import com.example.bio.BioQueryClient;
import com.example.bio.ProcessRequest;
import com.example.bio.ProcessResponse;
import com.example.downstream.model.RequestMessage;
import com.example.downstream.ratelimit.DownstreamRateLimiters;
import com.example.match.MatchClient;
import com.example.match.MatchRequest;
import com.example.match.MatchResponse;
import com.example.social.SocialClient;
import com.example.social.SocialRequest;
import com.example.social.SocialResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Off-listener-thread processor for a single {@link RequestMessage}. The {@code @Async} method runs
 * on Spring's task executor (default {@code SimpleAsyncTaskExecutor} unless a {@code TaskExecutor}
 * bean is provided), so the listener returns to polling as soon as it has decoded + dispatched.
 *
 * <p>Each downstream call is preceded by a {@code rateLimiters.<service>().acquire()}. By default
 * those limiters are {@code DisabledRateLimiter} no-ops; activating the {@code kafka-rate-limit-enabled}
 * profile swaps them for real Guava-backed limiters configured via {@code kafka.rate-limit.*} yml.
 * The acquire is intentionally OUTSIDE the {@code @CircuitBreaker} annotation: if the breaker is OPEN
 * the call short-circuits to a thrown exception and the token is wasted, but the breaker pre-check
 * already runs before the method body, so most CB-OPEN cases don't even reach this code.
 *
 * <p>Async + the listener's {@code @Transactional("kafkaTransactionManager")} have a tradeoff worth
 * naming: the Kafka transaction (which includes the offset commit) commits when the listener method
 * returns, BEFORE the async work completes. That moves delivery semantics toward at-most-once for
 * the downstream calls — if the app dies between dispatch and async completion, the offset is
 * already committed and the work is lost. Acceptable for fire-and-forget side effects; not for
 * "must process every message" cases.
 */
@Component
public class AsyncProcessor {

    private static final Logger log = LoggerFactory.getLogger(AsyncProcessor.class);

    private final BioQueryClient bioQueryClient;
    private final MatchClient matchClient;
    private final SocialClient socialClient;
    private final DownstreamRateLimiters rateLimiters;

    public AsyncProcessor(BioQueryClient bioQueryClient,
                          MatchClient matchClient,
                          SocialClient socialClient,
                          DownstreamRateLimiters rateLimiters) {
        this.bioQueryClient = bioQueryClient;
        this.matchClient = matchClient;
        this.socialClient = socialClient;
        this.rateLimiters = rateLimiters;
    }

    @Async
    public void processRecord(RequestMessage message) {
        rateLimiters.bio().acquire();
        ProcessResponse bio = bioQueryClient.process(
                new ProcessRequest(message.id(), message.destination(), message.payload()));

        rateLimiters.match().acquire();
        MatchResponse match = matchClient.match(new MatchRequest(message.id(), message.payload()));

        rateLimiters.social().acquire();
        SocialResponse social = socialClient.lookup(new SocialRequest(message.id(), message.payload()));

        log.info("STANDARD-DOWNSTREAM received id={} payload={} bio={} match={} social={}",
                message.id(), message.payload(), bio.status(), match.status(), social.status());
    }
}
