package com.example.downstream;

import com.example.bio.BioQueryClient;
import com.example.bio.ProcessRequest;
import com.example.bio.ProcessResponse;
import com.example.downstream.model.DownstreamResult;
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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

/**
 * Processes a single {@link RequestMessage} through bio → match → social on the {@code @Async}
 * executor, returning a {@link CompletableFuture} of the aggregated {@link AsyncResult}. The listener
 * fans every record of a batch out through this method, awaits them all
 * ({@code CompletableFuture.allOf(...).join()}), then produces each result — so the downstream calls
 * across a batch run in parallel while the transactional produce still happens on the listener thread.
 *
 * <p>Each downstream call is preceded by a {@code rateLimiters.<service>().acquire()}. By default
 * those limiters are {@code DisabledRateLimiter} no-ops; activating the {@code kafka-rate-limit-enabled}
 * profile swaps them for real Guava-backed limiters configured via {@code kafka.rate-limit.*} yml.
 * The acquire is intentionally OUTSIDE the {@code @CircuitBreaker} annotation: if the breaker is OPEN
 * the call short-circuits to a thrown exception and the token is wasted, but the breaker pre-check
 * already runs before the method body, so most CB-OPEN cases don't even reach this code.
 *
 * <p>Downstream failures are caught here and carried in the {@link AsyncResult#exception()} (alongside
 * an {@link DownstreamResult#OUTCOME_ERROR} result) rather than thrown — so the returned future never
 * completes exceptionally and batch aggregation via {@code join()} is safe, and the listener routes
 * the failed result to the dead-letter topic. The circuit breaker still records the failure (the
 * CB-wrapping client decorator records and rethrows before this catch), so CB tripping and
 * backpressure are unaffected. A failure aborts the remaining calls (a bio failure skips match +
 * social), matching the prior behavior; those fields stay null in the error result.
 */
@Component
public class AsyncProcessor {

    private static final Logger log = LoggerFactory.getLogger(AsyncProcessor.class);

    private final BioQueryClient bioQueryClient;
    private final MatchClient matchClient;
    private final SocialClient socialClient;
    private final DownstreamRateLimiters rateLimiters;
    private final boolean socialEnabled;

    public AsyncProcessor(BioQueryClient bioQueryClient,
                          MatchClient matchClient,
                          SocialClient socialClient,
                          DownstreamRateLimiters rateLimiters,
                          @Value("${app.social.enabled:true}") boolean socialEnabled) {
        this.bioQueryClient = bioQueryClient;
        this.matchClient = matchClient;
        this.socialClient = socialClient;
        this.rateLimiters = rateLimiters;
        this.socialEnabled = socialEnabled;
    }

    @Async
    public CompletableFuture<AsyncResult> processRecord(RequestMessage message) {
        long processedAt = System.currentTimeMillis();
        try {
            rateLimiters.bio().acquire();
            ProcessResponse bio = bioQueryClient.process(
                    new ProcessRequest(message.id(), message.destination(), message.payload()));

            rateLimiters.match().acquire();
            MatchResponse match = matchClient.match(new MatchRequest(message.id(), message.payload()));

            // Social lookup is gated by app.social.enabled (default true). When false, the call — and its
            // rate-limiter acquire — are skipped entirely, so social becomes a no-op downstream.
            SocialResponse social = null;
            if (socialEnabled) {
                rateLimiters.social().acquire();
                social = socialClient.lookup(new SocialRequest(message.id(), message.payload()));
            }

            log.info("STANDARD-DOWNSTREAM received id={} payload={} bio={} match={} social={}",
                    message.id(), message.payload(), bio.status(), match.status(),
                    social != null ? social.status() : "skipped");

            DownstreamResult result = new DownstreamResult(
                    message.id(), message.destination(), DownstreamResult.OUTCOME_OK,
                    bio.status(),
                    match.status(), match.score(),
                    social != null ? social.status() : null,
                    social != null ? social.reach() : null,
                    null, processedAt);
            return CompletableFuture.completedFuture(new AsyncResult(message.id(), result, null));
        } catch (Exception e) {
            // CB already recorded + rethrew in the client decorator. Carry the failure in the result
            // (never complete the future exceptionally, so batch join() is safe); the listener routes
            // this to the dead-letter topic because exception != null.
            log.warn("STANDARD-DOWNSTREAM processing failed id={}: {}", message.id(), e.toString());
            DownstreamResult result = new DownstreamResult(
                    message.id(), message.destination(), DownstreamResult.OUTCOME_ERROR,
                    null, null, null, null, null, e.getMessage(), processedAt);
            return CompletableFuture.completedFuture(new AsyncResult(message.id(), result, e));
        }
    }
}
