package com.example.downstream.ratelimit;

import com.example.kafka.Profiles;
import com.example.kafka.ratelimit.DisabledRateLimiter;
import com.example.kafka.ratelimit.RateLimiterWrapper;
import com.example.kafka.ratelimit.RateLimiterWrapperImpl;
import com.google.common.util.concurrent.RateLimiter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * Wires the three downstream rate limiters (bio, match, social) for this consumer. Each named bean
 * has two profile-gated definitions:
 * <ul>
 *   <li>profile ON  → {@link RateLimiterWrapperImpl} with rate from {@code kafka.rate-limit.<name>}.</li>
 *   <li>profile OFF → {@link DisabledRateLimiter} no-op.</li>
 * </ul>
 * Bean names are stable across both modes, so call sites can {@code @Qualifier} on the fixed names
 * regardless of whether throttling is live.
 */
@Configuration
public class RateLimiterConfig {

    static final String BIO = "bioRateLimiter";
    static final String MATCH = "matchRateLimiter";
    static final String SOCIAL = "socialRateLimiter";

    // --- bio --------------------------------------------------------------------------------

    @Bean(BIO)
    @Profile(Profiles.KAFKA_RATE_LIMIT_ENABLED)
    public RateLimiterWrapper bioRateLimiterEnabled(
            @Value("${kafka.rate-limit.bio:10.0}") double permitsPerSecond) {
        return new RateLimiterWrapperImpl(RateLimiter.create(permitsPerSecond));
    }

    @Bean(BIO)
    @Profile("!" + Profiles.KAFKA_RATE_LIMIT_ENABLED)
    public RateLimiterWrapper bioRateLimiterDisabled() {
        return new DisabledRateLimiter();
    }

    // --- match ------------------------------------------------------------------------------

    @Bean(MATCH)
    @Profile(Profiles.KAFKA_RATE_LIMIT_ENABLED)
    public RateLimiterWrapper matchRateLimiterEnabled(
            @Value("${kafka.rate-limit.match:10.0}") double permitsPerSecond) {
        return new RateLimiterWrapperImpl(RateLimiter.create(permitsPerSecond));
    }

    @Bean(MATCH)
    @Profile("!" + Profiles.KAFKA_RATE_LIMIT_ENABLED)
    public RateLimiterWrapper matchRateLimiterDisabled() {
        return new DisabledRateLimiter();
    }

    // --- social -----------------------------------------------------------------------------

    @Bean(SOCIAL)
    @Profile(Profiles.KAFKA_RATE_LIMIT_ENABLED)
    public RateLimiterWrapper socialRateLimiterEnabled(
            @Value("${kafka.rate-limit.social:10.0}") double permitsPerSecond) {
        return new RateLimiterWrapperImpl(RateLimiter.create(permitsPerSecond));
    }

    @Bean(SOCIAL)
    @Profile("!" + Profiles.KAFKA_RATE_LIMIT_ENABLED)
    public RateLimiterWrapper socialRateLimiterDisabled() {
        return new DisabledRateLimiter();
    }

    // --- composite --------------------------------------------------------------------------

    @Bean
    public DownstreamRateLimiters downstreamRateLimiters(
            @Qualifier(BIO) RateLimiterWrapper bio,
            @Qualifier(MATCH) RateLimiterWrapper match,
            @Qualifier(SOCIAL) RateLimiterWrapper social) {
        return new DownstreamRateLimiters(bio, match, social);
    }
}
