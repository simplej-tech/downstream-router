package com.example.downstream.ratelimit;

import com.example.kafka.ratelimit.RateLimiterWrapper;

/**
 * Composite holder for the three downstream rate limiters used by the standard-downstream consumer
 * (bio, match, social). Routes-side throttling lives in {@code main-router}'s own config; it
 * isn't part of this composite.
 *
 * <p>The three members are themselves {@link RateLimiterWrapper} beans whose concrete type is decided
 * by {@link RateLimiterConfig} — real Guava-backed under the {@code kafka-rate-limit-enabled}
 * profile, no-op {@code DisabledRateLimiter} otherwise.
 */
public class DownstreamRateLimiters {

    private final RateLimiterWrapper bio;
    private final RateLimiterWrapper match;
    private final RateLimiterWrapper social;

    public DownstreamRateLimiters(RateLimiterWrapper bio, RateLimiterWrapper match, RateLimiterWrapper social) {
        this.bio = bio;
        this.match = match;
        this.social = social;
    }

    public RateLimiterWrapper bio() {
        return bio;
    }

    public RateLimiterWrapper match() {
        return match;
    }

    public RateLimiterWrapper social() {
        return social;
    }
}
