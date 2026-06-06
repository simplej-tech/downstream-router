package com.example.downstream.it;

import com.example.downstream.it.stubs.DownstreamStubs;

/**
 * Whole-system stub configurations as one call: {@code Scenarios.allHealthy(stubs)} replaces
 * a fistful of per-service {@code returns()}/{@code failsWith()} lines.
 *
 * Each scenario calls {@link DownstreamStubs#resetAll()} so the prior phase's stubs and request
 * log don't leak. Tests should call {@code verify*} BEFORE the next scenario flip to avoid losing
 * the counts they're about to assert on.
 */
public final class Scenarios {

    private Scenarios() {}

    /** All three downstreams return their default healthy fixture. */
    public static void allHealthy(DownstreamStubs stubs) {
        stubs.resetAll();
        stubs.bio.returnsHealthy();
        stubs.match.returnsHealthy();
        stubs.social.returnsHealthy();
    }

    /** Bio fails (5xx); match + social healthy. */
    public static void bioFails(DownstreamStubs stubs) {
        stubs.resetAll();
        stubs.bio.fails5xx();
        stubs.match.returnsHealthy();
        stubs.social.returnsHealthy();
    }

    /** Match fails (5xx); bio + social healthy. */
    public static void matchFails(DownstreamStubs stubs) {
        stubs.resetAll();
        stubs.bio.returnsHealthy();
        stubs.match.fails5xx();
        stubs.social.returnsHealthy();
    }

    /** Social fails (5xx); bio + match healthy. */
    public static void socialFails(DownstreamStubs stubs) {
        stubs.resetAll();
        stubs.bio.returnsHealthy();
        stubs.match.returnsHealthy();
        stubs.social.fails5xx();
    }
}
