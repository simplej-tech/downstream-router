package com.example.downstream.it.mockserver;

/**
 * MockServer parallel of {@link com.example.downstream.it.Scenarios}. Whole-system stub
 * configurations as one call: {@code MockScenarios.allHealthy(stubs)} replaces a fistful of
 * per-service {@code returns()}/{@code failsWith()} lines.
 *
 * <p>Each scenario calls {@link MockDownstreamStubs#resetAll()} first so the prior phase's
 * expectations and request log don't leak. This reset-then-stub ordering also matters more for
 * MockServer than WireMock: MockServer matches equal-priority expectations in INSERTION order
 * (first added wins), so without the reset a later {@code returns()} would NOT override an earlier
 * one. Tests should call {@code verify*} BEFORE the next scenario flip to avoid losing the counts.
 */
public final class MockScenarios {

    private MockScenarios() {}

    /** All three downstreams return their default healthy fixture. */
    public static void allHealthy(MockDownstreamStubs stubs) {
        stubs.resetAll();
        stubs.bio.returnsHealthy();
        stubs.match.returnsHealthy();
        stubs.social.returnsHealthy();
    }

    /** Bio fails (5xx); match + social healthy. */
    public static void bioFails(MockDownstreamStubs stubs) {
        stubs.resetAll();
        stubs.bio.fails5xx();
        stubs.match.returnsHealthy();
        stubs.social.returnsHealthy();
    }

    /** Match fails (5xx); bio + social healthy. */
    public static void matchFails(MockDownstreamStubs stubs) {
        stubs.resetAll();
        stubs.bio.returnsHealthy();
        stubs.match.fails5xx();
        stubs.social.returnsHealthy();
    }

    /** Social fails (5xx); bio + match healthy. */
    public static void socialFails(MockDownstreamStubs stubs) {
        stubs.resetAll();
        stubs.bio.returnsHealthy();
        stubs.match.returnsHealthy();
        stubs.social.fails5xx();
    }
}
