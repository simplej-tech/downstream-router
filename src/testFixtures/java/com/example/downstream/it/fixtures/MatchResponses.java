package com.example.downstream.it.fixtures;

import com.example.match.MatchResponse;

/** Factory for {@link MatchResponse} fixtures used as WireMock response bodies. */
public final class MatchResponses {

    private MatchResponses() {}

    /** Default healthy response. */
    public static MatchResponse ok() {
        return new MatchResponse("x", "ok", 0.9);
    }
}
