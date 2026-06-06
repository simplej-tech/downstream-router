package com.example.downstream.it.fixtures;

import com.example.social.SocialResponse;

/** Factory for {@link SocialResponse} fixtures used as WireMock response bodies. */
public final class SocialResponses {

    private SocialResponses() {}

    /** Default healthy response. */
    public static SocialResponse ok() {
        return new SocialResponse("x", "ok", 1000L);
    }
}
