package com.example.downstream.it.fixtures;

import com.example.bio.ProcessResponse;

/** Factory for {@link ProcessResponse} fixtures used as WireMock response bodies. */
public final class BioResponses {

    private BioResponses() {}

    /** Default healthy response. */
    public static ProcessResponse ok() {
        return new ProcessResponse("x", "ok");
    }
}
