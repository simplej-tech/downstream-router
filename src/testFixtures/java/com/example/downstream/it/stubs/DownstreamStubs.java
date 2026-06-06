package com.example.downstream.it.stubs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;

/**
 * Composes per-service stubs holders into one entry point. Tests grab the inner refs
 * ({@code STUBS.bio.fails5xx()}, {@code STUBS.match.verifyCalled(4)}), and orchestrators like
 * {@link com.example.downstream.it.Scenarios} take a {@code DownstreamStubs} to set them all at once.
 */
public class DownstreamStubs extends AbstractStubs {

    public final BioStubs bio;
    public final MatchStubs match;
    public final SocialStubs social;

    public DownstreamStubs(WireMockServer wiremock, ObjectMapper mapper) {
        super(wiremock, mapper);
        this.bio = new BioStubs(wiremock, mapper);
        this.match = new MatchStubs(wiremock, mapper);
        this.social = new SocialStubs(wiremock, mapper);
    }

    /** Clear the WireMock request log only — stubs stay. Use between phases for per-phase counts. */
    public void resetRequests() {
        wiremock.resetRequests();
    }

    /** Clear both stubs and request log — use to start fresh (typically at test start or phase swap). */
    public void resetAll() {
        wiremock.resetAll();
    }
}
