package com.example.downstream.it.mockserver;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.mockserver.client.MockServerClient;
import org.mockserver.model.ClearType;

import static org.mockserver.model.HttpRequest.request;

/**
 * MockServer parallel of {@link com.example.downstream.it.stubs.DownstreamStubs}. Composes the
 * per-service stubs holders into one entry point; tests grab the inner refs
 * ({@code STUBS.bio.fails5xx()}, {@code STUBS.match.verifyCalled(4)}), and orchestrators like
 * {@link MockScenarios} take a {@code MockDownstreamStubs} to set them all at once.
 */
public class MockDownstreamStubs extends MockAbstractStubs {

    public final MockBioStubs bio;
    public final MockMatchStubs match;
    public final MockSocialStubs social;

    public MockDownstreamStubs(MockServerClient client, ObjectMapper mapper) {
        super(client, mapper);
        this.bio = new MockBioStubs(client, mapper);
        this.match = new MockMatchStubs(client, mapper);
        this.social = new MockSocialStubs(client, mapper);
    }

    /** Clear the recorded-request log only — expectations stay. (WireMock: {@code resetRequests()}.) */
    public void resetRequests() {
        client.clear(request(), ClearType.LOG);
    }

    /** Clear both expectations and request log — start fresh. (WireMock: {@code resetAll()}.) */
    public void resetAll() {
        client.reset();
    }
}
