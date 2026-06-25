package com.example.downstream.it.mockserver;

import com.example.bio.ProcessRequest;
import com.example.bio.ProcessResponse;
import com.example.downstream.it.fixtures.BioResponses;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.mockserver.client.MockServerClient;
import org.mockserver.matchers.MatchType;
import org.mockserver.verify.VerificationTimes;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;
import static org.mockserver.model.JsonBody.json;

/**
 * MockServer parallel of {@link com.example.downstream.it.stubs.BioStubs} — bio downstream
 * (low-level ES RestClient, POST /process).
 */
public class MockBioStubs extends MockAbstractStubs {

    public static final String PATH = "/process";

    public MockBioStubs(MockServerClient client, ObjectMapper mapper) {
        super(client, mapper);
    }

    // -- stubbing --------------------------------------------------------------------------------

    public void returnsHealthy() {
        returns(BioResponses.ok());
    }

    public void returns(ProcessResponse response) {
        client.when(request().withMethod("POST").withPath(PATH)).respond(okJson(response));
    }

    public void failsWith(int status) {
        client.when(request().withMethod("POST").withPath(PATH))
                .respond(response().withStatusCode(status));
    }

    /** 500 (not 5xx in general) — the ES RestClient retries 502/503/504 as dead-node candidates. */
    public void fails5xx() {
        failsWith(500);
    }

    public void fails4xx() {
        failsWith(400);
    }

    // -- verification ----------------------------------------------------------------------------

    /** Asserts exactly {@code times} POSTs to /process (regardless of body). */
    public void verifyCalled(int times) {
        client.verify(request().withMethod("POST").withPath(PATH), VerificationTimes.exactly(times));
    }

    public void verifyNotCalled() {
        verifyCalled(0);
    }

    /** Asserts >= 1 POST whose body matches the given {@link ProcessRequest} field-for-field. */
    public void verifyCalledFor(ProcessRequest expected) {
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("id", expected.id());
        fields.put("destination", expected.destination());
        fields.put("payload", expected.payload());
        client.verify(request().withMethod("POST").withPath(PATH)
                .withHeader("Content-Type", ".*application/json.*")
                .withBody(json(toJson(fields), MatchType.ONLY_MATCHING_FIELDS)));
    }

    /** Total POST /process count, for AssertJ ranges. */
    public int callCount() {
        return client.retrieveRecordedRequests(request().withMethod("POST").withPath(PATH)).length;
    }
}
