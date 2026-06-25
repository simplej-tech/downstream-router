package com.example.downstream.it.mockserver;

import com.example.downstream.it.fixtures.SocialResponses;
import com.example.social.SocialRequest;
import com.example.social.SocialResponse;
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
 * MockServer parallel of {@link com.example.downstream.it.stubs.SocialStubs} — social downstream
 * (OpenAPI okhttp-gson, POST /social).
 */
public class MockSocialStubs extends MockAbstractStubs {

    public static final String PATH = "/social";

    public MockSocialStubs(MockServerClient client, ObjectMapper mapper) {
        super(client, mapper);
    }

    // -- stubbing --------------------------------------------------------------------------------

    public void returnsHealthy() {
        returns(SocialResponses.ok());
    }

    public void returns(SocialResponse response) {
        client.when(request().withMethod("POST").withPath(PATH)).respond(okJson(response));
    }

    public void failsWith(int status) {
        client.when(request().withMethod("POST").withPath(PATH))
                .respond(response().withStatusCode(status));
    }

    public void fails5xx() {
        failsWith(503);
    }

    public void fails4xx() {
        failsWith(400);
    }

    // -- verification ----------------------------------------------------------------------------

    public void verifyCalled(int times) {
        client.verify(request().withMethod("POST").withPath(PATH), VerificationTimes.exactly(times));
    }

    public void verifyNotCalled() {
        verifyCalled(0);
    }

    public void verifyCalledFor(SocialRequest expected) {
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("id", expected.id());
        fields.put("handle", expected.handle());
        client.verify(request().withMethod("POST").withPath(PATH)
                .withHeader("Content-Type", ".*application/json.*")
                .withBody(json(toJson(fields), MatchType.ONLY_MATCHING_FIELDS)));
    }

    public int callCount() {
        return client.retrieveRecordedRequests(request().withMethod("POST").withPath(PATH)).length;
    }
}
