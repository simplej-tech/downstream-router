package com.example.downstream.it.mockserver;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.mockserver.client.MockServerClient;
import org.mockserver.model.HttpResponse;

import static org.mockserver.model.HttpResponse.response;

/**
 * MockServer parallel of {@link com.example.downstream.it.stubs.AbstractStubs}. Owns the
 * {@link MockServerClient} + {@link ObjectMapper} collaborators and exposes the JSON-body /
 * response-builder helpers used by every concrete stubs class.
 */
public abstract class MockAbstractStubs {

    protected final MockServerClient client;
    protected final ObjectMapper mapper;

    protected MockAbstractStubs(MockServerClient client, ObjectMapper mapper) {
        this.client = client;
        this.mapper = mapper;
    }

    /** Serializes the fixture into a JSON string suitable for a response body. */
    protected String toJson(Object body) {
        try {
            return mapper.writeValueAsString(body);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("failed to serialize stub body " + body, e);
        }
    }

    /** 200 + application/json + the serialized fixture. */
    protected HttpResponse okJson(Object body) {
        return response()
                .withStatusCode(200)
                .withHeader("Content-Type", "application/json")
                .withBody(toJson(body));
    }
}
