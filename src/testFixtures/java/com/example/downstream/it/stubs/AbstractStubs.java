package com.example.downstream.it.stubs;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;

/**
 * Shared base for per-service stubs holders. Owns the {@link WireMockServer} + {@link ObjectMapper}
 * collaborators and exposes JSON-body / response-builder helpers used by every concrete stubs class.
 */
public abstract class AbstractStubs {

    protected final WireMockServer wiremock;
    protected final ObjectMapper mapper;

    protected AbstractStubs(WireMockServer wiremock, ObjectMapper mapper) {
        this.wiremock = wiremock;
        this.mapper = mapper;
    }

    /** Serializes the fixture into a JSON string suitable for {@code .withBody(...)}. */
    protected String toJson(Object body) {
        try {
            return mapper.writeValueAsString(body);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("failed to serialize stub body " + body, e);
        }
    }

    /** 200 + application/json + the serialized fixture. */
    protected ResponseDefinitionBuilder okJson(Object body) {
        return aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBody(toJson(body));
    }
}
