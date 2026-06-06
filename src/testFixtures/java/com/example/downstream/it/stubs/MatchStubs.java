package com.example.downstream.it.stubs;

import com.example.downstream.it.fixtures.MatchResponses;
import com.example.match.MatchRequest;
import com.example.match.MatchResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;

/** Stubbing + verification for the match downstream (OpenAPI okhttp-gson, POST /match). */
public class MatchStubs extends AbstractStubs {

    public static final String PATH = "/match";

    public MatchStubs(WireMockServer wiremock, ObjectMapper mapper) {
        super(wiremock, mapper);
    }

    // -- stubbing --------------------------------------------------------------------------------

    public void returnsHealthy() {
        returns(MatchResponses.ok());
    }

    public void returns(MatchResponse response) {
        wiremock.stubFor(post(urlEqualTo(PATH)).willReturn(okJson(response)));
    }

    public void failsWith(int status) {
        wiremock.stubFor(post(urlEqualTo(PATH)).willReturn(aResponse().withStatus(status)));
    }

    public void fails5xx() {
        failsWith(503);
    }

    public void fails4xx() {
        failsWith(400);
    }

    // -- verification ----------------------------------------------------------------------------

    public void verifyCalled(int times) {
        wiremock.verify(times, postRequestedFor(urlEqualTo(PATH)));
    }

    public void verifyNotCalled() {
        verifyCalled(0);
    }

    public void verifyCalledFor(MatchRequest expected) {
        wiremock.verify(postRequestedFor(urlEqualTo(PATH))
                .withHeader("Content-Type", containing("application/json"))
                .withRequestBody(matchingJsonPath("$.id", equalTo(expected.id())))
                .withRequestBody(matchingJsonPath("$.query", equalTo(expected.query()))));
    }

    public int callCount() {
        return wiremock.findAll(postRequestedFor(urlEqualTo(PATH))).size();
    }
}
