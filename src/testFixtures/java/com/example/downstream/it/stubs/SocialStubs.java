package com.example.downstream.it.stubs;

import com.example.downstream.it.fixtures.SocialResponses;
import com.example.social.SocialRequest;
import com.example.social.SocialResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;

/** Stubbing + verification for the social downstream (OpenAPI okhttp-gson, POST /social). */
public class SocialStubs extends AbstractStubs {

    public static final String PATH = "/social";

    public SocialStubs(WireMockServer wiremock, ObjectMapper mapper) {
        super(wiremock, mapper);
    }

    // -- stubbing --------------------------------------------------------------------------------

    public void returnsHealthy() {
        returns(SocialResponses.ok());
    }

    public void returns(SocialResponse response) {
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

    public void verifyCalledFor(SocialRequest expected) {
        wiremock.verify(postRequestedFor(urlEqualTo(PATH))
                .withHeader("Content-Type", containing("application/json"))
                .withRequestBody(matchingJsonPath("$.id", equalTo(expected.id())))
                .withRequestBody(matchingJsonPath("$.handle", equalTo(expected.handle()))));
    }

    public int callCount() {
        return wiremock.findAll(postRequestedFor(urlEqualTo(PATH))).size();
    }
}
