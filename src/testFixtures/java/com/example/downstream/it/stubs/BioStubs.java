package com.example.downstream.it.stubs;

import com.example.bio.ProcessRequest;
import com.example.bio.ProcessResponse;
import com.example.downstream.it.fixtures.BioResponses;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;

/** Stubbing + verification for the bio downstream (low-level ES RestClient, POST /process). */
public class BioStubs extends AbstractStubs {

    public static final String PATH = "/process";

    public BioStubs(WireMockServer wiremock, ObjectMapper mapper) {
        super(wiremock, mapper);
    }

    // -- stubbing --------------------------------------------------------------------------------

    public void returnsHealthy() {
        returns(BioResponses.ok());
    }

    public void returns(ProcessResponse response) {
        wiremock.stubFor(post(urlEqualTo(PATH)).willReturn(okJson(response)));
    }

    public void failsWith(int status) {
        wiremock.stubFor(post(urlEqualTo(PATH)).willReturn(aResponse().withStatus(status)));
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
        wiremock.verify(times, postRequestedFor(urlEqualTo(PATH)));
    }

    public void verifyNotCalled() {
        verifyCalled(0);
    }

    /** Asserts ≥1 POST whose body matches the given {@link ProcessRequest} field-for-field. */
    public void verifyCalledFor(ProcessRequest expected) {
        wiremock.verify(postRequestedFor(urlEqualTo(PATH))
                .withHeader("Content-Type", containing("application/json"))
                .withRequestBody(matchingJsonPath("$.id", equalTo(expected.id())))
                .withRequestBody(matchingJsonPath("$.destination", equalTo(expected.destination())))
                .withRequestBody(matchingJsonPath("$.payload", equalTo(expected.payload()))));
    }

    /** Total POST /process count, for AssertJ ranges (e.g., isBetween(...)). */
    public int callCount() {
        return wiremock.findAll(postRequestedFor(urlEqualTo(PATH))).size();
    }
}
