package com.example.downstream.client;

import com.example.match.MatchClient;
import com.example.match.MatchRequest;
import com.example.match.MatchResponse;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;

public class MatchClientCircuitBreaker implements MatchClient {

    static final String CB_NAME = "match";

    private final MatchClient delegate;

    public MatchClientCircuitBreaker(MatchClient delegate) {
        this.delegate = delegate;
    }

    @Override
    @CircuitBreaker(name = CB_NAME)
    public MatchResponse match(MatchRequest request) {
        return delegate.match(request);
    }
}
