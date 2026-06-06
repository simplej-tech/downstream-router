package com.example.downstream.client;

import com.example.social.SocialClient;
import com.example.social.SocialRequest;
import com.example.social.SocialResponse;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;

public class SocialClientCircuitBreaker implements SocialClient {

    static final String CB_NAME = "social";

    private final SocialClient delegate;

    public SocialClientCircuitBreaker(SocialClient delegate) {
        this.delegate = delegate;
    }

    @Override
    @CircuitBreaker(name = CB_NAME)
    public SocialResponse lookup(SocialRequest request) {
        return delegate.lookup(request);
    }
}
