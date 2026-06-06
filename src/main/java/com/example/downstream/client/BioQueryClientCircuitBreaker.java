package com.example.downstream.client;

import com.example.bio.BioQueryClient;
import com.example.bio.ProcessRequest;
import com.example.bio.ProcessResponse;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;

public class BioQueryClientCircuitBreaker implements BioQueryClient {

    static final String CB_NAME = "bio";

    private final BioQueryClient delegate;

    public BioQueryClientCircuitBreaker(BioQueryClient delegate) {
        this.delegate = delegate;
    }

    @Override
    @CircuitBreaker(name = CB_NAME)
    public ProcessResponse process(ProcessRequest request) {
        return delegate.process(request);
    }
}
