package com.example.downstream.backpressure;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class CircuitBreakerMetaDataRegistry {

    private final Map<String, CircuitBreakerMetaData> byName = new LinkedHashMap<>();

    public CircuitBreakerMetaDataRegistry(List<CircuitBreakerMetaData> circuits) {
        for (CircuitBreakerMetaData circuit : circuits) {
            byName.put(circuit.name(), circuit);
        }
    }

    public CircuitBreakerMetaData byName(String name) {
        return byName.get(name);
    }

    public Collection<CircuitBreakerMetaData> all() {
        return byName.values();
    }
}
