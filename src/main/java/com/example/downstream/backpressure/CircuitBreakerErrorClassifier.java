package com.example.downstream.backpressure;

/**
 * Decides which downstream failures should open a breaker. A 4xx {@code ApiException} is a permanent
 * client error and must NOT trip the breaker; a 5xx {@code ApiException} or any non-{@code ApiException}
 * (network, timeout, deserialization, unexpected) should. The match and social clients generate
 * distinct {@code ApiException} types, so the shared helper checks both as it unwraps the cause chain.
 */
public class CircuitBreakerErrorClassifier {

    public Boolean shouldRecordForMatch(Throwable t) {
        return checkNonApiOr5xx(t);
    }

    public Boolean shouldRecordForSocial(Throwable t) {
        return checkNonApiOr5xx(t);
    }

    private Boolean checkNonApiOr5xx(Throwable t) {
        Throwable current = t;
        int guard = 0;
        while (current != null && guard++ < 20) {
            if (current instanceof com.example.match.invoker.ApiException matchApi) {
                return matchApi.getCode() >= 500;
            }
            if (current instanceof com.example.social.invoker.ApiException socialApi) {
                return socialApi.getCode() >= 500;
            }
            current = current.getCause();
        }
        return true;
    }
}
