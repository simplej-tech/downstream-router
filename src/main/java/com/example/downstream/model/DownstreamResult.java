package com.example.downstream.model;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Aggregated outcome of the bio → match → social calls for one {@link RequestMessage}, published to
 * the results topic ({@code app.topics.results}). Inlined per repo — like {@link RequestMessage},
 * this is a wire contract, not a shared lib type.
 *
 * <p>{@code outcome} is {@link #OUTCOME_OK} when every invoked service succeeded, else
 * {@link #OUTCOME_ERROR} with {@code error} set to the failure message. Per-service fields hold
 * whatever completed before a failure aborted the chain; social fields are null when social is
 * disabled ({@code app.social.enabled=false}).
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record DownstreamResult(
        String id,
        String destination,
        String outcome,
        String bioStatus,
        String matchStatus,
        Double matchScore,
        String socialStatus,
        Long socialReach,
        String error,
        long processedAtEpochMs) {

    public static final String OUTCOME_OK = "ok";
    public static final String OUTCOME_ERROR = "error";
}
