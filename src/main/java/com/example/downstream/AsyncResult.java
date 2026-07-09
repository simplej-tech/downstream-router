package com.example.downstream;

import com.example.downstream.model.DownstreamResult;

/**
 * Internal carrier for one record's async processing outcome — NOT a wire type. Produced by
 * {@link AsyncProcessor#processRecord}, aggregated per batch by {@link StandardDownstreamListener},
 * and routed by {@link #exception()}: null → results topic, non-null → dead-letter topic.
 *
 * <p>The future returned by the processor never completes exceptionally — a downstream failure is
 * caught and carried here as {@code exception} alongside an {@link DownstreamResult#OUTCOME_ERROR}
 * result — so {@code CompletableFuture.join()} during aggregation is always safe.
 *
 * @param key       the message id (Kafka record key for the produced result)
 * @param result    the aggregated result (OK on success, error-outcome on failure)
 * @param exception the downstream failure, or null on success
 */
public record AsyncResult(String key, DownstreamResult result, Throwable exception) {}
