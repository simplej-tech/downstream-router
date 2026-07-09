package com.example.downstream;

import com.example.downstream.model.RequestMessage;
import com.example.kafka.utils.Result;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Consumes the standard-downstream topic, decrypts + parses each record, fans the whole batch out to
 * {@link AsyncProcessor#processRecord(RequestMessage)} (bio → match → social) on the async executor,
 * awaits every result, then produces each to the results topic ({@code app.topics.results}) or the
 * dead-letter topic ({@code app.topics.dlt}) depending on whether processing failed.
 *
 * <p>Async fan-out + aggregation: each record yields a {@code CompletableFuture<AsyncResult>}; the
 * listener joins them all ({@code allOf(...).join()}) so the downstream calls across a batch run in
 * parallel, but the produces still happen on the listener thread inside
 * {@code @Transactional("kafkaTransactionManager")} — so the offset commit and all result/DLT
 * publishes are one atomic Kafka transaction (exactly-once). Downstream failures never fail the
 * future ({@link AsyncProcessor} carries them in {@link AsyncResult#exception()}), so a bad response
 * is routed to the DLT and still commits rather than rolling the batch back and replaying; only
 * infra/produce errors roll back and redeliver. Result values ride the same encrypting serializer as
 * every other message.
 */
@Component
public class StandardDownstreamListener {

    private static final Logger log = LoggerFactory.getLogger(StandardDownstreamListener.class);

    private final ObjectMapper mapper;
    private final AsyncProcessor processor;
    private final KafkaTemplate<String, byte[]> kafkaTemplate;
    private final String resultsTopic;
    private final String dltTopic;

    public StandardDownstreamListener(ObjectMapper mapper,
                                      AsyncProcessor processor,
                                      KafkaTemplate<String, byte[]> kafkaTemplate,
                                      @Value("${app.topics.results}") String resultsTopic,
                                      @Value("${app.topics.dlt}") String dltTopic) {
        this.mapper = mapper;
        this.processor = processor;
        this.kafkaTemplate = kafkaTemplate;
        this.resultsTopic = resultsTopic;
        this.dltTopic = dltTopic;
    }

    @KafkaListener(id = "standard-downstream-processor", topics = "${app.topics.standard-downstream}")
    @Transactional("kafkaTransactionManager")
    public void onBatch(List<ConsumerRecord<String, Result<byte[], Pair<Exception, byte[]>>>> records) throws Exception {
        // Fan out: one async processing future per decodable record.
        List<CompletableFuture<AsyncResult>> futures = new ArrayList<>();
        for (ConsumerRecord<String, Result<byte[], Pair<Exception, byte[]>>> record : records) {
            Result<byte[], Pair<Exception, byte[]>> result = record.value();
            if (result == null) {
                log.warn("STANDARD-DOWNSTREAM got null value for key={}", record.key());
                continue;
            }
            byte[] plaintext = result.contentOrNull();
            if (plaintext == null) {
                Pair<Exception, byte[]> err = result.exception();
                log.error("STANDARD-DOWNSTREAM decryption failed for key={}: {}", record.key(), err.getLeft().getMessage());
                continue;
            }
            RequestMessage message = mapper.readValue(plaintext, RequestMessage.class);
            futures.add(processor.processRecord(message));
        }

        // Aggregate: await the whole batch, then collect the per-record results.
        CompletableFuture<List<AsyncResult>> allResults =
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                        .thenApply(v -> futures.stream().map(CompletableFuture::join).collect(Collectors.toList()));
        List<AsyncResult> results = allResults.join();

        // Route: DLT on failure, results topic otherwise. All within the active transaction, so these
        // publishes commit atomically with the consumer offsets.
        for (AsyncResult res : results) {
            String topic = res.exception() != null ? dltTopic : resultsTopic;
            kafkaTemplate.send(topic, res.key(), mapper.writeValueAsBytes(res.result()));
        }
    }
}
