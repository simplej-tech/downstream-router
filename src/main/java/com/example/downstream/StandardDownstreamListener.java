package com.example.downstream;

import com.example.downstream.model.RequestMessage;
import com.example.kafka.utils.Result;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * Consumes the standard-downstream topic, decrypts, parses, and hands each message off to
 * {@link AsyncProcessor#processRecord(RequestMessage)} for the downstream client calls. The listener
 * itself only owns the consumer-thread work (decrypt + parse + dispatch); the slow HTTP path runs on
 * the async executor.
 *
 * <p>{@code @Transactional("kafkaTransactionManager")} ties the offset commit to the Kafka producer
 * transaction. With the async dispatch this is at-most-once for downstream calls — see
 * {@link AsyncProcessor} for the tradeoff.
 */
@Component
public class StandardDownstreamListener {

    private static final Logger log = LoggerFactory.getLogger(StandardDownstreamListener.class);

    private final ObjectMapper mapper;
    private final AsyncProcessor processor;

    public StandardDownstreamListener(ObjectMapper mapper, AsyncProcessor processor) {
        this.mapper = mapper;
        this.processor = processor;
    }

    @KafkaListener(id = "standard-downstream-processor", topics = "${app.topics.standard-downstream}")
    @Transactional("kafkaTransactionManager")
    public void onBatch(List<ConsumerRecord<String, Result<byte[], Pair<Exception, byte[]>>>> records) throws Exception {
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
            processor.processRecord(message);
        }
    }
}
