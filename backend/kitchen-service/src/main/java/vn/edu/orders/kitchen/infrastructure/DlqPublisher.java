package vn.edu.orders.kitchen.infrastructure;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.ObjectMapper;
import vn.edu.orders.contracts.events.AlertKind;
import vn.edu.orders.contracts.events.DlqHeaders;
import vn.edu.orders.contracts.events.EventTopics;
import vn.edu.orders.kitchen.config.KitchenProperties;

@Component
public class DlqPublisher {
    private static final Logger log = LoggerFactory.getLogger(DlqPublisher.class);

    private final KafkaTemplate<String, String> kafka;
    private final KitchenOrderStore store;
    private final TransactionTemplate tx;
    private final ObjectMapper json;
    private final Clock clock;
    private final String instanceId;

    public DlqPublisher(
            KafkaTemplate<String, String> kafka,
            KitchenOrderStore store,
            PlatformTransactionManager txManager,
            ObjectMapper json,
            Clock clock,
            KitchenProperties properties
    ) {
        this.kafka = kafka;
        this.store = store;
        this.tx = new TransactionTemplate(txManager);
        this.json = json;
        this.clock = clock;
        this.instanceId = properties.instanceId();
    }

    public void sendToDlqAndRecordAlert(
            ConsumerRecord<String, String> record,
            String errorMessage,
            Throwable cause,
            UUID readableEventId,
            UUID readableOrderId,
            String readableEventType
    ) {
        UUID resolvedOrderId = readableOrderId != null ? readableOrderId : tryParseUuid(record.key());
        Instant nowInstant = clock.instant();
        String safeError = errorMessage != null ? errorMessage : "Unprocessable message";
        String errorClass = cause != null ? cause.getClass().getName() : "InvalidEvent";

        ProducerRecord<String, String> dlqRecord = new ProducerRecord<>(
                EventTopics.DLQ,
                record.key(),
                record.value());
        dlqRecord.headers().add(DlqHeaders.ORIGINAL_TOPIC, bytes(record.topic()));
        dlqRecord.headers().add(DlqHeaders.ORIGINAL_PARTITION, bytes(Integer.toString(record.partition())));
        dlqRecord.headers().add(DlqHeaders.ORIGINAL_OFFSET, bytes(Long.toString(record.offset())));
        dlqRecord.headers().add(DlqHeaders.CONSUMER_NAME, bytes(instanceId));
        dlqRecord.headers().add(DlqHeaders.ERROR, bytes(safeError));
        dlqRecord.headers().add(DlqHeaders.ERROR_CLASS, bytes(errorClass));
        dlqRecord.headers().add(DlqHeaders.FAILED_AT, bytes(nowInstant.toString()));

        try {
            kafka.send(dlqRecord).get(5, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while sending message to dlq", ex);
        } catch (ExecutionException | TimeoutException | KafkaException
                 | org.apache.kafka.common.KafkaException ex) {
            throw new IllegalStateException("Failed to publish bad message to dlq from topic=" + record.topic(), ex);
        }

        Map<String, Object> detail = new LinkedHashMap<>();
        detail.put("error", safeError);
        detail.put("errorClass", errorClass);
        if (readableEventType != null) {
            detail.put("eventType", readableEventType);
        }
        if (record.key() != null) {
            detail.put("kafkaKey", record.key());
        }
        String detailJson = json.writeValueAsString(detail);
        OffsetDateTime now = OffsetDateTime.ofInstant(nowInstant, ZoneOffset.UTC);

        tx.executeWithoutResult(status -> store.insertAlert(
                AlertKind.NEEDS_REVIEW.name(),
                resolvedOrderId,
                readableEventId,
                record.topic(),
                record.partition(),
                record.offset(),
                detailJson,
                now));

        log.error("Moved message to dlq and raised NEEDS_REVIEW alert instanceId={} orderId={} eventId={} topic={} partition={} offset={} error={}",
                instanceId, resolvedOrderId, readableEventId, record.topic(), record.partition(), record.offset(), safeError);
    }

    private static byte[] bytes(String value) {
        return value == null ? new byte[0] : value.getBytes(StandardCharsets.UTF_8);
    }

    private static UUID tryParseUuid(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        try {
            return UUID.fromString(raw.trim());
        } catch (IllegalArgumentException ignored) {
            return null;
        }
    }
}
