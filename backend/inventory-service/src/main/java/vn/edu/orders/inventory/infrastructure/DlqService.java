package vn.edu.orders.inventory.infrastructure;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
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
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import vn.edu.orders.contracts.events.AlertKind;
import vn.edu.orders.contracts.events.DlqHeaders;
import vn.edu.orders.contracts.events.EventTopics;
import vn.edu.orders.inventory.config.InventoryProperties;

@Service
public class DlqService {
    private static final Logger log = LoggerFactory.getLogger(DlqService.class);

    private final KafkaTemplate<String, String> kafka;
    private final InventoryStore store;
    private final TransactionTemplate tx;
    private final ObjectMapper json;
    private final Clock clock;
    private final String instanceId;

    public DlqService(
            KafkaTemplate<String, String> kafka,
            InventoryStore store,
            PlatformTransactionManager transactionManager,
            ObjectMapper json,
            Clock clock,
            InventoryProperties properties
    ) {
        this.kafka = kafka;
        this.store = store;
        this.tx = new TransactionTemplate(transactionManager);
        this.json = json;
        this.clock = clock;
        this.instanceId = properties.instanceId();
    }

    /**
     * Publishes a bad/unreadable/unknown message directly to {@code dlq} with metadata headers,
     * waits synchronously for Kafka ACK, and persists an idempotent {@code NEEDS_REVIEW} alert.
     */
    public void sendToDlqAndRecordAlert(
            ConsumerRecord<String, String> record,
            UUID readableEventId,
            UUID readableOrderId,
            String errorMessage,
            String errorClass
    ) {
        UUID resolvedOrderId = readableOrderId != null ? readableOrderId : tryParseUuid(record.key());
        String payload = record.value() == null ? "" : record.value();
        String failedAt = clock.instant().toString();

        ProducerRecord<String, String> dlqRecord =
                new ProducerRecord<>(EventTopics.DLQ, record.key(), payload);
        addHeader(dlqRecord, DlqHeaders.ORIGINAL_TOPIC, record.topic());
        addHeader(dlqRecord, DlqHeaders.ORIGINAL_PARTITION, Integer.toString(record.partition()));
        addHeader(dlqRecord, DlqHeaders.ORIGINAL_OFFSET, Long.toString(record.offset()));
        addHeader(dlqRecord, DlqHeaders.CONSUMER_NAME, instanceId);
        addHeader(dlqRecord, DlqHeaders.ERROR, errorMessage == null ? "Poison message" : errorMessage);
        addHeader(dlqRecord, DlqHeaders.ERROR_CLASS, errorClass == null ? "InvalidEvent" : errorClass);
        addHeader(dlqRecord, DlqHeaders.FAILED_AT, failedAt);

        try {
            kafka.send(dlqRecord).get(6, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new DlqPublishException("Interrupted sending poison message to DLQ", ex);
        } catch (ExecutionException | TimeoutException | RuntimeException ex) {
            Throwable cause = ex instanceof ExecutionException ? ex.getCause() : ex;
            throw new DlqPublishException("Failed publishing poison message to DLQ: " + cause.getMessage(), cause);
        }

        Map<String, Object> detail = new LinkedHashMap<>();
        detail.put("error", errorMessage);
        detail.put("errorClass", errorClass);
        detail.put("kafkaKey", record.key());
        detail.put("failedAt", failedAt);
        String detailJson = json.writeValueAsString(detail);

        OffsetDateTime now = OffsetDateTime.ofInstant(clock.instant(), ZoneOffset.UTC).truncatedTo(ChronoUnit.MICROS);
        tx.executeWithoutResult(status -> store.insertAlertIdempotent(
                AlertKind.NEEDS_REVIEW,
                resolvedOrderId,
                readableEventId,
                record.topic(),
                record.partition(),
                record.offset(),
                detailJson,
                now));

        log.error("Moved message to DLQ and recorded NEEDS_REVIEW alert instanceId={} orderId={} eventId={} sourceTopic={} partition={} offset={} error={}",
                instanceId, resolvedOrderId, readableEventId, record.topic(), record.partition(), record.offset(), errorMessage);
    }

    public static UUID tryReadUuid(JsonNode root, String field) {
        if (root == null) {
            return null;
        }
        JsonNode node = root.get(field);
        if (node == null || !node.isString()) {
            return null;
        }
        return tryParseUuid(node.asString());
    }

    public static UUID tryParseUuid(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        try {
            return UUID.fromString(raw.trim());
        } catch (IllegalArgumentException ignored) {
            return null;
        }
    }

    private static void addHeader(ProducerRecord<String, String> record, String name, String value) {
        if (value != null) {
            record.headers().add(name, value.getBytes(StandardCharsets.UTF_8));
        }
    }

    public static final class DlqPublishException extends RuntimeException {
        public DlqPublishException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
