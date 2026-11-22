package vn.edu.orders.order.status;

import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import vn.edu.orders.contracts.events.AlertKind;
import vn.edu.orders.contracts.events.DlqHeaders;
import vn.edu.orders.contracts.events.EventTopics;
import vn.edu.orders.order.config.MutableClock;
import vn.edu.orders.order.config.OrderLifecycleProperties;
import vn.edu.orders.order.config.SiteProperties;

/**
 * Sends permanently failing unreadable messages directly to {@code dlq} with metadata headers,
 * waits for Kafka acknowledgement, and persists a deduplicated {@code NEEDS_REVIEW} alert.
 */
@Component
public class DlqPublisher implements ConsumerRecordRecoverer {
    private static final Logger log = LoggerFactory.getLogger(DlqPublisher.class);

    private final KafkaTemplate<String, String> kafka;
    private final OrderStatusStore statusStore;
    private final ObjectMapper mapper;
    private final MutableClock clock;
    private final OrderLifecycleProperties properties;
    private final SiteProperties site;

    public DlqPublisher(
            KafkaTemplate<String, String> kafka,
            OrderStatusStore statusStore,
            ObjectMapper mapper,
            MutableClock clock,
            OrderLifecycleProperties properties,
            SiteProperties site
    ) {
        this.kafka = kafka;
        this.statusStore = statusStore;
        this.mapper = mapper;
        this.clock = clock;
        this.properties = properties;
        this.site = site;
    }

    @Override
    public void accept(ConsumerRecord<?, ?> record, Exception exception) {
        Throwable root = unwrap(exception);
        String topic = record.topic();
        int partition = record.partition();
        long offset = record.offset();
        String key = record.key() == null ? null : record.key().toString();
        String value = record.value() == null ? "" : record.value().toString();

        ProducerRecord<String, String> dlqRecord = new ProducerRecord<>(EventTopics.DLQ, key, value);
        addHeader(dlqRecord, DlqHeaders.ORIGINAL_TOPIC, topic);
        addHeader(dlqRecord, DlqHeaders.ORIGINAL_PARTITION, Integer.toString(partition));
        addHeader(dlqRecord, DlqHeaders.ORIGINAL_OFFSET, Long.toString(offset));
        addHeader(dlqRecord, DlqHeaders.CONSUMER_NAME, site.instanceId());
        addHeader(dlqRecord, DlqHeaders.ERROR, root.getMessage() != null ? root.getMessage() : root.toString());
        addHeader(dlqRecord, DlqHeaders.ERROR_CLASS, root.getClass().getName());
        addHeader(dlqRecord, DlqHeaders.FAILED_AT, clock.instant().toString());

        try {
            kafka.send(dlqRecord).get(properties.sendTimeoutMs(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while sending record to DLQ", ex);
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to publish record to DLQ", ex);
        }

        UUID eventId = tryExtractUuidFromJson(value, "eventId");
        UUID orderId = tryExtractUuidFromJson(value, "orderId");
        if (orderId == null) {
            orderId = tryParseUuid(key);
        }

        Map<String, Object> detail = new LinkedHashMap<>();
        detail.put("errorClass", root.getClass().getName());
        detail.put("error", root.getMessage() != null ? root.getMessage() : root.toString());
        detail.put("consumerName", site.instanceId());
        detail.put("rawKey", key);

        OffsetDateTime now = OffsetDateTime.ofInstant(clock.instant(), ZoneOffset.UTC);
        statusStore.insertAlert(
                AlertKind.NEEDS_REVIEW.name(),
                orderId,
                eventId,
                topic,
                partition,
                offset,
                mapper.writeValueAsString(detail),
                now
        );

        log.error("Moved unreadable message to dlq service=order-service siteId={} topic={} partition={} offset={} orderId={} eventId={} error={}",
                site.id(), topic, partition, offset, orderId, eventId, root.getMessage());
    }

    private static void addHeader(ProducerRecord<String, String> record, String key, String value) {
        if (value != null) {
            record.headers().add(key, value.getBytes(StandardCharsets.UTF_8));
        }
    }

    private UUID tryExtractUuidFromJson(String raw, String field) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        try {
            JsonNode tree = mapper.readTree(raw);
            if (tree != null && tree.isObject()) {
                JsonNode node = tree.get(field);
                if (node != null && node.isString()) {
                    return tryParseUuid(node.asString());
                }
            }
        } catch (Exception ignored) {
            // Raw payload may be non-JSON; key fallback handles orderId.
        }
        return null;
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

    private static Throwable unwrap(Throwable ex) {
        Throwable curr = ex;
        while (curr.getCause() != null && curr.getCause() != curr
                && "org.springframework.kafka.listener.ListenerExecutionFailedException".equals(curr.getClass().getName())) {
            curr = curr.getCause();
        }
        return curr;
    }
}
