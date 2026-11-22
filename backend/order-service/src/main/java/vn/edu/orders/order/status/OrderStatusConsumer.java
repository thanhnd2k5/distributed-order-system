package vn.edu.orders.order.status;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JavaType;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import vn.edu.orders.contracts.events.CancelPendingPayload;
import vn.edu.orders.contracts.events.CancelRejectedPayload;
import vn.edu.orders.contracts.events.EventEnvelope;
import vn.edu.orders.contracts.events.EventTopics;
import vn.edu.orders.contracts.events.EventType;
import vn.edu.orders.contracts.events.InvalidEventException;
import vn.edu.orders.contracts.events.OrderCancelledPayload;
import vn.edu.orders.contracts.events.OrderCompletedPayload;
import vn.edu.orders.contracts.events.OrderFailedPayload;
import vn.edu.orders.contracts.events.OrderReservedPayload;
import vn.edu.orders.order.config.MutableClock;
import vn.edu.orders.order.config.SiteProperties;

/**
 * Consumes per-branch {@code status-events.<branch>} messages with manual offset acknowledgement
 * only after DB commit (01 §13.3). Display-side unknown {@code eventType} or higher
 * {@code schemaVersion} are logged, alerted, ignored, and acknowledged per 01 §9.
 */
@Component
public class OrderStatusConsumer {
    private static final Logger log = LoggerFactory.getLogger(OrderStatusConsumer.class);
    public static final String LISTENER_ID = "order-status-consumer";

    private final OrderStatusSyncService syncService;
    private final OrderStatusStore statusStore;
    private final SiteProperties siteProperties;
    private final MutableClock clock;
    private final ObjectMapper mapper;

    public OrderStatusConsumer(
            OrderStatusSyncService syncService,
            OrderStatusStore statusStore,
            SiteProperties siteProperties,
            MutableClock clock,
            ObjectMapper mapper
    ) {
        this.syncService = syncService;
        this.statusStore = statusStore;
        this.siteProperties = siteProperties;
        this.clock = clock;
        this.mapper = mapper;
    }

    @KafkaListener(
            id = LISTENER_ID,
            topics = "#{@orderStatusTopic}",
            groupId = "#{@orderStatusGroupId}"
    )
    public void onMessage(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        String raw = record.value();
        if (raw == null || raw.isBlank()) {
            throw new InvalidEventException("Empty event payload in " + record.topic());
        }

        JsonNode root;
        try {
            root = mapper.readTree(raw);
        } catch (JacksonException e) {
            log.error("Unreadable status event (malformed JSON) service=order-service siteId={} topic={} partition={} offset={}",
                    siteProperties.id(), record.topic(), record.partition(), record.offset(), e);
            throw new InvalidEventException("Malformed JSON record in " + record.topic(), e);
        }

        if (root == null || !root.isObject()) {
            throw new InvalidEventException("Status event is not a JSON object");
        }

        UUID eventId = tryReadUuid(root, "eventId");
        UUID orderId = tryReadUuid(root, "orderId");

        JsonNode schemaNode = root.get("schemaVersion");
        if (schemaNode == null || !schemaNode.isInt()) {
            throw new InvalidEventException("Missing or invalid schemaVersion", eventId, orderId);
        }
        int schemaVersion = schemaNode.asInt();
        if (schemaVersion < 1) {
            throw new InvalidEventException("Invalid schemaVersion=" + schemaVersion, eventId, orderId);
        }
        if (schemaVersion > EventTopics.SUPPORTED_SCHEMA_VERSION) {
            recordUnsupportedAndIgnore(
                    "UNSUPPORTED_SCHEMA_VERSION",
                    "Unsupported schemaVersion=" + schemaVersion,
                    record,
                    eventId,
                    orderId
            );
            acknowledgment.acknowledge();
            return;
        }

        JsonNode eventTypeNode = root.get("eventType");
        if (eventTypeNode == null || !eventTypeNode.isString() || eventTypeNode.asString().isBlank()) {
            throw new InvalidEventException("Missing or blank eventType", eventId, orderId);
        }

        String rawEventType = eventTypeNode.asString();
        Optional<EventType> parsedType = EventType.fromValue(rawEventType);
        if (parsedType.isEmpty()) {
            recordUnsupportedAndIgnore(
                    "UNKNOWN_EVENT_TYPE",
                    "Unknown eventType=" + rawEventType,
                    record,
                    eventId,
                    orderId
            );
            acknowledgment.acknowledge();
            return;
        }

        StatusEventContext context = new StatusEventContext(record.partition(), record.offset());
        switch (parsedType.get()) {
            case OrderReserved -> {
                EventEnvelope<OrderReservedPayload> env = bind(root, OrderReservedPayload.class, eventId, orderId);
                syncService.processOrderReserved(env, context);
            }
            case OrderFailed -> {
                EventEnvelope<OrderFailedPayload> env = bind(root, OrderFailedPayload.class, eventId, orderId);
                syncService.processOrderFailed(env, context);
            }
            case OrderCancelled -> {
                EventEnvelope<OrderCancelledPayload> env = bind(root, OrderCancelledPayload.class, eventId, orderId);
                syncService.processOrderCancelled(env, context);
            }
            case OrderCompleted -> {
                EventEnvelope<OrderCompletedPayload> env = bind(root, OrderCompletedPayload.class, eventId, orderId);
                syncService.processOrderCompleted(env, context);
            }
            case CancelPending -> {
                EventEnvelope<CancelPendingPayload> env = bind(root, CancelPendingPayload.class, eventId, orderId);
                syncService.processCancelPending(env, context);
            }
            case CancelRejected -> {
                EventEnvelope<CancelRejectedPayload> env = bind(root, CancelRejectedPayload.class, eventId, orderId);
                syncService.processCancelRejected(env, context);
            }
            default -> recordUnsupportedAndIgnore(
                    "UNKNOWN_EVENT_TYPE",
                    "Unexpected eventType on status topic: " + rawEventType,
                    record,
                    eventId,
                    orderId
            );
        }
        acknowledgment.acknowledge();
    }

    private <P> EventEnvelope<P> bind(JsonNode root, Class<P> payloadType, UUID eventId, UUID orderId) {
        try {
            JavaType envelopeType = mapper.getTypeFactory()
                    .constructParametricType(EventEnvelope.class, payloadType);
            EventEnvelope<P> envelope = mapper.treeToValue(root, envelopeType);
            if (envelope == null) {
                throw new InvalidEventException("Deserialized null envelope", eventId, orderId);
            }
            return envelope;
        } catch (JacksonException e) {
            throw new InvalidEventException(
                    "Failed to bind " + payloadType.getSimpleName() + ": " + e.getMessage(),
                    eventId,
                    orderId,
                    e
            );
        }
    }

    private void recordUnsupportedAndIgnore(
            String alertKind,
            String detailMessage,
            ConsumerRecord<String, String> record,
            UUID eventId,
            UUID orderId
    ) {
        log.warn("Display-side ignored event service=order-service siteId={} kind={} topic={} partition={} offset={} orderId={} eventId={} reason={}",
                siteProperties.id(), alertKind, record.topic(), record.partition(), record.offset(),
                orderId, eventId, detailMessage);
        OffsetDateTime now = OffsetDateTime.ofInstant(clock.instant(), ZoneOffset.UTC);
        statusStore.insertAlert(
                alertKind,
                orderId,
                eventId,
                record.topic(),
                record.partition(),
                record.offset(),
                mapper.writeValueAsString(Map.of("message", detailMessage)),
                now
        );
    }

    private static UUID tryReadUuid(JsonNode root, String field) {
        JsonNode node = root.get(field);
        if (node == null || !node.isString()) {
            return null;
        }
        try {
            return UUID.fromString(node.asString());
        } catch (IllegalArgumentException ignored) {
            return null;
        }
    }
}
