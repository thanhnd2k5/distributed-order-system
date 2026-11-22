package vn.edu.orders.inventory.infrastructure;

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
import vn.edu.orders.contracts.events.EventEnvelope;
import vn.edu.orders.contracts.events.EventTopics;
import vn.edu.orders.contracts.events.EventType;
import vn.edu.orders.contracts.events.InvalidEventException;
import vn.edu.orders.contracts.events.OrderCreatedPayload;
import vn.edu.orders.inventory.application.DemoFailpoint;
import vn.edu.orders.inventory.application.EventProcessingContext;
import vn.edu.orders.inventory.application.InventoryOrderService;
import vn.edu.orders.inventory.application.InventoryOrderService.OrderProcessingResult;
import vn.edu.orders.inventory.config.InventoryProperties;

@Component
public class OrderEventsConsumer {
    public static final String LISTENER_ID = "inventory-order-events";
    private static final Logger log = LoggerFactory.getLogger(OrderEventsConsumer.class);

    private final ObjectMapper json;
    private final JavaType orderCreatedEnvelopeType;
    private final InventoryOrderService orderService;
    private final DlqService dlqService;
    private final DemoFailpoint failpoint;
    private final InventoryProperties properties;

    public OrderEventsConsumer(
            ObjectMapper json,
            InventoryOrderService orderService,
            DlqService dlqService,
            DemoFailpoint failpoint,
            InventoryProperties properties
    ) {
        this.json = json;
        this.orderCreatedEnvelopeType = json.getTypeFactory()
                .constructParametricType(EventEnvelope.class, OrderCreatedPayload.class);
        this.orderService = orderService;
        this.dlqService = dlqService;
        this.failpoint = failpoint;
        this.properties = properties;
    }

    @KafkaListener(
            id = LISTENER_ID,
            idIsGroup = false,
            groupId = "${spring.kafka.consumer.group-id}",
            topicPattern = EventTopics.ORDER_EVENTS_PATTERN,
            concurrency = "1")
    public void onMessage(ConsumerRecord<String, String> record, Acknowledgment ack) {
        EventProcessingContext context = new EventProcessingContext(
                record.topic(), record.partition(), record.offset(), record.timestamp(), record.key());

        // Validate topic -> branch mapping and envelope; retry poison parsing up to maxPoisonAttempts before DLQ
        ParsedOrderMessage parsed = null;
        InvalidEventException poisonFailure = null;
        int attempts = Math.max(1, properties.maxPoisonAttempts());
        for (int attempt = 1; attempt <= attempts; attempt++) {
            try {
                parsed = parseOrderCreated(record);
                poisonFailure = null;
                break;
            } catch (InvalidEventException ex) {
                poisonFailure = ex;
                if (attempt < attempts && properties.poisonBackoffMs() > 0) {
                    sleepQuietly(properties.poisonBackoffMs());
                }
            }
        }

        if (poisonFailure != null) {
            dlqService.sendToDlqAndRecordAlert(
                    record,
                    poisonFailure.eventId(),
                    poisonFailure.orderId(),
                    poisonFailure.getMessage(),
                    poisonFailure.getClass().getSimpleName());
            ack.acknowledge();
            return;
        }

        OrderProcessingResult result;
        if (parsed.invalidOrderDataDetail() != null) {
            result = orderService.processInvalidOrderData(
                    parsed.eventId(), parsed.orderId(), parsed.invalidOrderDataDetail(), context);
        } else {
            result = orderService.processOrderCreated(parsed.envelope(), context);
        }

        failpoint.maybeHaltAfterCommit(result, record.partition(), record.offset());
        ack.acknowledge();
    }

    private ParsedOrderMessage parseOrderCreated(ConsumerRecord<String, String> record) {
        try {
            EventTopics.branchFromTopic(record.topic());
        } catch (IllegalArgumentException ex) {
            throw new InvalidEventException("Invalid order-events topic: " + record.topic(), null, null, ex);
        }

        String raw = record.value();
        if (raw == null || raw.isBlank()) {
            throw new InvalidEventException("Empty event payload");
        }

        JsonNode root;
        try {
            root = json.readTree(raw);
        } catch (JacksonException ex) {
            throw new InvalidEventException("Malformed JSON payload: " + ex.getOriginalMessage(), null, null, ex);
        }
        if (root == null || !root.isObject()) {
            throw new InvalidEventException("JSON payload must be an object");
        }

        UUID eventId = DlqService.tryReadUuid(root, "eventId");
        UUID orderId = DlqService.tryReadUuid(root, "orderId");

        if (eventId == null) {
            throw new InvalidEventException("Missing or invalid eventId", null, orderId);
        }

        JsonNode versionNode = root.get("schemaVersion");
        if (versionNode == null || !versionNode.isInt()) {
            throw new InvalidEventException("Missing or invalid schemaVersion", eventId, orderId);
        }
        int schemaVersion = versionNode.asInt();
        if (schemaVersion != EventTopics.SUPPORTED_SCHEMA_VERSION) {
            throw new InvalidEventException("Unsupported schemaVersion: " + schemaVersion, eventId, orderId);
        }

        JsonNode eventTypeNode = root.get("eventType");
        if (eventTypeNode == null || !eventTypeNode.isString() || eventTypeNode.asString().isBlank()) {
            throw new InvalidEventException("Missing or invalid eventType", eventId, orderId);
        }
        String eventTypeText = eventTypeNode.asString();
        var eventTypeOpt = EventType.fromValue(eventTypeText);
        if (eventTypeOpt.isEmpty() || eventTypeOpt.get() != EventType.OrderCreated) {
            throw new InvalidEventException(
                    "Unexpected or unknown eventType on order-events: " + eventTypeText, eventId, orderId);
        }

        if (orderId == null) {
            throw new InvalidEventException("Missing or invalid orderId", eventId, null);
        }

        if (record.key() != null && !orderId.toString().equals(record.key())) {
            log.warn("OrderCreated Kafka key does not match orderId instanceId={} topic={} partition={} offset={} eventId={} orderId={} kafkaKey={}",
                    properties.instanceId(), record.topic(), record.partition(), record.offset(),
                    eventId, orderId, record.key());
        }

        // Envelope is readable & supported OrderCreated; payload/field errors become FAILED(INVALID_ORDER_DATA), NOT dlq
        try {
            EventEnvelope<OrderCreatedPayload> envelope = json.readValue(raw, orderCreatedEnvelopeType);
            return new ParsedOrderMessage(eventId, orderId, envelope, null);
        } catch (JacksonException ex) {
            return new ParsedOrderMessage(eventId, orderId, null,
                    "Invalid OrderCreated order data: " + ex.getOriginalMessage());
        }
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    private record ParsedOrderMessage(
            UUID eventId,
            UUID orderId,
            EventEnvelope<OrderCreatedPayload> envelope,
            String invalidOrderDataDetail
    ) {
    }
}
