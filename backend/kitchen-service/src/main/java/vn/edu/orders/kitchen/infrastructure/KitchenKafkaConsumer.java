package vn.edu.orders.kitchen.infrastructure;

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
import vn.edu.orders.contracts.events.CancelRequestedPayload;
import vn.edu.orders.contracts.events.EventEnvelope;
import vn.edu.orders.contracts.events.EventTopics;
import vn.edu.orders.contracts.events.EventType;
import vn.edu.orders.contracts.events.IngredientCheckChangedPayload;
import vn.edu.orders.contracts.events.InvalidEventException;
import vn.edu.orders.contracts.events.OrderFailedPayload;
import vn.edu.orders.contracts.events.OrderReservedPayload;
import vn.edu.orders.kitchen.application.EventProcessingContext;
import vn.edu.orders.kitchen.application.KitchenOrderService;
import vn.edu.orders.kitchen.config.KitchenProperties;

@Component
public class KitchenKafkaConsumer {
    public static final String KITCHEN_ORDERS_LISTENER_ID = "kitchen-orders-listener";
    public static final String CANCEL_EVENTS_LISTENER_ID = "kitchen-cancel-events-listener";
    public static final String MENU_EVENTS_LISTENER_ID = "kitchen-menu-events-listener";

    private static final Logger log = LoggerFactory.getLogger(KitchenKafkaConsumer.class);

    private final ObjectMapper json;
    private final KitchenOrderService orderService;
    private final DlqPublisher dlqPublisher;
    private final String instanceId;

    public KitchenKafkaConsumer(
            ObjectMapper json,
            KitchenOrderService orderService,
            DlqPublisher dlqPublisher,
            KitchenProperties properties
    ) {
        this.json = json;
        this.orderService = orderService;
        this.dlqPublisher = dlqPublisher;
        this.instanceId = properties.instanceId();
    }

    @KafkaListener(
            id = KITCHEN_ORDERS_LISTENER_ID,
            idIsGroup = false,
            groupId = "${spring.kafka.consumer.group-id}",
            topics = EventTopics.KITCHEN_ORDERS,
            concurrency = "1")
    public void onKitchenOrdersMessage(ConsumerRecord<String, String> record, Acknowledgment ack) {
        ParsedHeader header = parseAndCheckEnvelopeHeader(record, ack);
        if (header == null) {
            return;
        }
        EventProcessingContext context = new EventProcessingContext(record.topic(), record.partition(), record.offset());
        try {
            switch (header.eventType()) {
                case OrderReserved -> {
                    EventEnvelope<OrderReservedPayload> envelope = deserializeEnvelope(record.value(), OrderReservedPayload.class);
                    orderService.processOrderReserved(envelope, context);
                }
                case OrderFailed -> {
                    EventEnvelope<OrderFailedPayload> envelope = deserializeEnvelope(record.value(), OrderFailedPayload.class);
                    orderService.processOrderFailed(envelope, context);
                }
                default -> throw new InvalidEventException(
                        "Unexpected eventType on kitchen-orders: " + header.eventType(),
                        header.eventId(),
                        header.orderId());
            }
            ack.acknowledge();
        } catch (JacksonException | InvalidEventException ex) {
            dlqPublisher.sendToDlqAndRecordAlert(
                    record, ex.getMessage(), ex, header.eventId(), header.orderId(), header.eventType().name());
            ack.acknowledge();
        }
    }

    @KafkaListener(
            id = CANCEL_EVENTS_LISTENER_ID,
            idIsGroup = false,
            groupId = "${spring.kafka.consumer.group-id}",
            topicPattern = EventTopics.CANCEL_EVENTS_PATTERN,
            concurrency = "1")
    public void onCancelEventsMessage(ConsumerRecord<String, String> record, Acknowledgment ack) {
        ParsedHeader header = parseAndCheckEnvelopeHeader(record, ack);
        if (header == null) {
            return;
        }
        EventProcessingContext context = new EventProcessingContext(record.topic(), record.partition(), record.offset());
        try {
            if (header.eventType() != EventType.CancelRequested) {
                throw new InvalidEventException(
                        "Unexpected eventType on cancel-events topic: " + header.eventType(),
                        header.eventId(),
                        header.orderId());
            }
            EventEnvelope<CancelRequestedPayload> envelope = deserializeEnvelope(record.value(), CancelRequestedPayload.class);
            orderService.processCancelRequested(record.topic(), envelope, context);
            ack.acknowledge();
        } catch (JacksonException | InvalidEventException ex) {
            dlqPublisher.sendToDlqAndRecordAlert(
                    record, ex.getMessage(), ex, header.eventId(), header.orderId(), header.eventType().name());
            ack.acknowledge();
        }
    }

    @KafkaListener(
            id = MENU_EVENTS_LISTENER_ID,
            idIsGroup = false,
            groupId = "${spring.kafka.consumer.group-id}",
            topics = EventTopics.MENU_EVENTS,
            concurrency = "1")
    public void onMenuEventsMessage(ConsumerRecord<String, String> record, Acknowledgment ack) {
        ParsedHeader header = parseAndCheckEnvelopeHeader(record, ack);
        if (header == null) {
            return;
        }
        EventProcessingContext context = new EventProcessingContext(record.topic(), record.partition(), record.offset());
        try {
            switch (header.eventType()) {
                case DishStatusChanged, Heartbeat -> {
                    log.debug("Kitchen gracefully ignoring menu event instanceId={} eventType={} eventId={} topic={} partition={} offset={}",
                            instanceId, header.eventType(), header.eventId(), record.topic(), record.partition(), record.offset());
                    ack.acknowledge();
                }
                case IngredientCheckChanged -> {
                    EventEnvelope<IngredientCheckChangedPayload> envelope =
                            deserializeEnvelope(record.value(), IngredientCheckChangedPayload.class);
                    orderService.processIngredientCheckChanged(envelope, header.root().get("payload"), context);
                    ack.acknowledge();
                }
                default -> throw new InvalidEventException(
                        "Unexpected eventType on menu-events: " + header.eventType(),
                        header.eventId(),
                        header.orderId());
            }
        } catch (JacksonException | InvalidEventException ex) {
            dlqPublisher.sendToDlqAndRecordAlert(
                    record, ex.getMessage(), ex, header.eventId(), header.orderId(), header.eventType().name());
            ack.acknowledge();
        }
    }

    private ParsedHeader parseAndCheckEnvelopeHeader(ConsumerRecord<String, String> record, Acknowledgment ack) {
        String raw = record.value();
        if (raw == null || raw.isBlank()) {
            dlqPublisher.sendToDlqAndRecordAlert(record, "Empty event payload", null, null, null, null);
            ack.acknowledge();
            return null;
        }

        JsonNode root;
        try {
            root = json.readTree(raw);
        } catch (JacksonException ex) {
            dlqPublisher.sendToDlqAndRecordAlert(
                    record, "Malformed JSON payload: " + ex.getOriginalMessage(), ex, null, null, null);
            ack.acknowledge();
            return null;
        }

        UUID readableEventId = tryReadUuid(root, "eventId");
        UUID readableOrderId = tryReadUuid(root, "orderId");
        JsonNode eventTypeNode = root.get("eventType");
        String eventTypeText = (eventTypeNode != null && eventTypeNode.isString()) ? eventTypeNode.asString() : null;

        if (eventTypeText == null || eventTypeText.isBlank()) {
            dlqPublisher.sendToDlqAndRecordAlert(
                    record, "Missing or invalid eventType", null, readableEventId, readableOrderId, null);
            ack.acknowledge();
            return null;
        }

        JsonNode schemaNode = root.get("schemaVersion");
        if (schemaNode == null || !schemaNode.isInt() || schemaNode.asInt() != EventTopics.SUPPORTED_SCHEMA_VERSION) {
            String msg = "Unsupported schemaVersion: " + (schemaNode == null ? "missing" : schemaNode.toString());
            dlqPublisher.sendToDlqAndRecordAlert(
                    record, msg, null, readableEventId, readableOrderId, eventTypeText);
            ack.acknowledge();
            return null;
        }

        Optional<EventType> parsedType = EventType.fromValue(eventTypeText);
        if (parsedType.isEmpty()) {
            dlqPublisher.sendToDlqAndRecordAlert(
                    record, "Unknown eventType: " + eventTypeText, null, readableEventId, readableOrderId, eventTypeText);
            ack.acknowledge();
            return null;
        }

        return new ParsedHeader(root, readableEventId, readableOrderId, parsedType.get());
    }

    private <T> EventEnvelope<T> deserializeEnvelope(String raw, Class<T> payloadClass) {
        JavaType type = json.getTypeFactory().constructParametricType(EventEnvelope.class, payloadClass);
        return json.readValue(raw, type);
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

    private record ParsedHeader(JsonNode root, UUID eventId, UUID orderId, EventType eventType) {
    }
}
