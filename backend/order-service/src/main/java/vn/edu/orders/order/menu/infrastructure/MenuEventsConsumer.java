package vn.edu.orders.order.menu.infrastructure;

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
import vn.edu.orders.contracts.events.DishStatusChangedPayload;
import vn.edu.orders.contracts.events.EventEnvelope;
import vn.edu.orders.contracts.events.EventTopics;
import vn.edu.orders.contracts.events.EventType;
import vn.edu.orders.contracts.events.HeartbeatPayload;
import vn.edu.orders.contracts.events.InvalidEventException;
import vn.edu.orders.order.config.MutableClock;
import vn.edu.orders.order.config.SiteProperties;
import vn.edu.orders.order.menu.application.MenuService;
import vn.edu.orders.order.status.OrderStatusStore;

/**
 * Consumes {@code menu-events} for Flow C (BR10).
 * {@code IngredientCheckChanged} is meant for Kitchen only and is silently ignored
 * without writing {@code processed_events} or routing to {@code dlq} (02 §2).
 */
@Component
public class MenuEventsConsumer {
    private static final Logger log = LoggerFactory.getLogger(MenuEventsConsumer.class);
    public static final String LISTENER_ID = "order-menu-consumer";

    private final MenuService menuService;
    private final OrderStatusStore alertStore;
    private final SiteProperties siteProperties;
    private final MutableClock clock;
    private final ObjectMapper mapper;

    public MenuEventsConsumer(
            MenuService menuService,
            OrderStatusStore alertStore,
            SiteProperties siteProperties,
            MutableClock clock,
            ObjectMapper mapper
    ) {
        this.menuService = menuService;
        this.alertStore = alertStore;
        this.siteProperties = siteProperties;
        this.clock = clock;
        this.mapper = mapper;
    }

    @KafkaListener(
            id = LISTENER_ID,
            topics = EventTopics.MENU_EVENTS,
            groupId = "#{@orderMenuGroupId}"
    )
    public void onMessage(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        String raw = record.value();
        if (raw == null || raw.isBlank()) {
            throw new InvalidEventException("Empty menu event payload in " + record.topic());
        }

        JsonNode root;
        try {
            root = mapper.readTree(raw);
        } catch (JacksonException e) {
            throw new InvalidEventException("Malformed JSON in " + record.topic(), e);
        }

        if (root == null || !root.isObject()) {
            throw new InvalidEventException("Menu event is not a JSON object");
        }

        UUID eventId = tryReadUuid(root, "eventId");
        JsonNode schemaNode = root.get("schemaVersion");
        if (schemaNode == null || !schemaNode.isInt()) {
            throw new InvalidEventException("Missing or invalid schemaVersion", eventId, null);
        }
        int schemaVersion = schemaNode.asInt();
        if (schemaVersion < 1) {
            throw new InvalidEventException("Invalid schemaVersion=" + schemaVersion, eventId, null);
        }
        if (schemaVersion > EventTopics.SUPPORTED_SCHEMA_VERSION) {
            recordUnsupportedAndIgnore(
                    "UNSUPPORTED_SCHEMA_VERSION",
                    "Unsupported schemaVersion=" + schemaVersion + " on menu-events",
                    record,
                    eventId
            );
            acknowledgment.acknowledge();
            return;
        }

        JsonNode eventTypeNode = root.get("eventType");
        if (eventTypeNode == null || !eventTypeNode.isString() || eventTypeNode.asString().isBlank()) {
            throw new InvalidEventException("Missing or blank eventType", eventId, null);
        }

        String rawEventType = eventTypeNode.asString();
        Optional<EventType> parsedType = EventType.fromValue(rawEventType);
        if (parsedType.isEmpty()) {
            recordUnsupportedAndIgnore(
                    "UNKNOWN_EVENT_TYPE",
                    "Unknown eventType=" + rawEventType + " on menu-events",
                    record,
                    eventId
            );
            acknowledgment.acknowledge();
            return;
        }

        switch (parsedType.get()) {
            case DishStatusChanged -> {
                EventEnvelope<DishStatusChangedPayload> env = bind(root, DishStatusChangedPayload.class, eventId);
                menuService.onDishStatusChanged(env);
            }
            case Heartbeat -> {
                EventEnvelope<HeartbeatPayload> env = bind(root, HeartbeatPayload.class, eventId);
                menuService.onHeartbeat(env);
            }
            case IngredientCheckChanged -> {
                // Known menu-events type meant for Kitchen only (02 §2): ignore without processed_events or dlq.
            }
            default -> recordUnsupportedAndIgnore(
                    "UNKNOWN_EVENT_TYPE",
                    "Unexpected eventType on menu-events: " + rawEventType,
                    record,
                    eventId
            );
        }
        acknowledgment.acknowledge();
    }

    private <P> EventEnvelope<P> bind(JsonNode root, Class<P> payloadType, UUID eventId) {
        try {
            JavaType envelopeType = mapper.getTypeFactory()
                    .constructParametricType(EventEnvelope.class, payloadType);
            EventEnvelope<P> envelope = mapper.treeToValue(root, envelopeType);
            if (envelope == null) {
                throw new InvalidEventException("Deserialized null menu envelope", eventId, null);
            }
            return envelope;
        } catch (JacksonException e) {
            throw new InvalidEventException(
                    "Failed to bind " + payloadType.getSimpleName() + ": " + e.getMessage(),
                    eventId,
                    null,
                    e
            );
        }
    }

    private void recordUnsupportedAndIgnore(
            String alertKind,
            String detailMessage,
            ConsumerRecord<String, String> record,
            UUID eventId
    ) {
        log.warn("Display-side ignored menu event service=order-service siteId={} kind={} topic={} partition={} offset={} eventId={} reason={}",
                siteProperties.id(), alertKind, record.topic(), record.partition(), record.offset(),
                eventId, detailMessage);
        OffsetDateTime now = OffsetDateTime.ofInstant(clock.instant(), ZoneOffset.UTC);
        alertStore.insertAlert(
                alertKind,
                null,
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
