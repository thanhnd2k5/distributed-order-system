package vn.edu.orders.order.status;

import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import vn.edu.orders.contracts.events.DoneEvent;
import vn.edu.orders.contracts.events.EventTopics;
import vn.edu.orders.contracts.events.FailedEvent;
import vn.edu.orders.contracts.events.ReservedEvent;
import vn.edu.orders.order.config.SiteProperties;

@Component
public class OrderStatusConsumer {
    private static final Logger log = LoggerFactory.getLogger(OrderStatusConsumer.class);
    public static final String LISTENER_ID = "order-status-consumer";

    private final OrderStatusSyncService syncService;
    private final SiteProperties siteProperties;
    private final ObjectMapper mapper;

    public OrderStatusConsumer(
            OrderStatusSyncService syncService,
            SiteProperties siteProperties,
            ObjectMapper mapper
    ) {
        this.syncService = syncService;
        this.siteProperties = siteProperties;
        this.mapper = mapper;
    }

    @KafkaListener(
            id = LISTENER_ID,
            topics = EventTopics.STATUS_EVENTS,
            groupId = "#{@orderStatusGroupId}"
    )
    public void onMessage(ConsumerRecord<String, String> record) {
        String raw = record.value();
        if (raw == null || raw.isBlank()) {
            poison(record, "Empty event payload", null, null);
        }

        JsonNode root;
        try {
            root = mapper.readTree(raw);
        } catch (JacksonException e) {
            log.error("Poison status event (malformed JSON) service=order-service siteId={} topic={} partition={} offset={}",
                    siteProperties.id(), record.topic(), record.partition(), record.offset(), e);
            throw new InvalidEventException("Malformed JSON record in " + record.topic(), e);
        }

        if (root == null || !root.isObject()) {
            poison(record, "Status event is not a JSON object", null, null);
        }

        UUID eventId = tryReadUuid(root, "eventId");
        UUID orderId = tryReadUuid(root, "orderId");

        String sourceSiteId = root.path("sourceSiteId").asText(null);
        if (sourceSiteId == null || !sourceSiteId.equals(siteProperties.id())) {
            log.info("Status event skipped for foreign site service=order-service siteId={} sourceSiteId={} topic={} partition={} offset={} eventId={} orderId={}",
                    siteProperties.id(), sourceSiteId, record.topic(), record.partition(), record.offset(), eventId, orderId);
            return;
        }

        JsonNode schemaNode = root.get("schemaVersion");
        if (schemaNode == null || !schemaNode.isInt() || schemaNode.asInt() != EventTopics.SCHEMA_VERSION_1) {
            String schemaText = schemaNode == null ? "missing" : schemaNode.toString();
            poison(record, "Unsupported schemaVersion=" + schemaText + " for siteId=" + siteProperties.id(), eventId, orderId);
        }

        JsonNode eventTypeNode = root.get("eventType");
        if (eventTypeNode == null || !eventTypeNode.isString() || eventTypeNode.asString().isBlank()) {
            poison(record, "Missing or invalid eventType", eventId, orderId);
        }

        String eventType = eventTypeNode.asString();
        StatusEventContext context = new StatusEventContext(record.partition(), record.offset());
        switch (eventType) {
            case "Reserved" -> {
                ReservedEvent event = bind(record, root, ReservedEvent.class, "ReservedEvent", eventId, orderId);
                apply(record, event.eventId(), event.orderId(), () -> syncService.processReserved(event, context));
            }
            case "Failed" -> {
                FailedEvent event = bind(record, root, FailedEvent.class, "FailedEvent", eventId, orderId);
                apply(record, event.eventId(), event.orderId(), () -> syncService.processFailed(event, context));
            }
            case "Done" -> {
                DoneEvent event = bind(record, root, DoneEvent.class, "DoneEvent", eventId, orderId);
                apply(record, event.eventId(), event.orderId(), () -> syncService.processDone(event, context));
            }
            default -> log.info("Status event ignored unexpected type service=order-service siteId={} eventType={} topic={} partition={} offset={} eventId={} orderId={}",
                    siteProperties.id(), eventType, record.topic(), record.partition(), record.offset(), eventId, orderId);
        }
    }

    private <T> T bind(
            ConsumerRecord<String, String> record,
            JsonNode root,
            Class<T> type,
            String name,
            UUID eventId,
            UUID orderId
    ) {
        try {
            return mapper.treeToValue(root, type);
        } catch (JacksonException e) {
            log.error("Poison status event (contract deserialization) service=order-service siteId={} topic={} partition={} offset={} eventId={} orderId={} type={}",
                    siteProperties.id(), record.topic(), record.partition(), record.offset(), eventId, orderId, name, e);
            throw new InvalidEventException("Failed to bind " + name, e);
        }
    }

    private void apply(ConsumerRecord<String, String> record, UUID eventId, UUID orderId, Runnable action) {
        try {
            action.run();
        } catch (InvalidEventException ex) {
            log.error("Poison status event (contract validation) service=order-service siteId={} topic={} partition={} offset={} eventId={} orderId={}: {}",
                    siteProperties.id(), record.topic(), record.partition(), record.offset(), eventId, orderId, ex.getMessage());
            throw ex;
        }
    }

    private void poison(ConsumerRecord<String, String> record, String message, UUID eventId, UUID orderId) {
        log.error("Poison status event service=order-service siteId={} topic={} partition={} offset={} eventId={} orderId={}: {}",
                siteProperties.id(), record.topic(), record.partition(), record.offset(), eventId, orderId, message);
        throw new InvalidEventException(message);
    }

    private UUID tryReadUuid(JsonNode root, String field) {
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
