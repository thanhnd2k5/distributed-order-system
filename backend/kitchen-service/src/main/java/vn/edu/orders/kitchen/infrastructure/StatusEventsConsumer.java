package vn.edu.orders.kitchen.infrastructure;

import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import vn.edu.orders.contracts.events.EventTopics;
import vn.edu.orders.contracts.events.EventType;
import vn.edu.orders.contracts.events.ReservedEvent;
import vn.edu.orders.kitchen.application.EventProcessingContext;
import vn.edu.orders.kitchen.application.InvalidEventException;
import vn.edu.orders.kitchen.application.KitchenTicketService;
import vn.edu.orders.kitchen.config.KitchenProperties;

@Component
public class StatusEventsConsumer {
    public static final String LISTENER_ID = "kitchen-status-events";
    private static final Logger log = LoggerFactory.getLogger(StatusEventsConsumer.class);

    private final ObjectMapper json;
    private final KitchenTicketService ticketService;
    private final String instanceId;

    public StatusEventsConsumer(
            ObjectMapper json,
            KitchenTicketService ticketService,
            KitchenProperties properties
    ) {
        this.json = json;
        this.ticketService = ticketService;
        this.instanceId = properties.instanceId();
    }

    @KafkaListener(
            id = LISTENER_ID,
            idIsGroup = false,
            groupId = "${spring.kafka.consumer.group-id}",
            topics = EventTopics.STATUS_EVENTS,
            concurrency = "1")
    public void onMessage(ConsumerRecord<String, String> record) {
        String topic = record.topic();
        int partition = record.partition();
        long offset = record.offset();
        String raw = record.value();

        if (raw == null || raw.isBlank()) {
            log.error("Poison message on Kafka topic (empty payload) instanceId={} topic={} partition={} offset={} eventId=unknown",
                    instanceId, topic, partition, offset);
            throw new InvalidEventException("Empty event payload");
        }

        JsonNode root;
        try {
            root = json.readTree(raw);
        } catch (JacksonException ex) {
            log.error("Poison message on Kafka topic (malformed JSON) instanceId={} topic={} partition={} offset={} eventId=unknown: {}",
                    instanceId, topic, partition, offset, ex.getOriginalMessage());
            throw new InvalidEventException("Malformed JSON payload", null, null, ex);
        }

        UUID readableEventId = tryReadUuid(root, "eventId");
        UUID readableOrderId = tryReadUuid(root, "orderId");

        JsonNode eventTypeNode = root.get("eventType");
        if (eventTypeNode == null || !eventTypeNode.isString() || eventTypeNode.asString().isBlank()) {
            log.error("Poison message on Kafka topic (missing/invalid eventType) instanceId={} topic={} partition={} offset={} eventId={} orderId={}",
                    instanceId, topic, partition, offset, readableEventId, readableOrderId);
            throw new InvalidEventException("Missing or invalid eventType", readableEventId, readableOrderId);
        }

        JsonNode schemaNode = root.get("schemaVersion");
        if (schemaNode == null || !schemaNode.isInt() || schemaNode.asInt() != EventTopics.SCHEMA_VERSION_1) {
            log.error("Poison message on Kafka topic (unsupported schemaVersion) instanceId={} topic={} partition={} offset={} eventId={} orderId={}",
                    instanceId, topic, partition, offset, readableEventId, readableOrderId);
            throw new InvalidEventException("Unsupported schemaVersion", readableEventId, readableOrderId);
        }

        String eventTypeText = eventTypeNode.asString();

        if (EventType.Failed.name().equals(eventTypeText) || EventType.Done.name().equals(eventTypeText)) {
            log.info("Kitchen skipped status event instanceId={} topic={} partition={} offset={} eventId={} orderId={} eventType={} outcome=IGNORED_EVENT_TYPE",
                    instanceId, topic, partition, offset, readableEventId, readableOrderId, eventTypeText);
            return;
        }

        if (!EventType.Reserved.name().equals(eventTypeText)) {
            log.warn("Skipping unknown eventType on status-events instanceId={} topic={} partition={} offset={} eventId={} orderId={} eventType={}",
                    instanceId, topic, partition, offset, readableEventId, readableOrderId, eventTypeText);
            return;
        }

        ReservedEvent event;
        try {
            event = json.readValue(raw, ReservedEvent.class);
        } catch (JacksonException ex) {
            log.error("Poison message on Kafka topic (contract deserialization error) instanceId={} topic={} partition={} offset={} eventId={} orderId={}: {}",
                    instanceId, topic, partition, offset, readableEventId, readableOrderId, ex.getOriginalMessage());
            throw new InvalidEventException("Invalid ReservedEvent JSON", readableEventId, readableOrderId, ex);
        }

        if (event.orderId() != null
                && record.key() != null
                && !event.orderId().toString().equals(record.key())) {
            log.warn("Reserved Kafka key does not match orderId instanceId={} topic={} partition={} offset={} eventId={} orderId={} kafkaKey={}",
                    instanceId, topic, partition, offset, event.eventId(), event.orderId(), record.key());
        }

        try {
            ticketService.processReserved(event, new EventProcessingContext(partition, offset));
        } catch (InvalidEventException ex) {
            log.error("Poison message on Kafka topic (contract validation error) instanceId={} topic={} partition={} offset={} eventId={} orderId={}: {}",
                    instanceId, topic, partition, offset,
                    ex.eventId() != null ? ex.eventId() : readableEventId,
                    ex.orderId() != null ? ex.orderId() : readableOrderId,
                    ex.getMessage());
            throw ex;
        }
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
