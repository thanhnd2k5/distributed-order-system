package vn.edu.orders.inventory.infrastructure;

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
import vn.edu.orders.contracts.events.OrderCreatedEvent;
import vn.edu.orders.inventory.application.DemoFailpoint;
import vn.edu.orders.inventory.application.EventProcessingContext;
import vn.edu.orders.inventory.application.InvalidEventException;
import vn.edu.orders.inventory.application.InventoryReservationService;
import vn.edu.orders.inventory.application.ReservationResult;
import vn.edu.orders.inventory.config.InventoryProperties;

@Component
public class OrderEventsConsumer {
    public static final String LISTENER_ID = "inventory-order-events";
    private static final Logger log = LoggerFactory.getLogger(OrderEventsConsumer.class);

    private final ObjectMapper json;
    private final InventoryReservationService reservationService;
    private final DemoFailpoint failpoint;
    private final String instanceId;

    public OrderEventsConsumer(
            ObjectMapper json,
            InventoryReservationService reservationService,
            DemoFailpoint failpoint,
            InventoryProperties properties
    ) {
        this.json = json;
        this.reservationService = reservationService;
        this.failpoint = failpoint;
        this.instanceId = properties.instanceId();
    }

    @KafkaListener(
            id = LISTENER_ID,
            idIsGroup = false,
            groupId = "${spring.kafka.consumer.group-id}",
            topics = EventTopics.ORDER_EVENTS,
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

        String eventTypeText = eventTypeNode.asString();
        if (!EventType.OrderCreated.name().equals(eventTypeText)) {
            log.warn("Skipping non-OrderCreated event on order-events instanceId={} topic={} partition={} offset={} eventId={} orderId={} eventType={}",
                    instanceId, topic, partition, offset, readableEventId, readableOrderId, eventTypeText);
            return;
        }

        OrderCreatedEvent event;
        try {
            event = json.readValue(raw, OrderCreatedEvent.class);
        } catch (JacksonException ex) {
            log.error("Poison message on Kafka topic (contract deserialization error) instanceId={} topic={} partition={} offset={} eventId={} orderId={}: {}",
                    instanceId, topic, partition, offset, readableEventId, readableOrderId, ex.getOriginalMessage());
            throw new InvalidEventException(
                    "Invalid OrderCreatedEvent JSON", readableEventId, readableOrderId, ex);
        }

        if (event.orderId() != null
                && record.key() != null
                && !event.orderId().toString().equals(record.key())) {
            log.warn("OrderCreated Kafka key does not match orderId instanceId={} topic={} partition={} offset={} eventId={} orderId={} kafkaKey={}",
                    instanceId, topic, partition, offset, event.eventId(), event.orderId(), record.key());
        }

        ReservationResult result;
        try {
            result = reservationService.processOrderCreated(event, new EventProcessingContext(partition, offset));
        } catch (InvalidEventException ex) {
            log.error("Poison message on Kafka topic (contract validation error) instanceId={} topic={} partition={} offset={} eventId={} orderId={}: {}",
                    instanceId, topic, partition, offset,
                    ex.eventId() != null ? ex.eventId() : readableEventId,
                    ex.orderId() != null ? ex.orderId() : readableOrderId,
                    ex.getMessage());
            throw ex;
        }

        failpoint.maybeHaltAfterCommit(result, partition, offset);
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
