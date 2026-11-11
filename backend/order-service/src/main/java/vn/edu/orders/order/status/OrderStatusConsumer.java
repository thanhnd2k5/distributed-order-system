package vn.edu.orders.order.status;

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
            id = "order-status-consumer",
            topics = EventTopics.STATUS_EVENTS,
            groupId = "#{@orderStatusGroupId}"
    )
    public void onMessage(ConsumerRecord<String, String> record) {
        JsonNode root;
        try {
            root = mapper.readTree(record.value());
        } catch (JacksonException e) {
            log.error("Failed to parse status event JSON from topic={} partition={} offset={}",
                    record.topic(), record.partition(), record.offset(), e);
            throw new InvalidEventException("Malformed JSON record in " + record.topic(), e);
        }

        if (root == null || !root.isObject()) {
            throw new InvalidEventException("Status event is not a JSON object: " + record.value());
        }

        // Blast-radius isolation: skip foreign site events before any deeper deserialization
        String sourceSiteId = root.path("sourceSiteId").asText(null);
        if (sourceSiteId == null || !sourceSiteId.equals(siteProperties.id())) {
            log.debug("OrderStatusConsumer skipping foreign site event siteId={} sourceSiteId={} topic={} partition={} offset={}",
                    siteProperties.id(), sourceSiteId, record.topic(), record.partition(), record.offset());
            return;
        }

        // Schema version check for matching site
        int schemaVersion = root.path("schemaVersion").asInt(-1);
        if (schemaVersion != 1) {
            throw new InvalidEventException("Unsupported schemaVersion=" + schemaVersion + " for siteId=" + siteProperties.id());
        }

        String eventType = root.path("eventType").asText("");
        switch (eventType) {
            case "Reserved" -> {
                ReservedEvent event;
                try {
                    event = mapper.treeToValue(root, ReservedEvent.class);
                } catch (JacksonException e) {
                    throw new InvalidEventException("Failed to bind ReservedEvent", e);
                }
                syncService.processReserved(event);
            }
            case "Failed" -> {
                FailedEvent event;
                try {
                    event = mapper.treeToValue(root, FailedEvent.class);
                } catch (JacksonException e) {
                    throw new InvalidEventException("Failed to bind FailedEvent", e);
                }
                syncService.processFailed(event);
            }
            case "Done" -> {
                DoneEvent event;
                try {
                    event = mapper.treeToValue(root, DoneEvent.class);
                } catch (JacksonException e) {
                    throw new InvalidEventException("Failed to bind DoneEvent", e);
                }
                syncService.processDone(event);
            }
            default -> {
                log.info("OrderStatusConsumer ignored unexpected eventType={} siteId={} topic={} partition={} offset={}",
                        eventType, siteProperties.id(), record.topic(), record.partition(), record.offset());
            }
        }
    }
}
