package vn.edu.orders.inventory.infrastructure;

import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
import vn.edu.orders.contracts.events.ReleaseRequestedPayload;
import vn.edu.orders.contracts.events.StockConsumedPayload;
import vn.edu.orders.inventory.application.EventProcessingContext;
import vn.edu.orders.inventory.application.StockCommandService;
import vn.edu.orders.inventory.config.InventoryProperties;

@Component
public class StockCommandsConsumer {
    public static final String LISTENER_ID = "inventory-stock-commands";

    private final ObjectMapper json;
    private final JavaType stockConsumedType;
    private final JavaType releaseRequestedType;
    private final StockCommandService stockCommandService;
    private final DlqService dlqService;
    private final InventoryProperties properties;

    public StockCommandsConsumer(
            ObjectMapper json,
            StockCommandService stockCommandService,
            DlqService dlqService,
            InventoryProperties properties
    ) {
        this.json = json;
        this.stockConsumedType = json.getTypeFactory()
                .constructParametricType(EventEnvelope.class, StockConsumedPayload.class);
        this.releaseRequestedType = json.getTypeFactory()
                .constructParametricType(EventEnvelope.class, ReleaseRequestedPayload.class);
        this.stockCommandService = stockCommandService;
        this.dlqService = dlqService;
        this.properties = properties;
    }

    @KafkaListener(
            id = LISTENER_ID,
            idIsGroup = false,
            groupId = "${spring.kafka.consumer.group-id}",
            topics = EventTopics.STOCK_COMMANDS,
            concurrency = "1")
    public void onMessage(ConsumerRecord<String, String> record, Acknowledgment ack) {
        EventProcessingContext context = new EventProcessingContext(
                record.topic(), record.partition(), record.offset(), record.timestamp(), record.key());

        ParsedStockCommand parsed = null;
        InvalidEventException poisonFailure = null;
        int attempts = Math.max(1, properties.maxPoisonAttempts());
        for (int attempt = 1; attempt <= attempts; attempt++) {
            try {
                parsed = parseCommand(record);
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

        if (parsed.stockConsumed() != null) {
            stockCommandService.processStockConsumed(parsed.stockConsumed(), context);
        } else {
            stockCommandService.processReleaseRequested(parsed.releaseRequested(), context);
        }
        ack.acknowledge();
    }

    private ParsedStockCommand parseCommand(ConsumerRecord<String, String> record) {
        String raw = record.value();
        if (raw == null || raw.isBlank()) {
            throw new InvalidEventException("Empty stock-commands payload");
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
        if (versionNode.asInt() != EventTopics.SUPPORTED_SCHEMA_VERSION) {
            throw new InvalidEventException("Unsupported schemaVersion: " + versionNode.asInt(), eventId, orderId);
        }

        JsonNode eventTypeNode = root.get("eventType");
        if (eventTypeNode == null || !eventTypeNode.isString() || eventTypeNode.asString().isBlank()) {
            throw new InvalidEventException("Missing or invalid eventType", eventId, orderId);
        }
        String eventTypeText = eventTypeNode.asString();
        EventType eventType = EventType.fromValue(eventTypeText)
                .orElseThrow(() -> new InvalidEventException(
                        "Unknown eventType on stock-commands: " + eventTypeText, eventId, orderId));

        if (eventType == EventType.StockConsumed) {
            EventEnvelope<StockConsumedPayload> envelope;
            try {
                envelope = json.readValue(raw, stockConsumedType);
            } catch (JacksonException ex) {
                throw new InvalidEventException("Invalid StockConsumed JSON", eventId, orderId, ex);
            }
            envelope.validateEnvelope();
            return new ParsedStockCommand(envelope, null);
        }

        if (eventType == EventType.ReleaseRequested) {
            EventEnvelope<ReleaseRequestedPayload> envelope;
            try {
                envelope = json.readValue(raw, releaseRequestedType);
            } catch (JacksonException ex) {
                throw new InvalidEventException("Invalid ReleaseRequested JSON", eventId, orderId, ex);
            }
            envelope.validateEnvelope();
            if (envelope.payload().cancelledBy() == null || envelope.payload().reasonCode() == null) {
                throw new InvalidEventException(
                        "ReleaseRequested payload requires cancelledBy and reasonCode", eventId, orderId);
            }
            return new ParsedStockCommand(null, envelope);
        }

        throw new InvalidEventException("Unexpected eventType on stock-commands: " + eventType, eventId, orderId);
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    private record ParsedStockCommand(
            EventEnvelope<StockConsumedPayload> stockConsumed,
            EventEnvelope<ReleaseRequestedPayload> releaseRequested
    ) {
    }
}
