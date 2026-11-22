package vn.edu.orders.contracts.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.time.Instant;
import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
public record EventEnvelope<T>(
        UUID eventId,
        EventType eventType,
        int schemaVersion,
        UUID orderId,
        String branchId,
        Instant occurredAt,
        UUID causationId,
        T payload
) {
    public static <T> EventEnvelope<T> v1(
            UUID eventId,
            EventType eventType,
            UUID orderId,
            String branchId,
            Instant occurredAt,
            UUID causationId,
            T payload
    ) {
        return new EventEnvelope<>(
                eventId,
                eventType,
                EventTopics.SCHEMA_VERSION_1,
                orderId,
                branchId,
                occurredAt,
                causationId,
                payload
        );
    }

    public static <T> EventEnvelope<T> v1(
            UUID eventId,
            EventType eventType,
            UUID orderId,
            String branchId,
            Instant occurredAt,
            T payload
    ) {
        return v1(eventId, eventType, orderId, branchId, occurredAt, null, payload);
    }

    public static <T> EventEnvelope<T> v1Menu(
            UUID eventId,
            EventType eventType,
            String branchId,
            Instant occurredAt,
            UUID causationId,
            T payload
    ) {
        return v1(eventId, eventType, null, branchId, occurredAt, causationId, payload);
    }

    public EventEnvelope<T> validateEnvelope() {
        if (eventId == null) {
            throw new InvalidEventException("Missing eventId", null, orderId);
        }
        if (eventType == null) {
            throw new InvalidEventException("Missing eventType", eventId, orderId);
        }
        if (schemaVersion != EventTopics.SUPPORTED_SCHEMA_VERSION) {
            throw new InvalidEventException("Unsupported schemaVersion: " + schemaVersion, eventId, orderId);
        }
        if (eventType.requiresOrderId() && orderId == null) {
            throw new InvalidEventException("Missing orderId for " + eventType, eventId, null);
        }
        if (eventType.requiresOrderId() && (branchId == null || branchId.isBlank())) {
            throw new InvalidEventException("Missing branchId for " + eventType, eventId, orderId);
        }
        if (occurredAt == null) {
            throw new InvalidEventException("Missing occurredAt", eventId, orderId);
        }
        if (payload == null) {
            throw new InvalidEventException("Missing payload", eventId, orderId);
        }
        return this;
    }
}
