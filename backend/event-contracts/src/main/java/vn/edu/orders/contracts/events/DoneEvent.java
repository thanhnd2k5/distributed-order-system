package vn.edu.orders.contracts.events;

import java.time.Instant;
import java.util.UUID;

public record DoneEvent(
        UUID eventId,
        int schemaVersion,
        EventType eventType,
        UUID orderId,
        SourceType sourceType,
        String sourceSiteId,
        Instant occurredAt,
        Payload payload
) {
    public static DoneEvent v1(
            UUID eventId,
            UUID orderId,
            SourceType sourceType,
            String sourceSiteId,
            Instant occurredAt,
            UUID ticketId,
            Instant completedAt
    ) {
        return new DoneEvent(
                eventId,
                EventTopics.SCHEMA_VERSION_1,
                EventType.Done,
                orderId,
                sourceType,
                sourceSiteId,
                occurredAt,
                new Payload(ticketId, completedAt)
        );
    }

    public record Payload(UUID ticketId, Instant completedAt) {
    }
}
