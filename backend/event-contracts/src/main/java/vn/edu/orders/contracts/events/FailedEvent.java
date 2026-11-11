package vn.edu.orders.contracts.events;

import java.time.Instant;
import java.util.UUID;

public record FailedEvent(
        UUID eventId,
        int schemaVersion,
        EventType eventType,
        UUID orderId,
        SourceType sourceType,
        String sourceSiteId,
        Instant occurredAt,
        Payload payload
) {
    public static FailedEvent v1(
            UUID eventId,
            UUID orderId,
            SourceType sourceType,
            String sourceSiteId,
            Instant occurredAt,
            FailureReasonCode reasonCode,
            String reasonMessage
    ) {
        return new FailedEvent(
                eventId,
                EventTopics.SCHEMA_VERSION_1,
                EventType.Failed,
                orderId,
                sourceType,
                sourceSiteId,
                occurredAt,
                new Payload(reasonCode, reasonMessage)
        );
    }

    public record Payload(FailureReasonCode reasonCode, String reasonMessage) {
    }
}
