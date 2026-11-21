package vn.edu.orders.contracts.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.time.Instant;
import java.util.UUID;

@Deprecated
@JsonIgnoreProperties(ignoreUnknown = true)
public record DoneEvent(
        UUID eventId,
        int schemaVersion,
        String eventType,
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
                "Done",
                orderId,
                sourceType,
                sourceSiteId,
                occurredAt,
                new Payload(ticketId, completedAt)
        );
    }

    public EventEnvelope<OrderCompletedPayload> toOrderCompletedEnvelope(UUID causationId) {
        return EventEnvelope.v1(
                eventId,
                EventType.OrderCompleted,
                orderId,
                sourceSiteId,
                occurredAt,
                causationId,
                OrderCompletedPayload.empty()
        );
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Payload(UUID ticketId, Instant completedAt) {
    }
}
