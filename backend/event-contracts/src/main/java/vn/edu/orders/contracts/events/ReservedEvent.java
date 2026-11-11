package vn.edu.orders.contracts.events;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record ReservedEvent(
        UUID eventId,
        int schemaVersion,
        EventType eventType,
        UUID orderId,
        SourceType sourceType,
        String sourceSiteId,
        Instant occurredAt,
        Payload payload
) {
    public static ReservedEvent v1(
            UUID eventId,
            UUID orderId,
            SourceType sourceType,
            String sourceSiteId,
            Instant occurredAt,
            UUID reservationId,
            List<OrderItemLine> items
    ) {
        return new ReservedEvent(
                eventId,
                EventTopics.SCHEMA_VERSION_1,
                EventType.Reserved,
                orderId,
                sourceType,
                sourceSiteId,
                occurredAt,
                new Payload(reservationId, items)
        );
    }

    public record Payload(UUID reservationId, List<OrderItemLine> items) {
        public Payload {
            items = items == null ? null : List.copyOf(items);
        }
    }
}
