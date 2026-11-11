package vn.edu.orders.contracts.events;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record OrderCreatedEvent(
        UUID eventId,
        int schemaVersion,
        EventType eventType,
        UUID orderId,
        SourceType sourceType,
        String sourceSiteId,
        Instant occurredAt,
        Payload payload
) {
    public static OrderCreatedEvent v1(
            UUID eventId,
            UUID orderId,
            SourceType sourceType,
            String sourceSiteId,
            Instant occurredAt,
            List<OrderItemLine> items
    ) {
        return new OrderCreatedEvent(
                eventId,
                EventTopics.SCHEMA_VERSION_1,
                EventType.OrderCreated,
                orderId,
                sourceType,
                sourceSiteId,
                occurredAt,
                new Payload(items)
        );
    }

    public record Payload(List<OrderItemLine> items) {
        public Payload {
            items = items == null ? null : List.copyOf(items);
        }
    }
}
