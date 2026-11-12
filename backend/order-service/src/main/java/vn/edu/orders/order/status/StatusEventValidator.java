package vn.edu.orders.order.status;

import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;
import vn.edu.orders.contracts.events.DoneEvent;
import vn.edu.orders.contracts.events.EventTopics;
import vn.edu.orders.contracts.events.EventType;
import vn.edu.orders.contracts.events.FailedEvent;
import vn.edu.orders.contracts.events.OrderItemLine;
import vn.edu.orders.contracts.events.ReservedEvent;
import vn.edu.orders.contracts.events.SourceType;

public final class StatusEventValidator {
    private static final Pattern SITE_PATTERN = Pattern.compile("[A-Z][A-Z0-9_]{0,49}");

    private StatusEventValidator() {
    }

    public static void validateReserved(ReservedEvent event) {
        if (event == null) {
            throw new InvalidEventException("Reserved event must not be null");
        }
        envelope(event.eventId(), event.schemaVersion(), event.eventType(), EventType.Reserved,
                event.orderId(), event.sourceType(), event.sourceSiteId(), event.occurredAt());
        if (event.payload() == null) {
            throw new InvalidEventException("Reserved payload must not be null");
        }
        if (event.payload().reservationId() == null) {
            throw new InvalidEventException("Reserved payload.reservationId must not be null");
        }
        if (event.payload().items() == null || event.payload().items().isEmpty()) {
            throw new InvalidEventException("Reserved payload.items must not be empty");
        }

        Set<String> seenDishes = new HashSet<>();
        Set<Integer> seenLines = new HashSet<>();
        for (OrderItemLine item : event.payload().items()) {
            if (item == null) {
                throw new InvalidEventException("OrderItemLine must not be null");
            }
            if (item.lineNo() <= 0 || !seenLines.add(item.lineNo())) {
                throw new InvalidEventException("Invalid or duplicate lineNo: " + item.lineNo());
            }
            if (item.dishId() == null || item.dishId().isBlank() || item.dishId().length() > 50) {
                throw new InvalidEventException("Invalid dishId: " + item.dishId());
            }
            if (item.dishName() == null || item.dishName().isBlank() || item.dishName().length() > 200) {
                throw new InvalidEventException("Invalid dishName for dishId " + item.dishId());
            }
            if (item.quantity() <= 0) {
                throw new InvalidEventException(
                        "Invalid quantity " + item.quantity() + " for dishId " + item.dishId());
            }
            if (!seenDishes.add(item.dishId())) {
                throw new InvalidEventException("Duplicate dishId: " + item.dishId());
            }
        }
    }

    public static void validateFailed(FailedEvent event) {
        if (event == null) {
            throw new InvalidEventException("Failed event must not be null");
        }
        envelope(event.eventId(), event.schemaVersion(), event.eventType(), EventType.Failed,
                event.orderId(), event.sourceType(), event.sourceSiteId(), event.occurredAt());
        if (event.payload() == null) {
            throw new InvalidEventException("Failed payload must not be null");
        }
        if (event.payload().reasonCode() == null) {
            throw new InvalidEventException("Failed payload.reasonCode must not be null");
        }
        if (event.payload().reasonMessage() == null || event.payload().reasonMessage().isBlank()) {
            throw new InvalidEventException("Failed payload.reasonMessage must not be blank");
        }
    }

    public static void validateDone(DoneEvent event) {
        if (event == null) {
            throw new InvalidEventException("Done event must not be null");
        }
        envelope(event.eventId(), event.schemaVersion(), event.eventType(), EventType.Done,
                event.orderId(), event.sourceType(), event.sourceSiteId(), event.occurredAt());
        if (event.payload() == null) {
            throw new InvalidEventException("Done payload must not be null");
        }
        if (event.payload().ticketId() == null) {
            throw new InvalidEventException("Done payload.ticketId must not be null");
        }
        if (event.payload().completedAt() == null) {
            throw new InvalidEventException("Done payload.completedAt must not be null");
        }
    }

    private static void envelope(
            UUID eventId,
            int schemaVersion,
            EventType actualType,
            EventType expectedType,
            UUID orderId,
            SourceType sourceType,
            String sourceSiteId,
            Instant occurredAt
    ) {
        if (eventId == null) {
            throw new InvalidEventException("Missing eventId");
        }
        if (schemaVersion != EventTopics.SCHEMA_VERSION_1) {
            throw new InvalidEventException("Unsupported schemaVersion: " + schemaVersion);
        }
        if (actualType != expectedType) {
            throw new InvalidEventException("Expected eventType " + expectedType + " but got " + actualType);
        }
        if (orderId == null) {
            throw new InvalidEventException("Missing orderId");
        }
        if (sourceType == null) {
            throw new InvalidEventException("Missing sourceType");
        }
        if (sourceSiteId == null || !SITE_PATTERN.matcher(sourceSiteId).matches()) {
            throw new InvalidEventException("Invalid sourceSiteId: " + sourceSiteId);
        }
        if (occurredAt == null) {
            throw new InvalidEventException("Missing occurredAt");
        }
    }
}
