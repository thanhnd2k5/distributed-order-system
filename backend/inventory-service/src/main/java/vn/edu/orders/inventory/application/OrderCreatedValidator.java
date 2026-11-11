package vn.edu.orders.inventory.application;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import vn.edu.orders.contracts.events.EventTopics;
import vn.edu.orders.contracts.events.EventType;
import vn.edu.orders.contracts.events.OrderCreatedEvent;
import vn.edu.orders.contracts.events.OrderItemLine;

public final class OrderCreatedValidator {
    private static final Pattern SITE_PATTERN = Pattern.compile("[A-Z][A-Z0-9_]{0,49}");

    private OrderCreatedValidator() {
    }

    public static void validate(OrderCreatedEvent event) {
        if (event == null) {
            throw new InvalidEventException("OrderCreated event must not be null");
        }
        UUID eventId = event.eventId();
        UUID orderId = event.orderId();

        if (eventId == null) {
            throw new InvalidEventException("Missing eventId", null, orderId);
        }
        if (event.schemaVersion() != EventTopics.SCHEMA_VERSION_1) {
            throw new InvalidEventException(
                    "Unsupported schemaVersion: " + event.schemaVersion(), eventId, orderId);
        }
        if (event.eventType() != EventType.OrderCreated) {
            throw new InvalidEventException(
                    "Expected eventType OrderCreated but got " + event.eventType(), eventId, orderId);
        }
        if (orderId == null) {
            throw new InvalidEventException("Missing orderId", eventId, null);
        }
        if (event.sourceType() == null) {
            throw new InvalidEventException("Missing sourceType", eventId, orderId);
        }
        if (event.sourceSiteId() == null || !SITE_PATTERN.matcher(event.sourceSiteId()).matches()) {
            throw new InvalidEventException(
                    "Invalid sourceSiteId: " + event.sourceSiteId(), eventId, orderId);
        }
        if (event.occurredAt() == null) {
            throw new InvalidEventException("Missing occurredAt", eventId, orderId);
        }
        if (event.payload() == null || event.payload().items() == null || event.payload().items().isEmpty()) {
            throw new InvalidEventException("OrderCreated payload.items must not be empty", eventId, orderId);
        }

        Set<String> seenDishes = new HashSet<>();
        Set<Integer> seenLines = new HashSet<>();
        for (OrderItemLine item : event.payload().items()) {
            if (item == null) {
                throw new InvalidEventException("OrderItemLine must not be null", eventId, orderId);
            }
            if (item.lineNo() <= 0 || !seenLines.add(item.lineNo())) {
                throw new InvalidEventException("Invalid or duplicate lineNo: " + item.lineNo(), eventId, orderId);
            }
            if (item.dishId() == null || item.dishId().isBlank() || item.dishId().length() > 50) {
                throw new InvalidEventException("Invalid dishId: " + item.dishId(), eventId, orderId);
            }
            if (item.dishName() == null || item.dishName().isBlank() || item.dishName().length() > 200) {
                throw new InvalidEventException("Invalid dishName for dishId " + item.dishId(), eventId, orderId);
            }
            if (item.quantity() <= 0) {
                throw new InvalidEventException(
                        "Invalid quantity " + item.quantity() + " for dishId " + item.dishId(), eventId, orderId);
            }
            if (!seenDishes.add(item.dishId())) {
                throw new InvalidEventException("Duplicate dishId: " + item.dishId(), eventId, orderId);
            }
        }
    }

    public static String fingerprint(OrderCreatedEvent event) {
        String canonicalItems = event.payload().items().stream()
                .sorted(Comparator.comparing(OrderItemLine::dishId))
                .map(item -> item.dishId() + ":" + item.quantity())
                .collect(Collectors.joining(","));
        String canonical = event.sourceType().name() + "|" + event.sourceSiteId() + "|" + canonicalItems;
        try {
            byte[] hash = MessageDigest.getInstance("SHA-256").digest(canonical.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SHA-256 algorithm unavailable", ex);
        }
    }
}
