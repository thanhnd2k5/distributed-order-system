package vn.edu.orders.contracts.events;

import java.util.UUID;

public class InvalidEventException extends RuntimeException {
    private final UUID eventId;
    private final UUID orderId;

    public InvalidEventException(String message) {
        this(message, null, null, null);
    }

    public InvalidEventException(String message, Throwable cause) {
        this(message, null, null, cause);
    }

    public InvalidEventException(String message, UUID eventId, UUID orderId) {
        this(message, eventId, orderId, null);
    }

    public InvalidEventException(String message, UUID eventId, UUID orderId, Throwable cause) {
        super(message, cause);
        this.eventId = eventId;
        this.orderId = orderId;
    }

    public UUID eventId() {
        return eventId;
    }

    public UUID orderId() {
        return orderId;
    }
}
