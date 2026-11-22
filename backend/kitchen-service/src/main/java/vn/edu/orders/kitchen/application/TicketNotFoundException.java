package vn.edu.orders.kitchen.application;

import java.util.UUID;

public class TicketNotFoundException extends RuntimeException {
    private final UUID orderId;

    public TicketNotFoundException(UUID orderId) {
        super("Kitchen ticket not found: " + orderId);
        this.orderId = orderId;
    }

    public UUID orderId() {
        return orderId;
    }
}
