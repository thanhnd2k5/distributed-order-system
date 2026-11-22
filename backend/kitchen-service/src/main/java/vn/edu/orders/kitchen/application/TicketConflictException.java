package vn.edu.orders.kitchen.application;

import java.util.UUID;

public class TicketConflictException extends RuntimeException {
    private final UUID orderId;
    private final String currentStatus;
    private final String expectedStatus;

    public TicketConflictException(UUID orderId, String currentStatus, String expectedStatus) {
        super("Ticket " + orderId + " is in status " + currentStatus + ", expected " + expectedStatus);
        this.orderId = orderId;
        this.currentStatus = currentStatus;
        this.expectedStatus = expectedStatus;
    }

    public UUID orderId() {
        return orderId;
    }

    public String currentStatus() {
        return currentStatus;
    }

    public String expectedStatus() {
        return expectedStatus;
    }
}
