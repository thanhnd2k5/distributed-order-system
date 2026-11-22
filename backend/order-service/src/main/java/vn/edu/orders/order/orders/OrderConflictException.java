package vn.edu.orders.order.orders;

import java.util.UUID;

public class OrderConflictException extends RuntimeException {
    private final UUID orderId;
    private final String currentStatus;

    public OrderConflictException(UUID orderId, String currentStatus, String message) {
        super(message);
        this.orderId = orderId;
        this.currentStatus = currentStatus;
    }

    public UUID orderId() {
        return orderId;
    }

    public String currentStatus() {
        return currentStatus;
    }
}
