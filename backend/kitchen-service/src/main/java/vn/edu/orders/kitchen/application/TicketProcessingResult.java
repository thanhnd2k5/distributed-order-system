package vn.edu.orders.kitchen.application;

import java.util.UUID;

public record TicketProcessingResult(
        UUID eventId,
        UUID orderId,
        UUID ticketId,
        Outcome outcome,
        String status
) {
    public enum Outcome {
        APPLIED,
        DUPLICATE_EVENT,
        DUPLICATE_ORDER,
        PAYLOAD_CONFLICT
    }
}
