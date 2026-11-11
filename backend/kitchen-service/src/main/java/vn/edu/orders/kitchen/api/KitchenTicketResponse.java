package vn.edu.orders.kitchen.api;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record KitchenTicketResponse(
        UUID ticketId,
        UUID orderId,
        String sourceType,
        String sourceSiteId,
        String status,
        Instant createdAt,
        Instant completedAt,
        List<Item> items
) {
    public KitchenTicketResponse {
        items = items == null ? List.of() : List.copyOf(items);
    }

    public record Item(
            int lineNo,
            String dishId,
            String dishName,
            int quantity
    ) {
    }
}
