package vn.edu.orders.inventory.api;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import vn.edu.orders.contracts.events.SourceType;

public record ReservationResponse(
        UUID orderId,
        SourceType sourceType,
        String sourceSiteId,
        String decision,
        String reasonCode,
        String reasonMessage,
        Instant createdAt,
        Instant updatedAt,
        List<ReservedIngredient> items
) {
    public ReservationResponse {
        items = items == null ? List.of() : List.copyOf(items);
    }

    public record ReservedIngredient(
            String ingredientId,
            String ingredientName,
            String unit,
            long reservedQuantity
    ) {
    }
}
