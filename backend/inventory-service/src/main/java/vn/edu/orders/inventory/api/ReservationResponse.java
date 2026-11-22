package vn.edu.orders.inventory.api;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import vn.edu.orders.contracts.events.InventoryStatus;
import vn.edu.orders.contracts.events.ReservationOutcome;

public record ReservationResponse(
        UUID orderId,
        String originBranch,
        InventoryStatus status,
        String reason,
        Instant orderCreatedAt,
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
            BigDecimal quantity,
            ReservationOutcome outcome
    ) {
    }
}
