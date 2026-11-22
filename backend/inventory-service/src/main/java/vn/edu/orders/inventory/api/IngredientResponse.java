package vn.edu.orders.inventory.api;

import java.math.BigDecimal;
import java.time.Instant;

public record IngredientResponse(
        String ingredientId,
        String name,
        String unit,
        BigDecimal availableQuantity,
        Instant updatedAt
) {
}
