package vn.edu.orders.inventory.api;

import java.time.Instant;

public record IngredientResponse(
        String ingredientId,
        String name,
        String unit,
        long availableQuantity,
        Instant updatedAt
) {
}
