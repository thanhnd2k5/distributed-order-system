package vn.edu.orders.kitchen.api;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record KitchenTicketResponse(
        UUID orderId,
        String status,
        String originBranch,
        String cancelledBy,
        String closeReason,
        List<DishItem> items,
        List<ReservedIngredient> reserved,
        List<String> spoiled,
        boolean warning,
        List<String> flaggedIngredients,
        Instant orderCreatedAt,
        Instant updatedAt
) {
    public KitchenTicketResponse {
        items = items == null ? List.of() : List.copyOf(items);
        reserved = reserved == null ? List.of() : List.copyOf(reserved);
        spoiled = spoiled == null ? List.of() : List.copyOf(spoiled);
        flaggedIngredients = flaggedIngredients == null ? List.of() : List.copyOf(flaggedIngredients);
    }

    public record DishItem(
            int lineNo,
            String dishId,
            String dishName,
            int quantity
    ) {
    }

    public record ReservedIngredient(
            String ingredientId,
            BigDecimal quantity
    ) {
    }
}
