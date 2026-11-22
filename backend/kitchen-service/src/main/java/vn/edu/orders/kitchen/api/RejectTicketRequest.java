package vn.edu.orders.kitchen.api;

import java.util.List;

public record RejectTicketRequest(
        String reasonCode,
        List<String> spoiledIngredients
) {
    public RejectTicketRequest {
        spoiledIngredients = spoiledIngredients == null ? List.of() : List.copyOf(spoiledIngredients);
    }
}
