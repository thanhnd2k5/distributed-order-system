package vn.edu.orders.inventory.api;

import java.util.List;

public record RecipeResponse(
        String dishId,
        List<RecipeIngredient> ingredients
) {
    public RecipeResponse {
        ingredients = ingredients == null ? List.of() : List.copyOf(ingredients);
    }

    public record RecipeIngredient(
            String ingredientId,
            String ingredientName,
            String unit,
            long quantityPerDish
    ) {
    }
}
