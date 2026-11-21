package vn.edu.orders.contracts.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public record IngredientCheckChangedPayload(
        String ingredientId,
        boolean needsCheck,
        int epoch,
        long seq
) {
    public MenuVersion version() {
        return new MenuVersion(epoch, seq);
    }
}
