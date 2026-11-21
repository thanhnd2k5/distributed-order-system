package vn.edu.orders.contracts.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import java.time.Instant;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public record OrderReservedPayload(
        String originBranch,
        Instant createdAt,
        List<OrderItemLine> items,
        List<IngredientQuantity> reserved
) {
    public OrderReservedPayload {
        items = items == null ? null : List.copyOf(items);
        reserved = reserved == null ? null : List.copyOf(reserved);
    }

    public static OrderReservedPayload forKitchen(
            String originBranch,
            Instant createdAt,
            List<OrderItemLine> items,
            List<IngredientQuantity> reserved
    ) {
        return new OrderReservedPayload(originBranch, createdAt, items, reserved);
    }

    public static OrderReservedPayload forBranch() {
        return new OrderReservedPayload(null, null, null, null);
    }

    public static OrderReservedPayload empty() {
        return forBranch();
    }
}
