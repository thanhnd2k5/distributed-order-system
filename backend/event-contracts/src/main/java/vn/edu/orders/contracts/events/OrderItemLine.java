package vn.edu.orders.contracts.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public record OrderItemLine(
        int lineNo,
        String dishId,
        String dishName,
        int quantity
) {
}
