package vn.edu.orders.contracts.events;

public record OrderItemLine(
        int lineNo,
        String dishId,
        String dishName,
        int quantity
) {
}
