package vn.edu.orders.query.api;

public record ReplicatedOrderItem(
        int lineNo,
        String dishId,
        String dishName,
        int quantity
) {
}
