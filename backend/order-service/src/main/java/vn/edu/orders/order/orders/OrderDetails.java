package vn.edu.orders.order.orders;

import java.util.List;

public record OrderDetails(OrderSummary order, List<Item> items) {
    public record Item(int lineNo, String dishId, String dishName, int quantity) {
    }
}
