package vn.edu.orders.order.menu.api;

import vn.edu.orders.contracts.events.DishStatus;

public record MenuItemResponse(
        String dishId,
        String name,
        DishStatus status,
        int epoch,
        long seq
) {
}
