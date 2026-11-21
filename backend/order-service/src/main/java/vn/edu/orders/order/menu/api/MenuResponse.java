package vn.edu.orders.order.menu.api;

import java.time.Instant;
import java.util.List;

public record MenuResponse(
        boolean stale,
        boolean kafkaConnected,
        int epoch,
        Instant lastHeardAt,
        List<MenuItemResponse> dishes
) {
}
