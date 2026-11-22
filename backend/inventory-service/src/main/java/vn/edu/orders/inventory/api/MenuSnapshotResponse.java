package vn.edu.orders.inventory.api;

import java.util.List;
import vn.edu.orders.contracts.events.DishStatus;

public record MenuSnapshotResponse(
        int epoch,
        long seq,
        List<DishSnapshotItem> dishes
) {
    public MenuSnapshotResponse {
        dishes = dishes == null ? List.of() : List.copyOf(dishes);
    }

    public record DishSnapshotItem(
            String dishId,
            String dishName,
            DishStatus status,
            int epoch,
            long seq
    ) {
    }
}
