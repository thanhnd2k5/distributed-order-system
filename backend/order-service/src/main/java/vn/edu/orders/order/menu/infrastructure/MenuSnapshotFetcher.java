package vn.edu.orders.order.menu.infrastructure;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;
import vn.edu.orders.contracts.events.DishStatus;

public interface MenuSnapshotFetcher {
    MenuSnapshotResponse fetchSnapshot();

    @JsonIgnoreProperties(ignoreUnknown = true)
    record MenuSnapshotResponse(
            int epoch,
            long seq,
            List<MenuSnapshotDish> dishes
    ) {
        public MenuSnapshotResponse {
            dishes = dishes == null ? List.of() : List.copyOf(dishes);
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    record MenuSnapshotDish(
            String dishId,
            String dishName,
            DishStatus status,
            int epoch,
            long seq
    ) {
    }
}
