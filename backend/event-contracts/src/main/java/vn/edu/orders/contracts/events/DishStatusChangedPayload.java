package vn.edu.orders.contracts.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public record DishStatusChangedPayload(
        String dishId,
        DishStatus status,
        int epoch,
        long seq
) {
    public MenuVersion version() {
        return new MenuVersion(epoch, seq);
    }
}
