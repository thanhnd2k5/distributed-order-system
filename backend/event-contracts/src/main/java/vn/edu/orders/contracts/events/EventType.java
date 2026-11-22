package vn.edu.orders.contracts.events;

import java.util.Optional;

public enum EventType {
    OrderCreated,
    CancelRequested,
    OrderReserved,
    OrderFailed,
    StockConsumed,
    ReleaseRequested,
    CancelPending,
    CancelRejected,
    OrderCancelled,
    OrderCompleted,
    DishStatusChanged,
    IngredientCheckChanged,
    Heartbeat;

    public boolean isMenuEvent() {
        return this == DishStatusChanged || this == IngredientCheckChanged || this == Heartbeat;
    }

    public boolean requiresOrderId() {
        return !isMenuEvent();
    }

    public static Optional<EventType> fromValue(String raw) {
        if (raw == null || raw.isBlank()) {
            return Optional.empty();
        }
        for (EventType value : values()) {
            if (value.name().equals(raw)) {
                return Optional.of(value);
            }
        }
        return Optional.empty();
    }
}
