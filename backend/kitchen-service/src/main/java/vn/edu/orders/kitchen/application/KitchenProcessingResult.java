package vn.edu.orders.kitchen.application;

import java.util.UUID;
import vn.edu.orders.contracts.events.KitchenStatus;

public record KitchenProcessingResult(
        UUID eventId,
        UUID orderId,
        Outcome outcome,
        KitchenStatus status
) {
    public enum Outcome {
        APPLIED,
        DUPLICATE_EVENT,
        DUPLICATE_ORDER,
        STORED_RESULT,
        CANCEL_REJECTED,
        IGNORED_WRONG_BRANCH,
        IGNORED_EVENT_TYPE,
        IGNORED_INVALID_TRANSITION,
        STALE_VERSION
    }
}
