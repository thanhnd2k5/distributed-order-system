package vn.edu.orders.inventory.application;

import java.util.UUID;
import vn.edu.orders.contracts.events.FailureReasonCode;

public record ReservationResult(
        UUID eventId,
        UUID orderId,
        Outcome outcome,
        String decision,
        FailureReasonCode reasonCode
) {
    public enum Outcome {
        APPLIED,
        DUPLICATE_EVENT,
        DUPLICATE_ORDER,
        PAYLOAD_CONFLICT
    }

    public boolean isNewlyApplied() {
        return outcome == Outcome.APPLIED;
    }
}
