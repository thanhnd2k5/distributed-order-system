package vn.edu.orders.contracts.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public record OrderCancelledPayload(
        CancelledBy cancelledBy,
        ReasonCode reasonCode
) {
}
