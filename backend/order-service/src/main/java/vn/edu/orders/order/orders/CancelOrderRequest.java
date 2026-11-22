package vn.edu.orders.order.orders;

import com.fasterxml.jackson.annotation.JsonAlias;
import jakarta.validation.constraints.NotNull;
import vn.edu.orders.contracts.events.ReasonCode;

public record CancelOrderRequest(
        @NotNull(message = "reasonCode is required")
        @JsonAlias("reason_code")
        ReasonCode reasonCode
) {
}
