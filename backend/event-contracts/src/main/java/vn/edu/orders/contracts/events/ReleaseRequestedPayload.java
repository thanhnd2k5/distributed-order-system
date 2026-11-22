package vn.edu.orders.contracts.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public record ReleaseRequestedPayload(
        CancelledBy cancelledBy,
        ReasonCode reasonCode,
        List<String> spoiledIngredients
) {
    public ReleaseRequestedPayload {
        spoiledIngredients = spoiledIngredients == null ? List.of() : List.copyOf(spoiledIngredients);
    }

    public ReleaseRequestedPayload(CancelledBy cancelledBy, ReasonCode reasonCode) {
        this(cancelledBy, reasonCode, List.of());
    }
}
