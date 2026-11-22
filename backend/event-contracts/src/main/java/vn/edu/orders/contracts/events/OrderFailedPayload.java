package vn.edu.orders.contracts.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public record OrderFailedPayload(
        String originBranch,
        ReasonCode reasonCode,
        String reasonMessage
) {
    public OrderFailedPayload(String originBranch, ReasonCode reasonCode) {
        this(originBranch, reasonCode, null);
    }
}
