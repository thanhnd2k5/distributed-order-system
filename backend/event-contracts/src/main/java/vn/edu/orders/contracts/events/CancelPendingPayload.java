package vn.edu.orders.contracts.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public record CancelPendingPayload() {
    public static final CancelPendingPayload INSTANCE = new CancelPendingPayload();

    public static CancelPendingPayload empty() {
        return INSTANCE;
    }
}
