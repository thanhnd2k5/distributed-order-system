package vn.edu.orders.contracts.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public record OrderCompletedPayload() {
    public static final OrderCompletedPayload INSTANCE = new OrderCompletedPayload();

    public static OrderCompletedPayload empty() {
        return INSTANCE;
    }
}
