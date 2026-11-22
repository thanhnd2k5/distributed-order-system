package vn.edu.orders.contracts.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public record StockConsumedPayload() {
    public static final StockConsumedPayload INSTANCE = new StockConsumedPayload();

    public static StockConsumedPayload empty() {
        return INSTANCE;
    }
}
