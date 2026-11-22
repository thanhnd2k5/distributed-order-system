package vn.edu.orders.contracts.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.time.Instant;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public record OrderCreatedPayload(
        Instant createdAt,
        SourceType channel,
        List<OrderItemLine> items
) {
    public OrderCreatedPayload {
        items = items == null ? null : List.copyOf(items);
    }
}
