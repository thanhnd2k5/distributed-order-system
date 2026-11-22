package vn.edu.orders.kitchen.api;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public record KitchenAlertResponse(
        long id,
        String kind,
        UUID orderId,
        UUID eventId,
        String sourceTopic,
        int sourcePartition,
        long sourceOffset,
        Map<String, Object> detail,
        Instant createdAt
) {
    public KitchenAlertResponse {
        detail = detail == null ? Map.of() : Map.copyOf(detail);
    }
}
