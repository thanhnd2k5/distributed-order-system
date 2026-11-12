package vn.edu.orders.query.api;

import java.time.Instant;
import java.util.UUID;

public record ReplicatedOrderSummary(
        UUID orderId,
        String sourceSiteId,
        String sourceType,
        String status,
        String failureReason,
        long version,
        Instant createdAt,
        Instant updatedAt
) {
}
