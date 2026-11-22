package vn.edu.orders.query.api;

import java.time.Instant;
import java.util.UUID;

public record ReplicatedOrderSummary(
        UUID orderId,
        String sourceSiteId,
        String sourceType,
        String status,
        int statusRank,
        String failureReason,
        String closeReason,
        String cancelFlag,
        Instant cancelRequestedAt,
        String cancelMessage,
        long version,
        Instant createdAt,
        Instant updatedAt
) {
}
