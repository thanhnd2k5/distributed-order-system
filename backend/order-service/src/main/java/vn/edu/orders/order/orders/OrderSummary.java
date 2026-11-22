package vn.edu.orders.order.orders;

import java.time.Instant;
import java.util.UUID;

public record OrderSummary(
        UUID orderId,
        String sourceSiteId,
        String status,
        int statusRank,
        String failureReason,
        String closeReason,
        String cancelFlag,
        Instant cancelRequestedAt,
        String cancelMessage,
        String deliveryStatus,
        Instant createdAt,
        Instant updatedAt
) {
}
