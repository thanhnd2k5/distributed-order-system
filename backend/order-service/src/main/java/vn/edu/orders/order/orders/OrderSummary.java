package vn.edu.orders.order.orders;

import java.time.Instant;
import java.util.UUID;

public record OrderSummary(
        UUID orderId, String sourceSiteId, String status, String failureReason, Instant createdAt
) {
}
