package vn.edu.orders.query.api;

import java.time.Instant;
import java.util.Map;

public record BranchSummary(
        String branchId,
        Instant dataAsOf,
        long totalOrders,
        Map<String, Long> byStatus,
        Map<String, Long> cancelledByReason,
        Map<String, Long> failedByReason
) {
}
