package vn.edu.orders.query.api;

import java.time.Instant;
import java.util.List;
import java.util.Map;

public record ReportingSummaryResponse(
        String replicaNote,
        Instant queriedAt,
        long totalOrders,
        Map<String, Long> bySite,
        Map<String, Long> byStatus,
        Map<String, Long> byCancelledReason,
        Map<String, Long> byFailedReason,
        List<BranchSummary> branches,
        List<SubscriptionStatus> subscriptions
) {
}
