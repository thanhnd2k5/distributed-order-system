package vn.edu.orders.query.api;

import java.time.Instant;
import java.util.List;

public record ReportingOrdersResponse(
        String replicaNote,
        Instant queriedAt,
        int count,
        List<ReplicatedOrderSummary> orders
) {
}
