package vn.edu.orders.query.api;

import java.time.Instant;
import java.util.List;

public record ReportingOrderDetailsResponse(
        String replicaNote,
        Instant queriedAt,
        ReplicatedOrderSummary order,
        List<ReplicatedOrderItem> items
) {
}
