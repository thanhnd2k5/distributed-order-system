package vn.edu.orders.query.api;

import java.time.Instant;

public record SubscriptionStatus(
        String subscriptionName,
        boolean enabled,
        Integer workerPid,
        Instant lastMessageSendTime,
        Instant lastMessageReceiptTime,
        Instant latestEndTime
) {
}
