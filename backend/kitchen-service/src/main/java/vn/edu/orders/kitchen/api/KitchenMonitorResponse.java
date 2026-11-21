package vn.edu.orders.kitchen.api;

import java.time.Instant;
import java.util.List;

public record KitchenMonitorResponse(
        String instanceId,
        String consumerGroup,
        boolean kafkaConnected,
        String error,
        long slowConsumerThresholdSeconds,
        boolean anySlowConsumerWarning,
        long pendingOutboxCount,
        List<TopicLagStatus> topics,
        Instant checkedAt
) {
    public KitchenMonitorResponse {
        topics = topics == null ? List.of() : List.copyOf(topics);
    }

    public record TopicLagStatus(
            String topic,
            long unreadCount,
            Instant oldestUnreadTimestamp,
            long oldestUnreadAgeSeconds,
            boolean slowConsumerWarning
    ) {
    }
}
