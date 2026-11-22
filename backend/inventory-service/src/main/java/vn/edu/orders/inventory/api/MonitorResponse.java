package vn.edu.orders.inventory.api;

import java.util.List;

public record MonitorResponse(
        String instanceId,
        boolean kafkaReachable,
        long slowConsumerThresholdSeconds,
        boolean slowConsumerWarning,
        List<TopicLag> topics,
        long needsCheckCount,
        long pendingOutboxCount,
        long alertsCount
) {
    public MonitorResponse {
        topics = topics == null ? List.of() : List.copyOf(topics);
    }

    public record TopicLag(
            String topic,
            int partitionCount,
            long totalUnread,
            Long oldestUnreadAgeSeconds,
            boolean slowWarning
    ) {
    }
}
