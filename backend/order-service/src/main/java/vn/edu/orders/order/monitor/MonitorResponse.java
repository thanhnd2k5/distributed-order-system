package vn.edu.orders.order.monitor;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record MonitorResponse(
        String branchId,
        boolean kafkaConnected,
        Instant lastProbeAt,
        String lastProbeError,
        long pendingOutboxCount,
        List<PendingInventoryAlert> pendingInventoryAlerts,
        List<ExpiringUnsentAlert> expiringUnsentAlerts,
        List<ActiveCancelRequest> activeCancelRequests,
        List<ConsumerAlert> alerts
) {
    public record PendingInventoryAlert(
            UUID orderId,
            Instant createdAt,
            long waitingSeconds
    ) {
    }

    public record ExpiringUnsentAlert(
            UUID orderId,
            long outboxId,
            String kind,
            Instant createdAt,
            Instant expiresAt,
            long remainingSeconds
    ) {
    }

    public record ActiveCancelRequest(
            UUID orderId,
            String status,
            String cancelFlag,
            Instant cancelRequestedAt,
            long waitedSeconds
    ) {
    }

    public record ConsumerAlert(
            long id,
            String kind,
            UUID orderId,
            UUID eventId,
            String sourceTopic,
            int sourcePartition,
            long sourceOffset,
            String detail,
            Instant createdAt
    ) {
    }
}
