package vn.edu.orders.order.orders;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "order.heartbeat-enabled", havingValue = "true", matchIfMissing = true)
class ReplicationHeartbeatScheduler {
    private final ReplicationHeartbeatService heartbeatService;

    ReplicationHeartbeatScheduler(ReplicationHeartbeatService heartbeatService) {
        this.heartbeatService = heartbeatService;
    }

    @Scheduled(fixedDelayString = "${order.heartbeat-interval-ms:5000}")
    void scheduledBeat() {
        heartbeatService.beat();
    }
}
