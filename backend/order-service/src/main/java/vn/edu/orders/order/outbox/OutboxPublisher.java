package vn.edu.orders.order.outbox;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "outbox.enabled", havingValue = "true")
class OutboxPublisher {
    private final OutboxRelayService relay;
    private final KafkaConnectivityProbe probe;

    OutboxPublisher(OutboxRelayService relay, KafkaConnectivityProbe probe) {
        this.relay = relay;
        this.probe = probe;
    }

    @Scheduled(fixedDelay = 500)
    void publishPending() {
        relay.publishPendingOnce();
    }

    @Scheduled(fixedDelayString = "${order.probe-interval-ms:5000}")
    void refreshProbe() {
        probe.probe();
    }
}
