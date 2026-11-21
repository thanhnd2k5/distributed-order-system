package vn.edu.orders.inventory.infrastructure;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import vn.edu.orders.inventory.application.DishStatusService;

@Component
@EnableScheduling
@ConditionalOnProperty(name = "inventory.heartbeat-enabled", havingValue = "true")
public class HeartbeatScheduler {
    private static final Logger log = LoggerFactory.getLogger(HeartbeatScheduler.class);

    private final DishStatusService dishStatusService;

    public HeartbeatScheduler(DishStatusService dishStatusService) {
        this.dishStatusService = dishStatusService;
    }

    @Scheduled(fixedDelayString = "${inventory.heartbeat-interval-ms:60000}")
    public void emitPeriodicHeartbeat() {
        try {
            dishStatusService.emitHeartbeat();
        } catch (RuntimeException ex) {
            log.warn("Failed emitting periodic inventory heartbeat: {}", ex.toString());
        }
    }
}
