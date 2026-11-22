package vn.edu.orders.order.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "order")
public record OrderLifecycleProperties(
        Duration staleThreshold,
        Duration pendingInventoryAlertAfter,
        Duration expiryThreshold,
        Duration expiryWarningBefore,
        int dlqMaxAttempts,
        long probeTimeoutMs,
        long sendTimeoutMs,
        String inventoryBaseUrl,
        long heartbeatIntervalMs,
        boolean heartbeatEnabled,
        String cancelMessageCooking
) {
    public OrderLifecycleProperties {
        if (staleThreshold == null) {
            staleThreshold = Duration.ofMinutes(5);
        }
        if (pendingInventoryAlertAfter == null) {
            pendingInventoryAlertAfter = Duration.ofMinutes(10);
        }
        if (expiryThreshold == null) {
            expiryThreshold = Duration.ofHours(4);
        }
        if (expiryWarningBefore == null) {
            expiryWarningBefore = Duration.ofHours(1);
        }
        if (dlqMaxAttempts <= 0) {
            dlqMaxAttempts = 5;
        }
        if (probeTimeoutMs <= 0) {
            probeTimeoutMs = 1500L;
        }
        if (sendTimeoutMs <= 0) {
            sendTimeoutMs = 6000L;
        }
        if (inventoryBaseUrl == null || inventoryBaseUrl.isBlank()) {
            inventoryBaseUrl = "http://localhost:8084";
        }
        if (heartbeatIntervalMs <= 0) {
            heartbeatIntervalMs = 5000L;
        }
        if (cancelMessageCooking == null || cancelMessageCooking.isBlank()) {
            cancelMessageCooking = "Cannot cancel: the kitchen has started cooking";
        }
    }
}
