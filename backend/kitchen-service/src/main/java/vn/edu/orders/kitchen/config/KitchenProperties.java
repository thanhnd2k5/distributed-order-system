package vn.edu.orders.kitchen.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "kitchen")
public record KitchenProperties(
        String instanceId,
        long slowConsumerThresholdSeconds,
        long monitorAdminTimeoutMs
) {
    public KitchenProperties {
        if (instanceId == null || instanceId.isBlank()) {
            instanceId = "kitchen-1";
        }
        if (slowConsumerThresholdSeconds <= 0) {
            slowConsumerThresholdSeconds = 300L;
        }
        if (monitorAdminTimeoutMs <= 0) {
            monitorAdminTimeoutMs = 2000L;
        }
    }
}
