package vn.edu.orders.kitchen.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "kitchen")
public record KitchenProperties(String instanceId) {
    public KitchenProperties {
        if (instanceId == null || instanceId.isBlank()) {
            instanceId = "kitchen-1";
        }
    }
}
