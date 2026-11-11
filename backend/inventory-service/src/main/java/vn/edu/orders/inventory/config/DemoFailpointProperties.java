package vn.edu.orders.inventory.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "demo.failpoint")
public record DemoFailpointProperties(
        boolean haltAfterCommitOnce,
        String markerFile
) {
}
