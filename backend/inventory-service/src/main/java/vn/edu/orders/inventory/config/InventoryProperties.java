package vn.edu.orders.inventory.config;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "inventory")
public record InventoryProperties(
        @NotBlank @Pattern(regexp = "[a-zA-Z0-9._-]{1,64}") String instanceId,
        @NotNull Duration orderExpiry,
        @NotNull Duration futureTolerance,
        @NotNull Duration slowConsumerThreshold,
        @Min(1) int maxPoisonAttempts,
        @Min(0) long poisonBackoffMs
) {
    public InventoryProperties {
        if (orderExpiry == null) {
            orderExpiry = Duration.ofHours(4);
        }
        if (futureTolerance == null) {
            futureTolerance = Duration.ofMinutes(5);
        }
        if (slowConsumerThreshold == null) {
            slowConsumerThreshold = Duration.ofMinutes(5);
        }
        if (maxPoisonAttempts <= 0) {
            maxPoisonAttempts = 5;
        }
        if (poisonBackoffMs < 0) {
            poisonBackoffMs = 50L;
        }
    }
}
