package vn.edu.orders.inventory.config;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "inventory")
public record InventoryProperties(
        @NotBlank @Pattern(regexp = "[a-zA-Z0-9._-]{1,64}") String instanceId
) {
}
