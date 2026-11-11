package vn.edu.orders.order.config;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;
import vn.edu.orders.contracts.events.SourceType;

@Validated
@ConfigurationProperties(prefix = "site")
public record SiteProperties(
        @NotBlank @Pattern(regexp = "[A-Z][A-Z0-9_]{0,49}") String id,
        @NotNull SourceType sourceType
) {
}
