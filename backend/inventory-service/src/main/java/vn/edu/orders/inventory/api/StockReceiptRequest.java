package vn.edu.orders.inventory.api;

import com.fasterxml.jackson.annotation.JsonAlias;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.math.BigDecimal;

public record StockReceiptRequest(
        @NotBlank @JsonAlias("ingredient_id") String ingredientId,
        @NotNull @DecimalMin(value = "0.001") BigDecimal quantity
) {
}
