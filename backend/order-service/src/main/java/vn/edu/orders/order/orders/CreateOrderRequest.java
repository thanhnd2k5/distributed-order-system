package vn.edu.orders.order.orders;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;
import java.util.List;
import java.util.UUID;

public record CreateOrderRequest(
        @NotNull UUID clientRequestId,
        @NotEmpty @Size(max = 100) List<@NotNull @Valid Item> items
) {
    public record Item(@NotBlank @Size(max = 50) String dishId, @NotNull @Positive Integer quantity) {
    }
}
