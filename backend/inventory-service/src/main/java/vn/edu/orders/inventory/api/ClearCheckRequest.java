package vn.edu.orders.inventory.api;

import com.fasterxml.jackson.annotation.JsonAlias;
import java.math.BigDecimal;

public record ClearCheckRequest(
        @JsonAlias("counted_on_hand") BigDecimal countedOnHand
) {
}
