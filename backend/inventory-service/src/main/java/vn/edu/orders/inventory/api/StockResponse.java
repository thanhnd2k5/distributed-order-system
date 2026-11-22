package vn.edu.orders.inventory.api;

import java.math.BigDecimal;
import java.time.Instant;
import vn.edu.orders.inventory.domain.InventoryQuantityUtils;
import vn.edu.orders.inventory.infrastructure.InventoryStore.StockRow;

public record StockResponse(
        String ingredientId,
        String name,
        String unit,
        BigDecimal available,
        BigDecimal reserved,
        BigDecimal consumed,
        BigDecimal wasted,
        boolean needsCheck,
        Instant updatedAt
) {
    public static StockResponse fromRow(StockRow row) {
        return new StockResponse(
                row.ingredientId(),
                row.name(),
                row.unit(),
                InventoryQuantityUtils.normalize(row.available()),
                InventoryQuantityUtils.normalize(row.reserved()),
                InventoryQuantityUtils.normalize(row.consumed()),
                InventoryQuantityUtils.normalize(row.wasted()),
                row.needsCheck(),
                row.updatedAt().toInstant());
    }
}
