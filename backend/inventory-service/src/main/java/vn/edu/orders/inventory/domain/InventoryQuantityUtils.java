package vn.edu.orders.inventory.domain;

import java.math.BigDecimal;

public final class InventoryQuantityUtils {
    private InventoryQuantityUtils() {
    }

    /** Normalizes NUMERIC(12,3) values cleanly for JSON serialization without scientific notation. */
    public static BigDecimal normalize(BigDecimal value) {
        if (value == null) {
            return null;
        }
        if (value.compareTo(BigDecimal.ZERO) == 0) {
            return BigDecimal.ZERO;
        }
        BigDecimal stripped = value.stripTrailingZeros();
        return stripped.scale() < 0 ? stripped.setScale(0) : stripped;
    }
}
