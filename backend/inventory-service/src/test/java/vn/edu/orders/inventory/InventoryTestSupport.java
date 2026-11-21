package vn.edu.orders.inventory;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.assertj.core.api.Assertions.assertThat;

public final class InventoryTestSupport {
    private InventoryTestSupport() {
    }

    @TestConfiguration
    public static class MutableClockConfig {
        @Bean
        @Primary
        public MutableClock mutableClock() {
            return new MutableClock(Instant.parse("2026-11-22T10:00:00Z"));
        }
    }

    public static final class MutableClock extends Clock {
        private final AtomicReference<Instant> current;
        private final ZoneId zone;

        public MutableClock(Instant initial) {
            this(initial, ZoneOffset.UTC);
        }

        private MutableClock(Instant initial, ZoneId zone) {
            this.current = new AtomicReference<>(initial);
            this.zone = zone;
        }

        public void setInstant(Instant instant) {
            current.set(instant);
        }

        public void advance(Duration duration) {
            current.updateAndGet(now -> now.plus(duration));
        }

        @Override
        public ZoneId getZone() {
            return zone;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return new MutableClock(current.get(), zone);
        }

        @Override
        public Instant instant() {
            return current.get();
        }
    }

    public static void resetSchema(JdbcTemplate jdbc, String schema) {
        String s = "\"" + schema + "\".";
        jdbc.execute("""
                BEGIN;
                DELETE FROM %soutbox_events;
                DELETE FROM %salerts;
                DELETE FROM %sinventory_reservations;
                DELETE FROM %sinventory_orders;
                DELETE FROM %sprocessed_events;
                DELETE FROM %srecipes;
                DELETE FROM %sdish_status;
                DELETE FROM %sstock;
                INSERT INTO %sstock (ingredient_id, name, unit, available, reserved, consumed, wasted, needs_check, updated_at) VALUES
                ('GAO', 'Gạo', 'gram', 1000.000, 0, 0, 0, FALSE, now()),
                ('THIT_GA', 'Thịt gà', 'gram', 1500.000, 0, 0, 0, FALSE, now()),
                ('BANH_PHO', 'Bánh phở', 'gram', 1000.000, 0, 0, 0, FALSE, now()),
                ('THIT_BO', 'Thịt bò', 'gram', 1000.000, 0, 0, 0, FALSE, now()),
                ('HANH_LA', 'Hành lá', 'gram', 500.000, 0, 0, 0, FALSE, now());
                INSERT INTO %srecipes (dish_id, ingredient_id, quantity) VALUES
                ('COM_GA', 'GAO', 100.000),
                ('COM_GA', 'THIT_GA', 150.000),
                ('CHAO_GA', 'GAO', 50.000),
                ('CHAO_GA', 'THIT_GA', 100.000),
                ('PHO_BO', 'BANH_PHO', 150.000),
                ('PHO_BO', 'THIT_BO', 150.000),
                ('PHO_BO', 'HANH_LA', 20.000);
                UPDATE %smenu_version SET epoch = 1, seq = 1 WHERE id = TRUE;
                INSERT INTO %sdish_status (dish_id, dish_name, status, epoch, seq) VALUES
                ('CHAO_GA', 'Cháo gà', 'AVAILABLE', 1, 1),
                ('COM_GA', 'Cơm gà', 'AVAILABLE', 1, 1),
                ('PHO_BO', 'Phở bò', 'AVAILABLE', 1, 1);
                COMMIT;
                """.formatted(s, s, s, s, s, s, s, s, s, s, s, s));
    }

    /**
     * Runs BOTH mandatory inventory invariant queries from docs/02-detailed-design.md §3 and 01 §3.3:
     * 1) Non-negative stock & stock.reserved == SUM(inventory_reservations.quantity WHERE outcome IS NULL).
     * 2) Held consistency: outcome IS NULL iff inventory_orders.status == 'HELD'.
     */
    public static void assertInventoryInvariants(JdbcTemplate jdbc, String schema) {
        String s = "\"" + schema + "\".";
        List<Map<String, Object>> stockViolations = jdbc.queryForList("""
                SELECT s.ingredient_id, s.available, s.reserved, s.consumed, s.wasted,
                       COALESCE(SUM(ir.quantity), 0) AS held_sum
                FROM %sstock s
                LEFT JOIN %sinventory_reservations ir
                       ON ir.ingredient_id = s.ingredient_id
                      AND ir.outcome IS NULL
                GROUP BY s.ingredient_id, s.available, s.reserved, s.consumed, s.wasted
                HAVING s.available < 0
                    OR s.reserved < 0
                    OR s.consumed < 0
                    OR s.wasted < 0
                    OR s.reserved <> COALESCE(SUM(ir.quantity), 0)
                """.formatted(s, s));
        assertThat(stockViolations)
                .withFailMessage("Stock invariant violated: %s", stockViolations)
                .isEmpty();

        List<Map<String, Object>> heldStateViolations = jdbc.queryForList("""
                SELECT ir.order_id, ir.ingredient_id, ir.outcome, io.status
                FROM %sinventory_reservations ir
                JOIN %sinventory_orders io ON io.order_id = ir.order_id
                WHERE (ir.outcome IS NULL AND io.status <> 'HELD')
                   OR (ir.outcome IS NOT NULL AND io.status = 'HELD')
                """.formatted(s, s));
        assertThat(heldStateViolations)
                .withFailMessage("Reservation outcome vs order status invariant violated: %s", heldStateViolations)
                .isEmpty();
    }

    public static BigDecimal bd(String value) {
        return new BigDecimal(value);
    }
}
