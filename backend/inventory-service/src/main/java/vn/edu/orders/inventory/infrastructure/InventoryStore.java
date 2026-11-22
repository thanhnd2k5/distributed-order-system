package vn.edu.orders.inventory.infrastructure;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import vn.edu.orders.contracts.events.AlertKind;
import vn.edu.orders.contracts.events.DishStatus;
import vn.edu.orders.contracts.events.EventType;
import vn.edu.orders.contracts.events.InventoryStatus;
import vn.edu.orders.contracts.events.MenuVersion;
import vn.edu.orders.contracts.events.ReservationOutcome;

@Repository
public class InventoryStore {
    private final JdbcTemplate jdbc;
    private final NamedParameterJdbcTemplate namedJdbc;

    private final String stockTable;
    private final String recipesTable;
    private final String ordersTable;
    private final String reservationsTable;
    private final String dishStatusTable;
    private final String menuVersionTable;
    private final String alertsTable;
    private final String processedEventsTable;
    private final String outboxEventsTable;

    private static final RowMapper<StockRow> STOCK_MAPPER = InventoryStore::mapStockRow;
    private static final RowMapper<InventoryOrderRow> ORDER_MAPPER = InventoryStore::mapOrderRow;
    private static final RowMapper<DishStatusRow> DISH_STATUS_MAPPER = InventoryStore::mapDishStatusRow;
    private static final RowMapper<MenuVersion> MENU_VERSION_MAPPER =
            (rs, rowNum) -> new MenuVersion(rs.getInt("epoch"), rs.getLong("seq"));

    public InventoryStore(JdbcTemplate jdbc, @Value("${spring.flyway.default-schema}") String schema) {
        this.jdbc = jdbc;
        this.namedJdbc = new NamedParameterJdbcTemplate(jdbc);
        String prefix = "\"" + schema.replace("\"", "\"\"") + "\".";
        this.stockTable = prefix + "stock";
        this.recipesTable = prefix + "recipes";
        this.ordersTable = prefix + "inventory_orders";
        this.reservationsTable = prefix + "inventory_reservations";
        this.dishStatusTable = prefix + "dish_status";
        this.menuVersionTable = prefix + "menu_version";
        this.alertsTable = prefix + "alerts";
        this.processedEventsTable = prefix + "processed_events";
        this.outboxEventsTable = prefix + "outbox_events";
    }

    public boolean tryInsertProcessedEvent(UUID eventId, UUID orderId, String eventType, OffsetDateTime processedAt) {
        String sql = """
                INSERT INTO %s (event_id, order_id, event_type, processed_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (event_id) DO NOTHING
                RETURNING event_id
                """.formatted(processedEventsTable);
        List<UUID> inserted = jdbc.query(
                sql,
                (rs, rowNum) -> rs.getObject(1, UUID.class),
                eventId,
                orderId,
                eventType,
                processedAt);
        return !inserted.isEmpty();
    }

    public boolean tryInsertOrder(
            UUID orderId,
            String originBranch,
            InventoryStatus initialStatus,
            String reason,
            OffsetDateTime orderCreatedAt,
            OffsetDateTime updatedAt
    ) {
        String sql = """
                INSERT INTO %s (order_id, origin_branch, status, reason, order_created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (order_id) DO NOTHING
                RETURNING order_id
                """.formatted(ordersTable);
        List<UUID> inserted = jdbc.query(
                sql,
                (rs, rowNum) -> rs.getObject(1, UUID.class),
                orderId,
                originBranch,
                initialStatus.name(),
                reason,
                orderCreatedAt,
                updatedAt);
        return !inserted.isEmpty();
    }

    public Optional<InventoryOrderRow> lockOrderForUpdate(UUID orderId) {
        String sql = """
                SELECT order_id, origin_branch, status, reason, order_created_at, updated_at
                FROM %s
                WHERE order_id = ?
                FOR UPDATE
                """.formatted(ordersTable);
        List<InventoryOrderRow> rows = jdbc.query(sql, ORDER_MAPPER, orderId);
        return rows.stream().findFirst();
    }

    public Optional<InventoryOrderRow> findOrder(UUID orderId) {
        String sql = """
                SELECT order_id, origin_branch, status, reason, order_created_at, updated_at
                FROM %s
                WHERE order_id = ?
                """.formatted(ordersTable);
        List<InventoryOrderRow> rows = jdbc.query(sql, ORDER_MAPPER, orderId);
        return rows.stream().findFirst();
    }

    public void markOrderFailed(UUID orderId, String reason, OffsetDateTime updatedAt) {
        String sql = """
                UPDATE %s
                SET status = 'FAILED',
                    reason = ?,
                    updated_at = ?
                WHERE order_id = ?
                """.formatted(ordersTable);
        int updated = jdbc.update(sql, reason, updatedAt, orderId);
        if (updated != 1) {
            throw new IllegalStateException("Expected 1 inventory_orders row for orderId=" + orderId + ", got " + updated);
        }
    }

    public boolean transitionOrderFromHeld(
            UUID orderId,
            InventoryStatus targetStatus,
            String reason,
            OffsetDateTime updatedAt
    ) {
        String sql = """
                UPDATE %s
                SET status = ?,
                    reason = ?,
                    updated_at = ?
                WHERE order_id = ?
                  AND status = 'HELD'
                """.formatted(ordersTable);
        return jdbc.update(sql, targetStatus.name(), reason, updatedAt, orderId) == 1;
    }

    public List<RecipeLine> findRecipesForDishes(Collection<String> dishIds) {
        if (dishIds == null || dishIds.isEmpty()) {
            return List.of();
        }
        String sql = """
                SELECT dish_id, ingredient_id, quantity
                FROM %s
                WHERE dish_id IN (:dishIds)
                ORDER BY dish_id ASC, ingredient_id ASC
                """.formatted(recipesTable);
        return namedJdbc.query(
                sql,
                new MapSqlParameterSource("dishIds", dishIds),
                (rs, rowNum) -> new RecipeLine(
                        rs.getString("dish_id"),
                        rs.getString("ingredient_id"),
                        rs.getBigDecimal("quantity")));
    }

    public List<String> findAffectedDishes(Collection<String> ingredientIds) {
        if (ingredientIds == null || ingredientIds.isEmpty()) {
            return List.of();
        }
        String sql = """
                SELECT DISTINCT dish_id
                FROM %s
                WHERE ingredient_id IN (:ingredientIds)
                ORDER BY dish_id ASC
                """.formatted(recipesTable);
        return namedJdbc.query(
                sql,
                new MapSqlParameterSource("ingredientIds", ingredientIds),
                (rs, rowNum) -> rs.getString("dish_id"));
    }

    public int reserveStockIfAvailableAndNotFlagged(String ingredientId, BigDecimal qty, OffsetDateTime now) {
        String sql = """
                UPDATE %s
                SET available = available - ?,
                    reserved = reserved + ?,
                    updated_at = ?
                WHERE ingredient_id = ?
                  AND available >= ?
                  AND NOT needs_check
                """.formatted(stockTable);
        return jdbc.update(sql, qty, qty, now, ingredientId, qty);
    }

    public Optional<StockRow> lockStockRow(String ingredientId) {
        String sql = """
                SELECT ingredient_id, name, unit, available, reserved, consumed, wasted, needs_check, updated_at
                FROM %s
                WHERE ingredient_id = ?
                FOR UPDATE
                """.formatted(stockTable);
        List<StockRow> rows = jdbc.query(sql, STOCK_MAPPER, ingredientId);
        return rows.stream().findFirst();
    }

    public List<StockRow> lockStockRowsInOrder(List<String> sortedIngredientIds) {
        if (sortedIngredientIds == null || sortedIngredientIds.isEmpty()) {
            return List.of();
        }
        String sql = """
                SELECT ingredient_id, name, unit, available, reserved, consumed, wasted, needs_check, updated_at
                FROM %s
                WHERE ingredient_id IN (:ingredientIds)
                ORDER BY ingredient_id ASC
                FOR UPDATE
                """.formatted(stockTable);
        return namedJdbc.query(sql, new MapSqlParameterSource("ingredientIds", sortedIngredientIds), STOCK_MAPPER);
    }

    public List<StockRow> findAllStockOrdered(Boolean needsCheckFilter) {
        StringBuilder sql = new StringBuilder("""
                SELECT ingredient_id, name, unit, available, reserved, consumed, wasted, needs_check, updated_at
                FROM %s
                """.formatted(stockTable));
        if (needsCheckFilter != null) {
            sql.append(" WHERE needs_check = ? ");
        }
        sql.append(" ORDER BY ingredient_id ASC ");
        if (needsCheckFilter != null) {
            return jdbc.query(sql.toString(), STOCK_MAPPER, needsCheckFilter);
        }
        return jdbc.query(sql.toString(), STOCK_MAPPER);
    }

    public int consumeReservedStock(String ingredientId, BigDecimal qty, OffsetDateTime now) {
        String sql = """
                UPDATE %s
                SET reserved = reserved - ?,
                    consumed = consumed + ?,
                    updated_at = ?
                WHERE ingredient_id = ?
                  AND reserved >= ?
                """.formatted(stockTable);
        return jdbc.update(sql, qty, qty, now, ingredientId, qty);
    }

    public int releaseReservedStock(String ingredientId, BigDecimal qty, OffsetDateTime now) {
        String sql = """
                UPDATE %s
                SET available = available + ?,
                    reserved = reserved - ?,
                    updated_at = ?
                WHERE ingredient_id = ?
                  AND reserved >= ?
                """.formatted(stockTable);
        return jdbc.update(sql, qty, qty, now, ingredientId, qty);
    }

    public int wasteAndFlagStock(String ingredientId, BigDecimal qty, OffsetDateTime now) {
        String sql = """
                UPDATE %s
                SET reserved = reserved - ?,
                    wasted = wasted + ?,
                    needs_check = TRUE,
                    updated_at = ?
                WHERE ingredient_id = ?
                  AND reserved >= ?
                """.formatted(stockTable);
        return jdbc.update(sql, qty, qty, now, ingredientId, qty);
    }

    public Optional<StockRow> clearCheck(String ingredientId, BigDecimal newAvailableOrNull, OffsetDateTime now) {
        if (newAvailableOrNull != null) {
            String sql = """
                    UPDATE %s
                    SET available = ?,
                        needs_check = FALSE,
                        updated_at = ?
                    WHERE ingredient_id = ?
                    RETURNING ingredient_id, name, unit, available, reserved, consumed, wasted, needs_check, updated_at
                    """.formatted(stockTable);
            List<StockRow> rows = jdbc.query(sql, STOCK_MAPPER, newAvailableOrNull, now, ingredientId);
            return rows.stream().findFirst();
        }
        String sql = """
                UPDATE %s
                SET needs_check = FALSE,
                    updated_at = ?
                WHERE ingredient_id = ?
                RETURNING ingredient_id, name, unit, available, reserved, consumed, wasted, needs_check, updated_at
                """.formatted(stockTable);
        List<StockRow> rows = jdbc.query(sql, STOCK_MAPPER, now, ingredientId);
        return rows.stream().findFirst();
    }

    public Optional<StockRow> receiveStock(String ingredientId, BigDecimal quantity, OffsetDateTime now) {
        String sql = """
                UPDATE %s
                SET available = available + ?,
                    updated_at = ?
                WHERE ingredient_id = ?
                RETURNING ingredient_id, name, unit, available, reserved, consumed, wasted, needs_check, updated_at
                """.formatted(stockTable);
        List<StockRow> rows = jdbc.query(sql, STOCK_MAPPER, quantity, now, ingredientId);
        return rows.stream().findFirst();
    }

    public void insertReservation(UUID orderId, String ingredientId, BigDecimal quantity) {
        String sql = """
                INSERT INTO %s (order_id, ingredient_id, quantity, outcome)
                VALUES (?, ?, ?, NULL)
                """.formatted(reservationsTable);
        jdbc.update(sql, orderId, ingredientId, quantity);
    }

    public List<ReservationLine> findHeldReservationsInOrder(UUID orderId) {
        String sql = """
                SELECT order_id, ingredient_id, quantity, outcome
                FROM %s
                WHERE order_id = ?
                  AND outcome IS NULL
                ORDER BY ingredient_id ASC
                """.formatted(reservationsTable);
        return jdbc.query(
                sql,
                (rs, rowNum) -> new ReservationLine(
                        rs.getObject("order_id", UUID.class),
                        rs.getString("ingredient_id"),
                        rs.getBigDecimal("quantity"),
                        null),
                orderId);
    }

    public int markAllHeldReservationsOutcome(UUID orderId, ReservationOutcome outcome) {
        String sql = """
                UPDATE %s
                SET outcome = ?
                WHERE order_id = ?
                  AND outcome IS NULL
                """.formatted(reservationsTable);
        return jdbc.update(sql, outcome.name(), orderId);
    }

    public int markReservationOutcome(UUID orderId, String ingredientId, ReservationOutcome outcome) {
        String sql = """
                UPDATE %s
                SET outcome = ?
                WHERE order_id = ?
                  AND ingredient_id = ?
                  AND outcome IS NULL
                """.formatted(reservationsTable);
        return jdbc.update(sql, outcome.name(), orderId, ingredientId);
    }

    public List<DishStatusRow> lockDishStatusesInOrder(Collection<String> dishIds) {
        if (dishIds == null) {
            String sql = """
                    SELECT dish_id, dish_name, status, epoch, seq
                    FROM %s
                    ORDER BY dish_id ASC
                    FOR UPDATE
                    """.formatted(dishStatusTable);
            return jdbc.query(sql, DISH_STATUS_MAPPER);
        }
        if (dishIds.isEmpty()) {
            return List.of();
        }
        String sql = """
                SELECT dish_id, dish_name, status, epoch, seq
                FROM %s
                WHERE dish_id IN (:dishIds)
                ORDER BY dish_id ASC
                FOR UPDATE
                """.formatted(dishStatusTable);
        return namedJdbc.query(sql, new MapSqlParameterSource("dishIds", dishIds), DISH_STATUS_MAPPER);
    }

    public List<DishStatusRow> findAllDishStatusesOrdered() {
        String sql = """
                SELECT dish_id, dish_name, status, epoch, seq
                FROM %s
                ORDER BY dish_id ASC
                """.formatted(dishStatusTable);
        return jdbc.query(sql, DISH_STATUS_MAPPER);
    }

    /** Plain SELECT (no FOR UPDATE) executed strictly AFTER locking dish_status rows. */
    public List<RecipeStockView> readRecipesWithStockForDishes(Collection<String> dishIds) {
        if (dishIds == null || dishIds.isEmpty()) {
            return List.of();
        }
        String sql = """
                SELECT r.dish_id, r.ingredient_id, r.quantity AS required_quantity,
                       s.available, s.needs_check
                FROM %s r
                JOIN %s s ON s.ingredient_id = r.ingredient_id
                WHERE r.dish_id IN (:dishIds)
                ORDER BY r.dish_id ASC, r.ingredient_id ASC
                """.formatted(recipesTable, stockTable);
        return namedJdbc.query(
                sql,
                new MapSqlParameterSource("dishIds", dishIds),
                (rs, rowNum) -> new RecipeStockView(
                        rs.getString("dish_id"),
                        rs.getString("ingredient_id"),
                        rs.getBigDecimal("required_quantity"),
                        rs.getBigDecimal("available"),
                        rs.getBoolean("needs_check")));
    }

    public MenuVersion incrementSeq() {
        String sql = """
                UPDATE %s
                SET seq = seq + 1
                WHERE id = TRUE
                RETURNING epoch, seq
                """.formatted(menuVersionTable);
        List<MenuVersion> rows = jdbc.query(sql, MENU_VERSION_MAPPER);
        if (rows.isEmpty()) {
            throw new IllegalStateException("menu_version row missing");
        }
        return rows.getFirst();
    }

    public MenuVersion incrementEpochAndSeq() {
        String sql = """
                UPDATE %s
                SET epoch = epoch + 1,
                    seq = seq + 1
                WHERE id = TRUE
                RETURNING epoch, seq
                """.formatted(menuVersionTable);
        List<MenuVersion> rows = jdbc.query(sql, MENU_VERSION_MAPPER);
        if (rows.isEmpty()) {
            throw new IllegalStateException("menu_version row missing");
        }
        return rows.getFirst();
    }

    public MenuVersion readMenuVersion() {
        String sql = "SELECT epoch, seq FROM %s WHERE id = TRUE".formatted(menuVersionTable);
        List<MenuVersion> rows = jdbc.query(sql, MENU_VERSION_MAPPER);
        if (rows.isEmpty()) {
            throw new IllegalStateException("menu_version row missing");
        }
        return rows.getFirst();
    }

    public void updateDishStatus(String dishId, DishStatus status, int epoch, long seq) {
        String sql = """
                UPDATE %s
                SET status = ?,
                    epoch = ?,
                    seq = ?
                WHERE dish_id = ?
                """.formatted(dishStatusTable);
        jdbc.update(sql, status.name(), epoch, seq, dishId);
    }

    public void ensureDishStatusRowExists(String dishId, String dishName, int epoch, long seq) {
        String sql = """
                INSERT INTO %s (dish_id, dish_name, status, epoch, seq)
                VALUES (?, ?, 'HIDDEN', ?, ?)
                ON CONFLICT (dish_id) DO NOTHING
                """.formatted(dishStatusTable);
        jdbc.update(sql, dishId, dishName == null ? "" : dishName, epoch, seq);
    }

    public void deleteRecipesForDish(String dishId) {
        String sql = "DELETE FROM %s WHERE dish_id = ?".formatted(recipesTable);
        jdbc.update(sql, dishId);
    }

    public void insertRecipeLine(String dishId, String ingredientId, BigDecimal quantity) {
        String sql = """
                INSERT INTO %s (dish_id, ingredient_id, quantity)
                VALUES (?, ?, ?)
                """.formatted(recipesTable);
        jdbc.update(sql, dishId, ingredientId, quantity);
    }

    public void insertAlertIdempotent(
            AlertKind kind,
            UUID orderId,
            UUID eventId,
            String sourceTopic,
            int sourcePartition,
            long sourceOffset,
            String detailJson,
            OffsetDateTime createdAt
    ) {
        String sql = """
                INSERT INTO %s (kind, order_id, event_id, source_topic, source_partition, source_offset, detail, created_at)
                VALUES (?, ?, ?, ?, ?, ?, CAST(? AS jsonb), ?)
                ON CONFLICT (kind, source_topic, source_partition, source_offset) DO NOTHING
                """.formatted(alertsTable);
        jdbc.update(
                sql,
                kind.name(),
                orderId,
                eventId,
                sourceTopic,
                sourcePartition,
                sourceOffset,
                detailJson == null ? "{}" : detailJson,
                createdAt);
    }

    public void insertOutboxEvent(
            UUID eventId,
            UUID aggregateId,
            EventType eventType,
            String topic,
            String messageKey,
            String payloadJson,
            OffsetDateTime now
    ) {
        String sql = """
                INSERT INTO %s (
                    event_id, aggregate_id, event_type, topic, message_key, payload, created_at, next_attempt_at
                )
                VALUES (?, ?, ?, ?, ?, CAST(? AS jsonb), ?, clock_timestamp())
                """.formatted(outboxEventsTable);
        jdbc.update(
                sql,
                eventId,
                aggregateId,
                eventType.name(),
                topic,
                messageKey,
                payloadJson,
                now);
    }

    private static StockRow mapStockRow(ResultSet rs, int rowNum) throws SQLException {
        return new StockRow(
                rs.getString("ingredient_id"),
                rs.getString("name"),
                rs.getString("unit"),
                rs.getBigDecimal("available"),
                rs.getBigDecimal("reserved"),
                rs.getBigDecimal("consumed"),
                rs.getBigDecimal("wasted"),
                rs.getBoolean("needs_check"),
                rs.getObject("updated_at", OffsetDateTime.class));
    }

    private static InventoryOrderRow mapOrderRow(ResultSet rs, int rowNum) throws SQLException {
        return new InventoryOrderRow(
                rs.getObject("order_id", UUID.class),
                rs.getString("origin_branch"),
                InventoryStatus.valueOf(rs.getString("status")),
                rs.getString("reason"),
                rs.getObject("order_created_at", OffsetDateTime.class),
                rs.getObject("updated_at", OffsetDateTime.class));
    }

    private static DishStatusRow mapDishStatusRow(ResultSet rs, int rowNum) throws SQLException {
        return new DishStatusRow(
                rs.getString("dish_id"),
                rs.getString("dish_name"),
                DishStatus.valueOf(rs.getString("status")),
                rs.getInt("epoch"),
                rs.getLong("seq"));
    }

    public record StockRow(
            String ingredientId,
            String name,
            String unit,
            BigDecimal available,
            BigDecimal reserved,
            BigDecimal consumed,
            BigDecimal wasted,
            boolean needsCheck,
            OffsetDateTime updatedAt
    ) {
    }

    public record InventoryOrderRow(
            UUID orderId,
            String originBranch,
            InventoryStatus status,
            String reason,
            OffsetDateTime orderCreatedAt,
            OffsetDateTime updatedAt
    ) {
    }

    public record RecipeLine(String dishId, String ingredientId, BigDecimal quantity) {
    }

    public record ReservationLine(
            UUID orderId,
            String ingredientId,
            BigDecimal quantity,
            ReservationOutcome outcome
    ) {
    }

    public record DishStatusRow(
            String dishId,
            String dishName,
            DishStatus status,
            int epoch,
            long seq
    ) {
    }

    public record RecipeStockView(
            String dishId,
            String ingredientId,
            BigDecimal requiredQuantity,
            BigDecimal available,
            boolean needsCheck
    ) {
    }
}
