package vn.edu.orders.inventory.infrastructure;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import vn.edu.orders.contracts.events.AlertKind;
import vn.edu.orders.contracts.events.InventoryStatus;
import vn.edu.orders.contracts.events.ReservationOutcome;
import vn.edu.orders.inventory.api.AlertResponse;
import vn.edu.orders.inventory.api.IngredientResponse;
import vn.edu.orders.inventory.api.RecipeResponse;
import vn.edu.orders.inventory.api.ReservationResponse;
import vn.edu.orders.inventory.api.StockResponse;
import vn.edu.orders.inventory.domain.InventoryQuantityUtils;

@Repository
public class InventoryQueryStore {
    private final JdbcTemplate jdbc;
    private final ObjectMapper json;
    private final String stockTable;
    private final String recipesTable;
    private final String ordersTable;
    private final String reservationsTable;
    private final String alertsTable;
    private final String outboxTable;

    public InventoryQueryStore(
            JdbcTemplate jdbc,
            ObjectMapper json,
            @Value("${spring.flyway.default-schema}") String schema
    ) {
        this.jdbc = jdbc;
        this.json = json;
        String prefix = "\"" + schema.replace("\"", "\"\"") + "\".";
        this.stockTable = prefix + "stock";
        this.recipesTable = prefix + "recipes";
        this.ordersTable = prefix + "inventory_orders";
        this.reservationsTable = prefix + "inventory_reservations";
        this.alertsTable = prefix + "alerts";
        this.outboxTable = prefix + "outbox_events";
    }

    public List<StockResponse> findStock(Boolean needsCheck) {
        StringBuilder sql = new StringBuilder("""
                SELECT ingredient_id, name, unit, available, reserved, consumed, wasted, needs_check, updated_at
                FROM %s
                """.formatted(stockTable));
        if (needsCheck != null) {
            sql.append(" WHERE needs_check = ? ");
        }
        sql.append(" ORDER BY ingredient_id ASC ");

        if (needsCheck != null) {
            return jdbc.query(sql.toString(), InventoryQueryStore::mapStockResponse, needsCheck);
        }
        return jdbc.query(sql.toString(), InventoryQueryStore::mapStockResponse);
    }

    public List<IngredientResponse> findIngredients() {
        String sql = """
                SELECT ingredient_id, name, unit, available, updated_at
                FROM %s
                ORDER BY ingredient_id ASC
                """.formatted(stockTable);
        return jdbc.query(sql, (rs, rowNum) -> new IngredientResponse(
                rs.getString("ingredient_id"),
                rs.getString("name"),
                rs.getString("unit"),
                InventoryQuantityUtils.normalize(rs.getBigDecimal("available")),
                rs.getObject("updated_at", OffsetDateTime.class).toInstant()));
    }

    public List<RecipeResponse> findRecipes() {
        record Row(String dishId, String ingredientId, String ingredientName, String unit, BigDecimal quantity) {
        }
        String sql = """
                SELECT r.dish_id, r.ingredient_id, s.name AS ingredient_name, s.unit, r.quantity
                FROM %s r
                JOIN %s s ON s.ingredient_id = r.ingredient_id
                ORDER BY r.dish_id ASC, r.ingredient_id ASC
                """.formatted(recipesTable, stockTable);
        List<Row> rows = jdbc.query(sql, (rs, rowNum) -> new Row(
                rs.getString("dish_id"),
                rs.getString("ingredient_id"),
                rs.getString("ingredient_name"),
                rs.getString("unit"),
                InventoryQuantityUtils.normalize(rs.getBigDecimal("quantity"))));

        Map<String, List<RecipeResponse.RecipeIngredient>> byDish = new LinkedHashMap<>();
        for (Row row : rows) {
            byDish.computeIfAbsent(row.dishId(), ignored -> new ArrayList<>())
                    .add(new RecipeResponse.RecipeIngredient(
                            row.ingredientId(), row.ingredientName(), row.unit(), row.quantity()));
        }
        return byDish.entrySet().stream()
                .map(entry -> new RecipeResponse(entry.getKey(), entry.getValue()))
                .toList();
    }

    public Optional<ReservationResponse> findReservation(UUID orderId) {
        record OrderHeader(
                UUID orderId,
                String originBranch,
                InventoryStatus status,
                String reason,
                OffsetDateTime orderCreatedAt,
                OffsetDateTime updatedAt
        ) {
        }
        String orderSql = """
                SELECT order_id, origin_branch, status, reason, order_created_at, updated_at
                FROM %s
                WHERE order_id = ?
                """.formatted(ordersTable);
        List<OrderHeader> headers = jdbc.query(orderSql, (rs, rowNum) -> new OrderHeader(
                rs.getObject("order_id", UUID.class),
                rs.getString("origin_branch"),
                InventoryStatus.valueOf(rs.getString("status")),
                rs.getString("reason"),
                rs.getObject("order_created_at", OffsetDateTime.class),
                rs.getObject("updated_at", OffsetDateTime.class)), orderId);

        if (headers.isEmpty()) {
            return Optional.empty();
        }

        String itemsSql = """
                SELECT r.ingredient_id, s.name AS ingredient_name, s.unit, r.quantity, r.outcome
                FROM %s r
                JOIN %s s ON s.ingredient_id = r.ingredient_id
                WHERE r.order_id = ?
                ORDER BY r.ingredient_id ASC
                """.formatted(reservationsTable, stockTable);
        List<ReservationResponse.ReservedIngredient> items = jdbc.query(itemsSql, (rs, rowNum) -> {
            String outcomeRaw = rs.getString("outcome");
            return new ReservationResponse.ReservedIngredient(
                    rs.getString("ingredient_id"),
                    rs.getString("ingredient_name"),
                    rs.getString("unit"),
                    InventoryQuantityUtils.normalize(rs.getBigDecimal("quantity")),
                    outcomeRaw == null ? null : ReservationOutcome.valueOf(outcomeRaw));
        }, orderId);

        OrderHeader h = headers.getFirst();
        return Optional.of(new ReservationResponse(
                h.orderId(),
                h.originBranch(),
                h.status(),
                h.reason(),
                h.orderCreatedAt() == null ? null : h.orderCreatedAt().toInstant(),
                h.updatedAt().toInstant(),
                items));
    }

    public List<AlertResponse> findAlerts(AlertKind kind) {
        StringBuilder sql = new StringBuilder("""
                SELECT id, kind, order_id, event_id, source_topic, source_partition, source_offset, detail::text, created_at
                FROM %s
                """.formatted(alertsTable));
        if (kind != null) {
            sql.append(" WHERE kind = ? ");
        }
        sql.append(" ORDER BY id DESC ");

        if (kind != null) {
            return jdbc.query(sql.toString(), this::mapAlertResponse, kind.name());
        }
        return jdbc.query(sql.toString(), this::mapAlertResponse);
    }

    public long countNeedsCheckIngredients() {
        Long count = jdbc.queryForObject("SELECT count(*) FROM %s WHERE needs_check = TRUE".formatted(stockTable), Long.class);
        return count == null ? 0L : count;
    }

    public long countPendingOutbox() {
        Long count = jdbc.queryForObject("SELECT count(*) FROM %s WHERE sent_at IS NULL".formatted(outboxTable), Long.class);
        return count == null ? 0L : count;
    }

    public long countAlerts() {
        Long count = jdbc.queryForObject("SELECT count(*) FROM %s".formatted(alertsTable), Long.class);
        return count == null ? 0L : count;
    }

    private AlertResponse mapAlertResponse(ResultSet rs, int rowNum) throws SQLException {
        String detailText = rs.getString("detail");
        JsonNode detailNode = detailText == null ? null : json.readTree(detailText);
        return new AlertResponse(
                rs.getLong("id"),
                AlertKind.valueOf(rs.getString("kind")),
                rs.getObject("order_id", UUID.class),
                rs.getObject("event_id", UUID.class),
                rs.getString("source_topic"),
                rs.getInt("source_partition"),
                rs.getLong("source_offset"),
                detailNode,
                rs.getObject("created_at", OffsetDateTime.class).toInstant());
    }

    private static StockResponse mapStockResponse(ResultSet rs, int rowNum) throws SQLException {
        return new StockResponse(
                rs.getString("ingredient_id"),
                rs.getString("name"),
                rs.getString("unit"),
                InventoryQuantityUtils.normalize(rs.getBigDecimal("available")),
                InventoryQuantityUtils.normalize(rs.getBigDecimal("reserved")),
                InventoryQuantityUtils.normalize(rs.getBigDecimal("consumed")),
                InventoryQuantityUtils.normalize(rs.getBigDecimal("wasted")),
                rs.getBoolean("needs_check"),
                rs.getObject("updated_at", OffsetDateTime.class).toInstant());
    }
}
