package vn.edu.orders.inventory.infrastructure;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import vn.edu.orders.contracts.events.SourceType;
import vn.edu.orders.inventory.api.RecipeResponse;
import vn.edu.orders.inventory.api.ReservationResponse;

@Repository
public class InventoryQueryStore {
    private final JdbcClient jdbc;
    private final String ingredients;
    private final String recipeItems;
    private final String decisions;
    private final String reservationItems;

    public InventoryQueryStore(JdbcClient jdbc, @Value("${spring.flyway.default-schema}") String schema) {
        this.jdbc = jdbc;
        String prefix = "\"" + schema.replace("\"", "\"\"") + "\".";
        this.ingredients = prefix + "ingredients";
        this.recipeItems = prefix + "recipe_items";
        this.decisions = prefix + "inventory_order_decisions";
        this.reservationItems = prefix + "reservation_items";
    }

    public List<RecipeResponse> findRecipes() {
        record Row(String dishId, String ingredientId, String ingredientName, String unit, long quantityPerDish) {
        }
        List<Row> rows = jdbc.sql("""
                SELECT r.dish_id, r.ingredient_id, i.name AS ingredient_name, i.unit, r.quantity_per_dish
                FROM %s r
                JOIN %s i ON i.ingredient_id = r.ingredient_id
                ORDER BY r.dish_id, r.ingredient_id
                """.formatted(recipeItems, ingredients))
                .query((rs, rowNum) -> new Row(
                        rs.getString("dish_id"),
                        rs.getString("ingredient_id"),
                        rs.getString("ingredient_name"),
                        rs.getString("unit"),
                        rs.getLong("quantity_per_dish")))
                .list();

        Map<String, List<RecipeResponse.RecipeIngredient>> byDish = new LinkedHashMap<>();
        for (Row row : rows) {
            byDish.computeIfAbsent(row.dishId(), ignored -> new ArrayList<>())
                    .add(new RecipeResponse.RecipeIngredient(
                            row.ingredientId(), row.ingredientName(), row.unit(), row.quantityPerDish()));
        }
        return byDish.entrySet().stream()
                .map(entry -> new RecipeResponse(entry.getKey(), entry.getValue()))
                .toList();
    }

    public Optional<ReservationResponse> findReservation(UUID orderId) {
        record DecisionHeader(
                UUID orderId,
                SourceType sourceType,
                String sourceSiteId,
                String decision,
                String reasonCode,
                String reasonMessage,
                OffsetDateTime createdAt,
                OffsetDateTime updatedAt
        ) {
        }
        Optional<DecisionHeader> header = jdbc.sql("""
                SELECT order_id, source_type, source_site_id, decision, reason_code, reason_message, created_at, updated_at
                FROM %s
                WHERE order_id = :orderId AND decision IN ('RESERVED', 'FAILED')
                """.formatted(decisions))
                .param("orderId", orderId)
                .query((rs, rowNum) -> new DecisionHeader(
                        rs.getObject("order_id", UUID.class),
                        SourceType.valueOf(rs.getString("source_type")),
                        rs.getString("source_site_id"),
                        rs.getString("decision"),
                        rs.getString("reason_code"),
                        rs.getString("reason_message"),
                        rs.getObject("created_at", OffsetDateTime.class),
                        rs.getObject("updated_at", OffsetDateTime.class)))
                .optional();

        if (header.isEmpty()) {
            return Optional.empty();
        }
        List<ReservationResponse.ReservedIngredient> items = jdbc.sql("""
                SELECT r.ingredient_id, i.name AS ingredient_name, i.unit, r.reserved_quantity
                FROM %s r
                JOIN %s i ON i.ingredient_id = r.ingredient_id
                WHERE r.order_id = :orderId
                ORDER BY r.ingredient_id
                """.formatted(reservationItems, ingredients))
                .param("orderId", orderId)
                .query((rs, rowNum) -> new ReservationResponse.ReservedIngredient(
                        rs.getString("ingredient_id"),
                        rs.getString("ingredient_name"),
                        rs.getString("unit"),
                        rs.getLong("reserved_quantity")))
                .list();

        DecisionHeader d = header.get();
        return Optional.of(new ReservationResponse(
                d.orderId(),
                d.sourceType(),
                d.sourceSiteId(),
                d.decision(),
                d.reasonCode(),
                d.reasonMessage(),
                d.createdAt().toInstant(),
                d.updatedAt().toInstant(),
                items));
    }
}
