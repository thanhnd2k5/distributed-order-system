package vn.edu.orders.inventory.infrastructure;

import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import vn.edu.orders.contracts.events.EventTopics;
import vn.edu.orders.contracts.events.EventType;
import vn.edu.orders.contracts.events.FailureReasonCode;
import vn.edu.orders.contracts.events.SourceType;

@Repository
public class InventoryReservationStore {
    private final JdbcClient jdbc;
    private final String processedEvents;
    private final String decisions;
    private final String recipeItems;
    private final String ingredients;
    private final String reservationItems;
    private final String outboxEvents;

    public InventoryReservationStore(JdbcClient jdbc, @Value("${spring.flyway.default-schema}") String schema) {
        this.jdbc = jdbc;
        String prefix = "\"" + schema.replace("\"", "\"\"") + "\".";
        this.processedEvents = prefix + "processed_events";
        this.decisions = prefix + "inventory_order_decisions";
        this.recipeItems = prefix + "recipe_items";
        this.ingredients = prefix + "ingredients";
        this.reservationItems = prefix + "reservation_items";
        this.outboxEvents = prefix + "outbox_events";
    }

    public boolean tryInsertProcessedEvent(
            UUID eventId,
            UUID orderId,
            String eventType,
            String fingerprint,
            OffsetDateTime processedAt
    ) {
        return jdbc.sql("""
                INSERT INTO %s (event_id, order_id, event_type, request_fingerprint, outcome, processed_at)
                VALUES (:eventId, :orderId, :eventType, :fingerprint, 'APPLIED', :processedAt)
                ON CONFLICT (event_id) DO NOTHING
                RETURNING event_id
                """.formatted(processedEvents))
                .param("eventId", eventId)
                .param("orderId", orderId)
                .param("eventType", eventType)
                .param("fingerprint", fingerprint)
                .param("processedAt", processedAt)
                .query(UUID.class)
                .optional()
                .isPresent();
    }

    public void updateProcessedOutcome(UUID eventId, String outcome) {
        int updated = jdbc.sql("""
                UPDATE %s SET outcome = :outcome WHERE event_id = :eventId
                """.formatted(processedEvents))
                .param("outcome", outcome)
                .param("eventId", eventId)
                .update();
        if (updated != 1) {
            throw new IllegalStateException("Expected 1 processed_events row for eventId=" + eventId + ", got " + updated);
        }
    }

    public boolean tryInsertProcessingDecision(
            UUID orderId,
            SourceType sourceType,
            String sourceSiteId,
            String fingerprint,
            OffsetDateTime now
    ) {
        return jdbc.sql("""
                INSERT INTO %s (
                    order_id, source_type, source_site_id, request_fingerprint, decision, created_at, updated_at
                )
                VALUES (:orderId, :sourceType, :sourceSiteId, :fingerprint, 'PROCESSING', :now, :now)
                ON CONFLICT (order_id) DO NOTHING
                RETURNING order_id
                """.formatted(decisions))
                .param("orderId", orderId)
                .param("sourceType", sourceType.name())
                .param("sourceSiteId", sourceSiteId)
                .param("fingerprint", fingerprint)
                .param("now", now)
                .query(UUID.class)
                .optional()
                .isPresent();
    }

    public Optional<DecisionRecord> findDecision(UUID orderId) {
        return jdbc.sql("""
                SELECT order_id, request_fingerprint, decision, reason_code
                FROM %s
                WHERE order_id = :orderId
                """.formatted(decisions))
                .param("orderId", orderId)
                .query((rs, rowNum) -> {
                    String reasonCode = rs.getString("reason_code");
                    return new DecisionRecord(
                            rs.getObject("order_id", UUID.class),
                            rs.getString("request_fingerprint").trim(),
                            rs.getString("decision"),
                            reasonCode == null ? null : FailureReasonCode.valueOf(reasonCode));
                })
                .optional();
    }

    public List<RecipeRow> findRecipeRows(Collection<String> dishIds) {
        return jdbc.sql("""
                SELECT dish_id, ingredient_id, quantity_per_dish
                FROM %s
                WHERE dish_id IN (:dishIds)
                ORDER BY dish_id, ingredient_id
                """.formatted(recipeItems))
                .param("dishIds", dishIds)
                .query((rs, rowNum) -> new RecipeRow(
                        rs.getString("dish_id"),
                        rs.getString("ingredient_id"),
                        rs.getLong("quantity_per_dish")))
                .list();
    }

    public List<LockedIngredient> lockIngredientsInOrder(List<String> sortedIngredientIds) {
        return jdbc.sql("""
                SELECT ingredient_id, name, unit, available_quantity
                FROM %s
                WHERE ingredient_id IN (:ingredientIds)
                ORDER BY ingredient_id
                FOR UPDATE
                """.formatted(ingredients))
                .param("ingredientIds", sortedIngredientIds)
                .query((rs, rowNum) -> new LockedIngredient(
                        rs.getString("ingredient_id"),
                        rs.getString("name"),
                        rs.getString("unit"),
                        rs.getLong("available_quantity")))
                .list();
    }

    public int deductIngredientIfSufficient(String ingredientId, long requiredQuantity, OffsetDateTime now) {
        return jdbc.sql("""
                UPDATE %s
                SET available_quantity = available_quantity - :required,
                    updated_at = :now
                WHERE ingredient_id = :ingredientId
                  AND available_quantity >= :required
                """.formatted(ingredients))
                .param("required", requiredQuantity)
                .param("now", now)
                .param("ingredientId", ingredientId)
                .update();
    }

    public void insertReservationItem(UUID orderId, String ingredientId, long reservedQuantity) {
        jdbc.sql("""
                INSERT INTO %s (order_id, ingredient_id, reserved_quantity)
                VALUES (:orderId, :ingredientId, :reservedQuantity)
                """.formatted(reservationItems))
                .param("orderId", orderId)
                .param("ingredientId", ingredientId)
                .param("reservedQuantity", reservedQuantity)
                .update();
    }

    public void markDecisionReserved(UUID orderId, OffsetDateTime now) {
        int updated = jdbc.sql("""
                UPDATE %s
                SET decision = 'RESERVED',
                    reason_code = NULL,
                    reason_message = NULL,
                    updated_at = :now
                WHERE order_id = :orderId
                """.formatted(decisions))
                .param("now", now)
                .param("orderId", orderId)
                .update();
        if (updated != 1) {
            throw new IllegalStateException("Expected 1 decision row for RESERVED orderId=" + orderId);
        }
    }

    public void markDecisionFailed(
            UUID orderId,
            FailureReasonCode reasonCode,
            String reasonMessage,
            OffsetDateTime now
    ) {
        int updated = jdbc.sql("""
                UPDATE %s
                SET decision = 'FAILED',
                    reason_code = :reasonCode,
                    reason_message = :reasonMessage,
                    updated_at = :now
                WHERE order_id = :orderId
                """.formatted(decisions))
                .param("reasonCode", reasonCode.name())
                .param("reasonMessage", reasonMessage)
                .param("now", now)
                .param("orderId", orderId)
                .update();
        if (updated != 1) {
            throw new IllegalStateException("Expected 1 decision row for FAILED orderId=" + orderId);
        }
    }

    public void insertOutboxStatusEvent(
            UUID outboxEventId,
            UUID orderId,
            EventType eventType,
            String payloadJson,
            OffsetDateTime now
    ) {
        jdbc.sql("""
                INSERT INTO %s (
                    event_id, aggregate_id, event_type, topic, message_key, payload, created_at, next_attempt_at
                )
                VALUES (
                    :eventId, :aggregateId, :eventType, :topic, :messageKey, CAST(:payload AS jsonb), :now, :now
                )
                """.formatted(outboxEvents))
                .param("eventId", outboxEventId)
                .param("aggregateId", orderId)
                .param("eventType", eventType.name())
                .param("topic", EventTopics.STATUS_EVENTS)
                .param("messageKey", orderId.toString())
                .param("payload", payloadJson)
                .param("now", now)
                .update();
    }

    public record DecisionRecord(
            UUID orderId,
            String requestFingerprint,
            String decision,
            FailureReasonCode reasonCode
    ) {
    }

    public record RecipeRow(String dishId, String ingredientId, long quantityPerDish) {
    }

    public record LockedIngredient(String ingredientId, String name, String unit, long availableQuantity) {
    }
}
