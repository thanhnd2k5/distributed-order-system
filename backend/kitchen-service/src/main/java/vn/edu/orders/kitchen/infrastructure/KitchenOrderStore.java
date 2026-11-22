package vn.edu.orders.kitchen.infrastructure;

import java.sql.Array;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class KitchenOrderStore {
    private final JdbcClient jdbc;
    private final String orders;
    private final String flaggedIngredients;
    private final String alerts;
    private final String processedEvents;
    private final String outboxEvents;

    public KitchenOrderStore(JdbcClient jdbc, @Value("${spring.flyway.default-schema}") String schema) {
        this.jdbc = jdbc;
        String prefix = "\"" + schema.replace("\"", "\"\"") + "\".";
        this.orders = prefix + "kitchen_orders";
        this.flaggedIngredients = prefix + "flagged_ingredients";
        this.alerts = prefix + "alerts";
        this.processedEvents = prefix + "processed_events";
        this.outboxEvents = prefix + "outbox_events";
    }

    public boolean tryInsertProcessedEvent(
            UUID eventId,
            UUID orderId,
            String eventType,
            String outcome,
            OffsetDateTime processedAt
    ) {
        return jdbc.sql("""
                INSERT INTO %s (event_id, order_id, event_type, request_fingerprint, outcome, processed_at)
                VALUES (:eventId, :orderId, :eventType, NULL, :outcome, :processedAt)
                ON CONFLICT (event_id) DO NOTHING
                RETURNING event_id
                """.formatted(processedEvents))
                .param("eventId", eventId)
                .param("orderId", orderId)
                .param("eventType", eventType)
                .param("outcome", outcome)
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

    /**
     * Single row-locked entry point per §13.4:
     * ensures a placeholder NOT_EXISTS row exists and acquires FOR UPDATE lock on it.
     */
    public OrderRow ensureAndLockOrder(UUID orderId) {
        jdbc.sql("""
                INSERT INTO %s (order_id, status)
                VALUES (:orderId, 'NOT_EXISTS')
                ON CONFLICT (order_id) DO NOTHING
                """.formatted(orders))
                .param("orderId", orderId)
                .update();

        return jdbc.sql("""
                SELECT order_id, status, origin_branch, cancel_branches,
                       cancel_reasons::text AS cancel_reasons_json,
                       cancelled_by, close_reason,
                       items::text AS items_json,
                       reserved::text AS reserved_json,
                       spoiled::text AS spoiled_json,
                       order_created_at, updated_at
                FROM %s
                WHERE order_id = :orderId
                FOR UPDATE
                """.formatted(orders))
                .param("orderId", orderId)
                .query((rs, rowNum) -> new OrderRow(
                        rs.getObject("order_id", UUID.class),
                        rs.getString("status"),
                        rs.getString("origin_branch"),
                        readTextArray(rs.getArray("cancel_branches")),
                        rs.getString("cancel_reasons_json"),
                        rs.getString("cancelled_by"),
                        rs.getString("close_reason"),
                        rs.getString("items_json"),
                        rs.getString("reserved_json"),
                        rs.getString("spoiled_json"),
                        rs.getObject("order_created_at", OffsetDateTime.class),
                        rs.getObject("updated_at", OffsetDateTime.class)
                ))
                .optional()
                .orElseThrow(() -> new IllegalStateException("Order row disappeared after insert for orderId=" + orderId));
    }

    public void transitionNotExistsToQueued(
            UUID orderId,
            String originBranch,
            String itemsJson,
            String reservedJson,
            OffsetDateTime orderCreatedAt,
            OffsetDateTime now
    ) {
        int updated = jdbc.sql("""
                UPDATE %s
                SET status = 'QUEUED',
                    origin_branch = :originBranch,
                    items = CAST(:itemsJson AS jsonb),
                    reserved = CAST(:reservedJson AS jsonb),
                    order_created_at = :orderCreatedAt,
                    updated_at = :now
                WHERE order_id = :orderId AND status = 'NOT_EXISTS'
                """.formatted(orders))
                .param("orderId", orderId)
                .param("originBranch", originBranch)
                .param("itemsJson", itemsJson)
                .param("reservedJson", reservedJson)
                .param("orderCreatedAt", orderCreatedAt)
                .param("now", now)
                .update();
        assertSingleRowUpdated(updated, orderId, "NOT_EXISTS", "QUEUED");
    }

    public void transitionNotExistsToCancelPending(
            UUID orderId,
            String requestingBranch,
            String reasonCode,
            OffsetDateTime now
    ) {
        int updated = jdbc.sql("""
                UPDATE %s
                SET status = 'CANCEL_PENDING',
                    cancel_branches = ARRAY[:branch]::text[],
                    cancel_reasons = jsonb_build_object(:branch, :reasonCode),
                    updated_at = :now
                WHERE order_id = :orderId AND status = 'NOT_EXISTS'
                """.formatted(orders))
                .param("orderId", orderId)
                .param("branch", requestingBranch)
                .param("reasonCode", reasonCode)
                .param("now", now)
                .update();
        assertSingleRowUpdated(updated, orderId, "NOT_EXISTS", "CANCEL_PENDING");
    }

    public void addCancelBranchToPending(
            UUID orderId,
            String requestingBranch,
            String reasonCode,
            OffsetDateTime now
    ) {
        int updated = jdbc.sql("""
                UPDATE %s
                SET cancel_branches = CASE
                        WHEN :branch = ANY(cancel_branches) THEN cancel_branches
                        ELSE array_append(cancel_branches, :branch)
                    END,
                    cancel_reasons = CASE
                        WHEN jsonb_exists(cancel_reasons, :branch) THEN cancel_reasons
                        ELSE cancel_reasons || jsonb_build_object(:branch, :reasonCode)
                    END,
                    updated_at = :now
                WHERE order_id = :orderId AND status = 'CANCEL_PENDING'
                """.formatted(orders))
                .param("orderId", orderId)
                .param("branch", requestingBranch)
                .param("reasonCode", reasonCode)
                .param("now", now)
                .update();
        assertSingleRowUpdated(updated, orderId, "CANCEL_PENDING", "CANCEL_PENDING");
    }

    public void transitionCancelPendingToCancelled(
            UUID orderId,
            String originBranch,
            String closeReason,
            String itemsJson,
            String reservedJson,
            OffsetDateTime orderCreatedAt,
            OffsetDateTime now
    ) {
        int updated = jdbc.sql("""
                UPDATE %s
                SET status = 'CANCELLED',
                    origin_branch = :originBranch,
                    cancelled_by = 'CUSTOMER',
                    close_reason = :closeReason,
                    items = CAST(:itemsJson AS jsonb),
                    reserved = CAST(:reservedJson AS jsonb),
                    order_created_at = :orderCreatedAt,
                    updated_at = :now
                WHERE order_id = :orderId AND status = 'CANCEL_PENDING'
                """.formatted(orders))
                .param("orderId", orderId)
                .param("originBranch", originBranch)
                .param("closeReason", closeReason)
                .param("itemsJson", itemsJson)
                .param("reservedJson", reservedJson)
                .param("orderCreatedAt", orderCreatedAt)
                .param("now", now)
                .update();
        assertSingleRowUpdated(updated, orderId, "CANCEL_PENDING", "CANCELLED");
    }

    public void transitionCancelPendingToQueuedDismissingWrongBranches(
            UUID orderId,
            String originBranch,
            String itemsJson,
            String reservedJson,
            OffsetDateTime orderCreatedAt,
            OffsetDateTime now
    ) {
        int updated = jdbc.sql("""
                UPDATE %s
                SET status = 'QUEUED',
                    origin_branch = :originBranch,
                    cancel_branches = '{}'::text[],
                    cancel_reasons = '{}'::jsonb,
                    items = CAST(:itemsJson AS jsonb),
                    reserved = CAST(:reservedJson AS jsonb),
                    order_created_at = :orderCreatedAt,
                    updated_at = :now
                WHERE order_id = :orderId AND status = 'CANCEL_PENDING'
                """.formatted(orders))
                .param("orderId", orderId)
                .param("originBranch", originBranch)
                .param("itemsJson", itemsJson)
                .param("reservedJson", reservedJson)
                .param("orderCreatedAt", orderCreatedAt)
                .param("now", now)
                .update();
        assertSingleRowUpdated(updated, orderId, "CANCEL_PENDING", "QUEUED");
    }

    public void transitionToFailed(
            UUID orderId,
            String expectedCurrentStatus,
            String originBranch,
            String closeReason,
            OffsetDateTime now
    ) {
        int updated = jdbc.sql("""
                UPDATE %s
                SET status = 'FAILED',
                    origin_branch = :originBranch,
                    close_reason = :closeReason,
                    updated_at = :now
                WHERE order_id = :orderId AND status = :expectedStatus
                """.formatted(orders))
                .param("orderId", orderId)
                .param("expectedStatus", expectedCurrentStatus)
                .param("originBranch", originBranch)
                .param("closeReason", closeReason)
                .param("now", now)
                .update();
        assertSingleRowUpdated(updated, orderId, expectedCurrentStatus, "FAILED");
    }

    public void transitionQueuedToCustomerCancelled(
            UUID orderId,
            String closeReason,
            OffsetDateTime now
    ) {
        int updated = jdbc.sql("""
                UPDATE %s
                SET status = 'CANCELLED',
                    cancelled_by = 'CUSTOMER',
                    close_reason = :closeReason,
                    updated_at = :now
                WHERE order_id = :orderId AND status = 'QUEUED'
                """.formatted(orders))
                .param("orderId", orderId)
                .param("closeReason", closeReason)
                .param("now", now)
                .update();
        assertSingleRowUpdated(updated, orderId, "QUEUED", "CANCELLED");
    }

    public void transitionQueuedToCooking(UUID orderId, OffsetDateTime now) {
        int updated = jdbc.sql("""
                UPDATE %s
                SET status = 'COOKING',
                    updated_at = :now
                WHERE order_id = :orderId AND status = 'QUEUED'
                """.formatted(orders))
                .param("orderId", orderId)
                .param("now", now)
                .update();
        assertSingleRowUpdated(updated, orderId, "QUEUED", "COOKING");
    }

    public void transitionQueuedToKitchenRejected(
            UUID orderId,
            String closeReason,
            String spoiledJson,
            OffsetDateTime now
    ) {
        int updated = jdbc.sql("""
                UPDATE %s
                SET status = 'CANCELLED',
                    cancelled_by = 'KITCHEN',
                    close_reason = :closeReason,
                    spoiled = CAST(:spoiledJson AS jsonb),
                    updated_at = :now
                WHERE order_id = :orderId AND status = 'QUEUED'
                """.formatted(orders))
                .param("orderId", orderId)
                .param("closeReason", closeReason)
                .param("spoiledJson", spoiledJson)
                .param("now", now)
                .update();
        assertSingleRowUpdated(updated, orderId, "QUEUED", "CANCELLED");
    }

    public void transitionCookingToDone(UUID orderId, OffsetDateTime now) {
        int updated = jdbc.sql("""
                UPDATE %s
                SET status = 'DONE',
                    updated_at = :now
                WHERE order_id = :orderId AND status = 'COOKING'
                """.formatted(orders))
                .param("orderId", orderId)
                .param("now", now)
                .update();
        assertSingleRowUpdated(updated, orderId, "COOKING", "DONE");
    }

    public void insertOutboxEvent(
            UUID eventId,
            UUID orderId,
            String eventType,
            String topic,
            String messageKey,
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
                .param("eventId", eventId)
                .param("aggregateId", orderId)
                .param("eventType", eventType)
                .param("topic", topic)
                .param("messageKey", messageKey)
                .param("payload", payloadJson)
                .param("now", now)
                .update();
    }

    public boolean upsertFlaggedIngredientIfNewer(
            String ingredientId,
            boolean needsCheck,
            int epoch,
            long seq,
            OffsetDateTime now
    ) {
        return jdbc.sql("""
                INSERT INTO %s (ingredient_id, needs_check, epoch, seq, updated_at)
                VALUES (:ingredientId, :needsCheck, :epoch, :seq, :now)
                ON CONFLICT (ingredient_id) DO UPDATE
                SET needs_check = EXCLUDED.needs_check,
                    epoch = EXCLUDED.epoch,
                    seq = EXCLUDED.seq,
                    updated_at = EXCLUDED.updated_at
                WHERE (%s.epoch, %s.seq) < (EXCLUDED.epoch, EXCLUDED.seq)
                RETURNING ingredient_id
                """.formatted(flaggedIngredients, flaggedIngredients, flaggedIngredients))
                .param("ingredientId", ingredientId)
                .param("needsCheck", needsCheck)
                .param("epoch", epoch)
                .param("seq", seq)
                .param("now", now)
                .query(String.class)
                .optional()
                .isPresent();
    }

    public void insertAlert(
            String kind,
            UUID orderId,
            UUID eventId,
            String sourceTopic,
            int sourcePartition,
            long sourceOffset,
            String detailJson,
            OffsetDateTime now
    ) {
        jdbc.sql("""
                INSERT INTO %s (kind, order_id, event_id, source_topic, source_partition, source_offset, detail, created_at)
                VALUES (:kind, :orderId, :eventId, :sourceTopic, :sourcePartition, :sourceOffset, CAST(:detailJson AS jsonb), :now)
                ON CONFLICT (kind, source_topic, source_partition, source_offset) DO NOTHING
                """.formatted(alerts))
                .param("kind", kind)
                .param("orderId", orderId)
                .param("eventId", eventId)
                .param("sourceTopic", sourceTopic)
                .param("sourcePartition", sourcePartition)
                .param("sourceOffset", sourceOffset)
                .param("detailJson", detailJson)
                .param("now", now)
                .update();
    }

    public List<TicketViewRow> listTickets(String statusFilter) {
        String whereClause;
        JdbcClient.StatementSpec spec;
        String baseSql = """
                SELECT o.order_id, o.status, o.origin_branch, o.cancelled_by, o.close_reason,
                       o.items::text AS items_json,
                       o.reserved::text AS reserved_json,
                       o.spoiled::text AS spoiled_json,
                       o.order_created_at, o.updated_at,
                       CASE
                           WHEN o.status = 'QUEUED' THEN COALESCE(
                               (SELECT array_agg(DISTINCT fi.ingredient_id ORDER BY fi.ingredient_id)
                                FROM jsonb_array_elements(COALESCE(o.reserved, '[]'::jsonb)) AS elem
                                JOIN %s fi
                                  ON fi.ingredient_id = COALESCE(elem->>'ingredientId', elem->>'ingredient_id')
                                 AND fi.needs_check = TRUE),
                               '{}'::text[]
                           )
                           ELSE '{}'::text[]
                       END AS flagged_ids
                FROM %s o
                %s
                ORDER BY o.order_created_at ASC NULLS LAST, o.order_id ASC
                LIMIT 100
                """;

        if (statusFilter == null || statusFilter.isBlank()) {
            whereClause = "WHERE o.status IN ('QUEUED', 'COOKING')";
            spec = jdbc.sql(baseSql.formatted(flaggedIngredients, orders, whereClause));
        } else if ("ALL".equals(statusFilter)) {
            whereClause = "WHERE o.status <> 'NOT_EXISTS'";
            spec = jdbc.sql(baseSql.formatted(flaggedIngredients, orders, whereClause));
        } else {
            whereClause = "WHERE o.status = :status";
            spec = jdbc.sql(baseSql.formatted(flaggedIngredients, orders, whereClause))
                    .param("status", statusFilter);
        }

        return spec.query((rs, rowNum) -> new TicketViewRow(
                rs.getObject("order_id", UUID.class),
                rs.getString("status"),
                rs.getString("origin_branch"),
                rs.getString("cancelled_by"),
                rs.getString("close_reason"),
                rs.getString("items_json"),
                rs.getString("reserved_json"),
                rs.getString("spoiled_json"),
                readTextArray(rs.getArray("flagged_ids")),
                rs.getObject("order_created_at", OffsetDateTime.class),
                rs.getObject("updated_at", OffsetDateTime.class)
        )).list();
    }

    public Optional<TicketViewRow> findTicket(UUID orderId) {
        return jdbc.sql("""
                SELECT o.order_id, o.status, o.origin_branch, o.cancelled_by, o.close_reason,
                       o.items::text AS items_json,
                       o.reserved::text AS reserved_json,
                       o.spoiled::text AS spoiled_json,
                       o.order_created_at, o.updated_at,
                       CASE
                           WHEN o.status = 'QUEUED' THEN COALESCE(
                               (SELECT array_agg(DISTINCT fi.ingredient_id ORDER BY fi.ingredient_id)
                                FROM jsonb_array_elements(COALESCE(o.reserved, '[]'::jsonb)) AS elem
                                JOIN %s fi
                                  ON fi.ingredient_id = COALESCE(elem->>'ingredientId', elem->>'ingredient_id')
                                 AND fi.needs_check = TRUE),
                               '{}'::text[]
                           )
                           ELSE '{}'::text[]
                       END AS flagged_ids
                FROM %s o
                WHERE o.order_id = :orderId AND o.status <> 'NOT_EXISTS'
                """.formatted(flaggedIngredients, orders))
                .param("orderId", orderId)
                .query((rs, rowNum) -> new TicketViewRow(
                        rs.getObject("order_id", UUID.class),
                        rs.getString("status"),
                        rs.getString("origin_branch"),
                        rs.getString("cancelled_by"),
                        rs.getString("close_reason"),
                        rs.getString("items_json"),
                        rs.getString("reserved_json"),
                        rs.getString("spoiled_json"),
                        readTextArray(rs.getArray("flagged_ids")),
                        rs.getObject("order_created_at", OffsetDateTime.class),
                        rs.getObject("updated_at", OffsetDateTime.class)
                ))
                .optional();
    }

    public List<AlertRow> listAlerts(String kindFilter) {
        String sql;
        JdbcClient.StatementSpec spec;
        if (kindFilter == null || kindFilter.isBlank()) {
            sql = """
                    SELECT id, kind, order_id, event_id, source_topic, source_partition, source_offset,
                           detail::text AS detail_json, created_at
                    FROM %s
                    ORDER BY created_at DESC, id DESC
                    LIMIT 100
                    """.formatted(alerts);
            spec = jdbc.sql(sql);
        } else {
            sql = """
                    SELECT id, kind, order_id, event_id, source_topic, source_partition, source_offset,
                           detail::text AS detail_json, created_at
                    FROM %s
                    WHERE kind = :kind
                    ORDER BY created_at DESC, id DESC
                    LIMIT 100
                    """.formatted(alerts);
            spec = jdbc.sql(sql).param("kind", kindFilter);
        }
        return spec.query((rs, rowNum) -> new AlertRow(
                rs.getLong("id"),
                rs.getString("kind"),
                rs.getObject("order_id", UUID.class),
                rs.getObject("event_id", UUID.class),
                rs.getString("source_topic"),
                rs.getInt("source_partition"),
                rs.getLong("source_offset"),
                rs.getString("detail_json"),
                rs.getObject("created_at", OffsetDateTime.class)
        )).list();
    }

    public long countPendingOutbox() {
        Long count = jdbc.sql("SELECT count(*) FROM %s WHERE sent_at IS NULL".formatted(outboxEvents))
                .query(Long.class)
                .single();
        return count == null ? 0L : count;
    }

    private static void assertSingleRowUpdated(int updated, UUID orderId, String fromStatus, String toStatus) {
        if (updated != 1) {
            throw new IllegalStateException(
                    "Expected 1 kitchen_orders row transitioning " + fromStatus + " -> " + toStatus
                            + " for orderId=" + orderId + ", updatedRows=" + updated);
        }
    }

    private static List<String> readTextArray(Array sqlArray) throws SQLException {
        if (sqlArray == null) {
            return List.of();
        }
        Object raw = sqlArray.getArray();
        if (raw == null) {
            return List.of();
        }
        if (raw instanceof String[] strings) {
            return List.of(strings);
        }
        Object[] objs = (Object[]) raw;
        return Arrays.stream(objs).map(String::valueOf).toList();
    }

    public record OrderRow(
            UUID orderId,
            String status,
            String originBranch,
            List<String> cancelBranches,
            String cancelReasonsJson,
            String cancelledBy,
            String closeReason,
            String itemsJson,
            String reservedJson,
            String spoiledJson,
            OffsetDateTime orderCreatedAt,
            OffsetDateTime updatedAt
    ) {
    }

    public record TicketViewRow(
            UUID orderId,
            String status,
            String originBranch,
            String cancelledBy,
            String closeReason,
            String itemsJson,
            String reservedJson,
            String spoiledJson,
            List<String> flaggedIngredients,
            OffsetDateTime orderCreatedAt,
            OffsetDateTime updatedAt
    ) {
    }

    public record AlertRow(
            long id,
            String kind,
            UUID orderId,
            UUID eventId,
            String sourceTopic,
            int sourcePartition,
            long sourceOffset,
            String detailJson,
            OffsetDateTime createdAt
    ) {
    }
}
