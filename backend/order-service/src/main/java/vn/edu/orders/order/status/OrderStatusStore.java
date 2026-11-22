package vn.edu.orders.order.status;

import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import vn.edu.orders.contracts.events.CancelFlag;
import vn.edu.orders.contracts.events.OrderStatus;

@Repository
public class OrderStatusStore {
    private final JdbcClient jdbc;
    private final String orders;
    private final String processedEvents;
    private final String alerts;

    public OrderStatusStore(JdbcClient jdbc, @Value("${spring.flyway.default-schema}") String schema) {
        this.jdbc = jdbc;
        String schemaPrefix = "\"" + schema.replace("\"", "\"\"") + "\".";
        this.orders = schemaPrefix + "orders";
        this.processedEvents = schemaPrefix + "processed_events";
        this.alerts = schemaPrefix + "alerts";
    }

    public boolean tryInsertProcessedEvent(UUID eventId, UUID orderId, String eventType, OffsetDateTime now) {
        int inserted = jdbc.sql("""
                INSERT INTO %s (event_id, order_id, event_type, processed_at, outcome)
                VALUES (:eventId, :orderId, :eventType, :now, 'SUCCESS')
                ON CONFLICT (event_id) DO NOTHING
                """.formatted(processedEvents))
                .param("eventId", eventId)
                .param("orderId", orderId)
                .param("eventType", eventType)
                .param("now", now)
                .update();
        return inserted == 1;
    }

    public void updateProcessedOutcome(UUID eventId, String outcome) {
        int updated = jdbc.sql("""
                UPDATE %s
                SET outcome = :outcome
                WHERE event_id = :eventId
                """.formatted(processedEvents))
                .param("outcome", outcome)
                .param("eventId", eventId)
                .update();
        if (updated != 1) {
            throw new IllegalStateException(
                    "Expected 1 processed_events row for eventId=" + eventId + ", got " + updated);
        }
    }

    public record OrderSnapshot(
            UUID orderId,
            String status,
            int statusRank,
            String cancelFlag
    ) {
    }

    public Optional<OrderSnapshot> findOrderSnapshot(UUID orderId) {
        return jdbc.sql("""
                SELECT order_id, status, status_rank, cancel_flag
                FROM %s
                WHERE order_id = :orderId
                """.formatted(orders))
                .param("orderId", orderId)
                .query((rs, row) -> new OrderSnapshot(
                        rs.getObject("order_id", UUID.class),
                        rs.getString("status"),
                        rs.getInt("status_rank"),
                        rs.getString("cancel_flag")
                ))
                .optional();
    }

    /**
     * Conditional Write (3) from 02 §5: forward-only rank update, clearing {@code cancel_flag}
     * when reaching terminal rank 2.
     */
    public int applyRankBasedStatus(
            UUID orderId,
            OrderStatus targetStatus,
            String closeReason,
            String failureReason,
            OffsetDateTime now
    ) {
        return jdbc.sql("""
                UPDATE %s
                SET status = :status,
                    status_rank = :rank,
                    close_reason = COALESCE(:closeReason, close_reason),
                    failure_reason = CASE
                        WHEN :status = 'FAILED' THEN COALESCE(:failureReason, failure_reason)
                        ELSE failure_reason
                    END,
                    cancel_flag = CASE
                        WHEN :rank = 2 THEN 'NONE'
                        ELSE cancel_flag
                    END,
                    version = version + 1,
                    updated_at = :now
                WHERE order_id = :orderId
                  AND status_rank < :rank
                """.formatted(orders))
                .param("status", targetStatus.name())
                .param("rank", targetStatus.statusRank())
                .param("closeReason", closeReason)
                .param("failureReason", failureReason)
                .param("now", now)
                .param("orderId", orderId)
                .update();
    }

    /**
     * Conditional Write (4) from 02 §5: applies {@code CancelPending} only when
     * {@code status_rank < 2} and {@code cancel_flag = 'REQUESTED'}.
     */
    public int applyCancelPending(UUID orderId, OffsetDateTime now) {
        return jdbc.sql("""
                UPDATE %s
                SET cancel_flag = :newFlag,
                    version = version + 1,
                    updated_at = :now
                WHERE order_id = :orderId
                  AND status_rank < 2
                  AND cancel_flag = :expectedFlag
                """.formatted(orders))
                .param("newFlag", CancelFlag.KITCHEN_WILL_NOT_COOK.name())
                .param("expectedFlag", CancelFlag.REQUESTED.name())
                .param("now", now)
                .param("orderId", orderId)
                .update();
    }

    /**
     * Conditional Write (5) from 02 §5: applies {@code CancelRejected} at any rank,
     * clearing {@code cancel_flag} and setting {@code cancel_message}.
     */
    public int applyCancelRejected(UUID orderId, String cancelMessage, OffsetDateTime now) {
        return jdbc.sql("""
                UPDATE %s
                SET cancel_flag = :newFlag,
                    cancel_message = :cancelMessage,
                    version = version + 1,
                    updated_at = :now
                WHERE order_id = :orderId
                """.formatted(orders))
                .param("newFlag", CancelFlag.NONE.name())
                .param("cancelMessage", cancelMessage)
                .param("now", now)
                .param("orderId", orderId)
                .update();
    }

    public void insertAlert(
            String kind,
            UUID orderId,
            UUID eventId,
            String sourceTopic,
            int sourcePartition,
            long sourceOffset,
            String detailJson,
            OffsetDateTime createdAt
    ) {
        jdbc.sql("""
                INSERT INTO %s (
                    kind, order_id, event_id, source_topic, source_partition, source_offset, detail, created_at
                )
                VALUES (
                    :kind, :orderId, :eventId, :sourceTopic, :sourcePartition, :sourceOffset,
                    CAST(:detail AS jsonb), :createdAt
                )
                ON CONFLICT (kind, source_topic, source_partition, source_offset) DO NOTHING
                """.formatted(alerts))
                .param("kind", kind)
                .param("orderId", orderId)
                .param("eventId", eventId)
                .param("sourceTopic", sourceTopic)
                .param("sourcePartition", sourcePartition)
                .param("sourceOffset", sourceOffset)
                .param("detail", detailJson)
                .param("createdAt", createdAt)
                .update();
    }
}
