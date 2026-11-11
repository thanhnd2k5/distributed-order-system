package vn.edu.orders.order.status;

import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class OrderStatusStore {
    private final JdbcClient jdbc;
    private final String orders;
    private final String processedEvents;

    public OrderStatusStore(JdbcClient jdbc, @Value("${spring.flyway.default-schema}") String schema) {
        this.jdbc = jdbc;
        String schemaPrefix = "\"" + schema.replace("\"", "\"\"") + "\".";
        this.orders = schemaPrefix + "orders";
        this.processedEvents = schemaPrefix + "processed_events";
    }

    public boolean tryInsertProcessedEvent(UUID eventId, UUID orderId, String eventType, OffsetDateTime now) {
        try {
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
        } catch (DuplicateKeyException e) {
            return false;
        }
    }

    public void updateProcessedOutcome(UUID eventId, String outcome) {
        jdbc.sql("""
                UPDATE %s
                SET outcome = :outcome
                WHERE event_id = :eventId
                """.formatted(processedEvents))
                .param("outcome", outcome)
                .param("eventId", eventId)
                .update();
    }

    public record OrderRecord(
            UUID orderId,
            String siteId,
            String sourceType,
            String status,
            String failureReason,
            long version,
            OffsetDateTime createdAt,
            OffsetDateTime updatedAt
    ) {}

    public Optional<OrderRecord> lockOrderForUpdate(UUID orderId) {
        return jdbc.sql("""
                SELECT order_id, site_id, source_type, status, failure_reason, version, created_at, updated_at
                FROM %s
                WHERE order_id = :orderId
                FOR UPDATE
                """.formatted(orders))
                .param("orderId", orderId)
                .query((rs, rowNum) -> new OrderRecord(
                        rs.getObject("order_id", UUID.class),
                        rs.getString("site_id"),
                        rs.getString("source_type"),
                        rs.getString("status"),
                        rs.getString("failure_reason"),
                        rs.getLong("version"),
                        rs.getObject("created_at", OffsetDateTime.class),
                        rs.getObject("updated_at", OffsetDateTime.class)
                ))
                .optional();
    }

    public void updateOrderStatus(UUID orderId, String newStatus, String failureReason, OffsetDateTime now) {
        int updated = jdbc.sql("""
                UPDATE %s
                SET status = :status,
                    failure_reason = :failureReason,
                    version = version + 1,
                    updated_at = :now
                WHERE order_id = :orderId
                """.formatted(orders))
                .param("status", newStatus)
                .param("failureReason", failureReason)
                .param("now", now)
                .param("orderId", orderId)
                .update();
        if (updated != 1) {
            throw new IllegalStateException("Failed to update status for orderId=" + orderId + ", updatedRows=" + updated);
        }
    }
}
