package vn.edu.orders.order.orders;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import vn.edu.orders.contracts.events.CancelFlag;
import vn.edu.orders.contracts.events.EventTopics;
import vn.edu.orders.contracts.events.EventType;
import vn.edu.orders.contracts.events.OrderStatus;
import vn.edu.orders.contracts.events.ReasonCode;
import vn.edu.orders.order.config.SiteProperties;

@Repository
public class OrderStore {
    private final JdbcClient jdbc;
    private final String orders;
    private final String orderItems;
    private final String outboxEvents;

    private final RowMapper<OrderSummary> summaryMapper;

    public OrderStore(JdbcClient jdbc, @Value("${spring.flyway.default-schema}") String schema) {
        this.jdbc = jdbc;
        String schemaPrefix = "\"" + schema.replace("\"", "\"\"") + "\".";
        this.orders = schemaPrefix + "orders";
        this.orderItems = schemaPrefix + "order_items";
        this.outboxEvents = schemaPrefix + "outbox_events";
        this.summaryMapper = (rs, row) -> new OrderSummary(
                rs.getObject("order_id", UUID.class),
                rs.getString("site_id"),
                rs.getString("status"),
                rs.getInt("status_rank"),
                rs.getString("failure_reason"),
                rs.getString("close_reason"),
                rs.getString("cancel_flag"),
                toInstant(rs.getObject("cancel_requested_at", OffsetDateTime.class)),
                rs.getString("cancel_message"),
                rs.getString("delivery_status"),
                rs.getObject("created_at", OffsetDateTime.class).toInstant(),
                rs.getObject("updated_at", OffsetDateTime.class).toInstant()
        );
    }

    public record LockedOrderForCancel(UUID orderId, String status, int statusRank, String cancelFlag) {
    }

    public boolean insertOrder(UUID orderId, UUID requestId, SiteProperties site, OffsetDateTime now) {
        return jdbc.sql("""
                INSERT INTO %s (
                    order_id, client_request_id, site_id, source_type, status, status_rank,
                    cancel_flag, created_at, updated_at
                )
                VALUES (:orderId, :requestId, :siteId, :sourceType, 'PENDING_INVENTORY', 0, 'NONE', :now, :now)
                ON CONFLICT (client_request_id) DO NOTHING
                """.formatted(orders))
                .param("orderId", orderId)
                .param("requestId", requestId)
                .param("siteId", site.id())
                .param("sourceType", site.sourceType().name())
                .param("now", now)
                .update() == 1;
    }

    public void insertItem(UUID orderId, OrderDetails.Item item) {
        jdbc.sql("""
                INSERT INTO %s (order_id, line_no, dish_id, dish_name_snapshot, quantity)
                VALUES (:orderId, :lineNo, :dishId, :dishName, :quantity)
                """.formatted(orderItems))
                .param("orderId", orderId)
                .param("lineNo", item.lineNo())
                .param("dishId", item.dishId())
                .param("dishName", item.dishName())
                .param("quantity", item.quantity())
                .update();
    }

    public void insertOrderCreated(
            UUID eventId,
            UUID orderId,
            String branchId,
            String eventJson,
            OffsetDateTime now
    ) {
        jdbc.sql("""
                INSERT INTO %s (
                    event_id, aggregate_id, order_id, kind, event_type, topic, message_key,
                    payload, attempted, created_at, next_attempt_at
                )
                VALUES (
                    :eventId, :orderId, :orderId, 'CREATE_ORDER', :eventType, :topic, :messageKey,
                    CAST(:payload AS jsonb), FALSE, :now, :now
                )
                """.formatted(outboxEvents))
                .param("eventId", eventId)
                .param("orderId", orderId)
                .param("eventType", EventType.OrderCreated.name())
                .param("topic", EventTopics.orderEventsTopic(branchId))
                .param("messageKey", orderId.toString())
                .param("payload", eventJson)
                .param("now", now)
                .update();
    }

    public Optional<OrderSummary> byRequestIdOptional(UUID requestId) {
        return jdbc.sql(summarySelectSql() + " WHERE o.client_request_id = :id")
                .param("id", requestId)
                .query(summaryMapper)
                .optional();
    }

    public OrderSummary byRequestId(UUID requestId) {
        return byRequestIdOptional(requestId)
                .orElseThrow(() -> new IllegalStateException("Order missing for clientRequestId=" + requestId));
    }

    public Optional<OrderSummary> byId(UUID orderId) {
        return jdbc.sql(summarySelectSql() + " WHERE o.order_id = :id")
                .param("id", orderId)
                .query(summaryMapper)
                .optional();
    }

    public List<OrderSummary> recent() {
        return jdbc.sql(summarySelectSql() + " ORDER BY o.created_at DESC, o.order_id DESC LIMIT 100")
                .query(summaryMapper)
                .list();
    }

    public List<OrderDetails.Item> items(UUID orderId) {
        return jdbc.sql("""
                SELECT line_no, dish_id, dish_name_snapshot, quantity
                FROM %s WHERE order_id = :id ORDER BY line_no
                """.formatted(orderItems))
                .param("id", orderId)
                .query((rs, row) -> new OrderDetails.Item(
                        rs.getInt("line_no"),
                        rs.getString("dish_id"),
                        rs.getString("dish_name_snapshot"),
                        rs.getInt("quantity")))
                .list();
    }

    public Optional<LockedOrderForCancel> lockOrderForCancel(UUID orderId) {
        return jdbc.sql("""
                SELECT order_id, status, status_rank, cancel_flag
                FROM %s
                WHERE order_id = :orderId
                FOR UPDATE
                """.formatted(orders))
                .param("orderId", orderId)
                .query((rs, row) -> new LockedOrderForCancel(
                        rs.getObject("order_id", UUID.class),
                        rs.getString("status"),
                        rs.getInt("status_rank"),
                        rs.getString("cancel_flag")
                ))
                .optional();
    }

    public List<Long> deleteUnattemptedCreateOrderOutbox(UUID orderId) {
        return jdbc.sql("""
                DELETE FROM %s
                WHERE order_id = :orderId
                  AND kind = 'CREATE_ORDER'
                  AND attempted = FALSE
                RETURNING id
                """.formatted(outboxEvents))
                .param("orderId", orderId)
                .query(Long.class)
                .list();
    }

    public void markLocallyCancelled(UUID orderId, ReasonCode reasonCode, OffsetDateTime now) {
        int updated = jdbc.sql("""
                UPDATE %s
                SET status = :status,
                    status_rank = :rank,
                    close_reason = :closeReason,
                    cancel_flag = :cancelFlag,
                    version = version + 1,
                    updated_at = :now
                WHERE order_id = :orderId
                """.formatted(orders))
                .param("status", OrderStatus.CANCELLED.name())
                .param("rank", OrderStatus.CANCELLED.statusRank())
                .param("closeReason", reasonCode.name())
                .param("cancelFlag", CancelFlag.NONE.name())
                .param("now", now)
                .param("orderId", orderId)
                .update();
        if (updated != 1) {
            throw new IllegalStateException("Expected 1 updated order on local cancel for " + orderId);
        }
    }

    public void insertCancelRequestOutbox(
            UUID eventId,
            UUID orderId,
            String branchId,
            String eventJson,
            OffsetDateTime now
    ) {
        jdbc.sql("""
                INSERT INTO %s (
                    event_id, aggregate_id, order_id, kind, event_type, topic, message_key,
                    payload, attempted, created_at, next_attempt_at
                )
                VALUES (
                    :eventId, :orderId, :orderId, 'CANCEL_REQUEST', :eventType, :topic, :messageKey,
                    CAST(:payload AS jsonb), FALSE, :now, :now
                )
                """.formatted(outboxEvents))
                .param("eventId", eventId)
                .param("orderId", orderId)
                .param("eventType", EventType.CancelRequested.name())
                .param("topic", EventTopics.cancelEventsTopic(branchId))
                .param("messageKey", orderId.toString())
                .param("payload", eventJson)
                .param("now", now)
                .update();
    }

    public void markCancelRequested(UUID orderId, OffsetDateTime now) {
        int updated = jdbc.sql("""
                UPDATE %s
                SET cancel_flag = :cancelFlag,
                    cancel_requested_at = :now,
                    cancel_message = NULL,
                    version = version + 1,
                    updated_at = :now
                WHERE order_id = :orderId
                """.formatted(orders))
                .param("cancelFlag", CancelFlag.REQUESTED.name())
                .param("now", now)
                .param("orderId", orderId)
                .update();
        if (updated != 1) {
            throw new IllegalStateException("Expected 1 updated order on cancel request for " + orderId);
        }
    }

    private String summarySelectSql() {
        return """
                SELECT o.order_id, o.site_id, o.status, o.status_rank, o.failure_reason, o.close_reason,
                       o.cancel_flag, o.cancel_requested_at, o.cancel_message, o.created_at, o.updated_at,
                       COALESCE(
                           (SELECT CASE
                                       WHEN oe.kind = 'CREATE_ORDER' AND oe.attempted = FALSE THEN 'NOT_SENT'
                                       WHEN oe.kind = 'CREATE_ORDER' AND oe.attempted = TRUE THEN 'SENDING'
                                       WHEN oe.kind = 'CANCEL_REQUEST' THEN 'CANCEL_NOT_SENT'
                                       ELSE 'SENT'
                                   END
                            FROM %s oe
                            WHERE oe.order_id = o.order_id AND oe.sent_at IS NULL
                            ORDER BY oe.id ASC
                            LIMIT 1),
                           'SENT'
                       ) AS delivery_status
                FROM %s o
                """.formatted(outboxEvents, orders);
    }

    private static java.time.Instant toInstant(OffsetDateTime timestamp) {
        return timestamp == null ? null : timestamp.toInstant();
    }
}
