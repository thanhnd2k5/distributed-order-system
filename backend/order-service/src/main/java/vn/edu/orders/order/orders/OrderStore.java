package vn.edu.orders.order.orders;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import vn.edu.orders.order.config.SiteProperties;

@Repository
class OrderStore {
    private static final String SUMMARY_COLUMNS = "order_id, site_id, status, failure_reason, created_at";
    private static final RowMapper<OrderSummary> SUMMARY = (rs, row) -> new OrderSummary(
            rs.getObject("order_id", UUID.class), rs.getString("site_id"), rs.getString("status"),
            rs.getString("failure_reason"), rs.getObject("created_at", OffsetDateTime.class).toInstant());

    private final JdbcClient jdbc;
    private final String orders;
    private final String orderItems;
    private final String outboxEvents;

    OrderStore(JdbcClient jdbc, @Value("${spring.flyway.default-schema}") String schema) {
        this.jdbc = jdbc;
        String schemaPrefix = "\"" + schema.replace("\"", "\"\"") + "\".";
        this.orders = schemaPrefix + "orders";
        this.orderItems = schemaPrefix + "order_items";
        this.outboxEvents = schemaPrefix + "outbox_events";
    }

    boolean insertOrder(UUID orderId, UUID requestId, SiteProperties site, OffsetDateTime now) {
        return jdbc.sql("""
                INSERT INTO %s (order_id, client_request_id, site_id, source_type, status, created_at, updated_at)
                VALUES (:orderId, :requestId, :siteId, :sourceType, 'PENDING_INVENTORY', :now, :now)
                ON CONFLICT (client_request_id) DO NOTHING
                """.formatted(orders))
                .param("orderId", orderId).param("requestId", requestId)
                .param("siteId", site.id()).param("sourceType", site.sourceType().name()).param("now", now)
                .update() == 1;
    }

    void insertItem(UUID orderId, OrderDetails.Item item) {
        jdbc.sql("""
                INSERT INTO %s (order_id, line_no, dish_id, dish_name_snapshot, quantity)
                VALUES (:orderId, :lineNo, :dishId, :dishName, :quantity)
                """.formatted(orderItems))
                .param("orderId", orderId).param("lineNo", item.lineNo()).param("dishId", item.dishId())
                .param("dishName", item.dishName()).param("quantity", item.quantity())
                .update();
    }

    void insertOrderCreated(UUID eventId, UUID orderId, String eventJson, OffsetDateTime now) {
        jdbc.sql("""
                INSERT INTO %s (event_id, aggregate_id, event_type, topic, message_key, payload, created_at, next_attempt_at)
                VALUES (:eventId, :orderId, 'OrderCreated', 'order-events', :messageKey, CAST(:payload AS jsonb), :now, :now)
                """.formatted(outboxEvents))
                .param("eventId", eventId).param("orderId", orderId).param("messageKey", orderId.toString())
                .param("payload", eventJson).param("now", now)
                .update();
    }

    OrderSummary byRequestId(UUID requestId) {
        return jdbc.sql("SELECT " + SUMMARY_COLUMNS + " FROM " + orders + " WHERE client_request_id = :id")
                .param("id", requestId).query(SUMMARY).single();
    }

    Optional<OrderSummary> byId(UUID orderId) {
        return jdbc.sql("SELECT " + SUMMARY_COLUMNS + " FROM " + orders + " WHERE order_id = :id")
                .param("id", orderId).query(SUMMARY).optional();
    }

    List<OrderSummary> recent() {
        return jdbc.sql("SELECT " + SUMMARY_COLUMNS + " FROM " + orders
                        + " ORDER BY created_at DESC, order_id DESC LIMIT 100")
                .query(SUMMARY).list();
    }

    List<OrderDetails.Item> items(UUID orderId) {
        return jdbc.sql("""
                SELECT line_no, dish_id, dish_name_snapshot, quantity
                FROM %s WHERE order_id = :id ORDER BY line_no
                """.formatted(orderItems))
                .param("id", orderId)
                .query((rs, row) -> new OrderDetails.Item(rs.getInt("line_no"), rs.getString("dish_id"),
                        rs.getString("dish_name_snapshot"), rs.getInt("quantity")))
                .list();
    }
}
