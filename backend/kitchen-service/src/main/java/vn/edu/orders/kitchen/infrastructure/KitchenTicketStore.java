package vn.edu.orders.kitchen.infrastructure;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import vn.edu.orders.contracts.events.EventTopics;
import vn.edu.orders.contracts.events.EventType;
import vn.edu.orders.contracts.events.SourceType;
import vn.edu.orders.kitchen.api.KitchenTicketResponse;

@Repository
public class KitchenTicketStore {
    private final JdbcClient jdbc;
    private final String tickets;
    private final String ticketItems;
    private final String processedEvents;
    private final String outboxEvents;

    public KitchenTicketStore(JdbcClient jdbc, @Value("${spring.flyway.default-schema}") String schema) {
        this.jdbc = jdbc;
        String prefix = "\"" + schema.replace("\"", "\"\"") + "\".";
        this.tickets = prefix + "kitchen_tickets";
        this.ticketItems = prefix + "kitchen_ticket_items";
        this.processedEvents = prefix + "processed_events";
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

    public boolean tryInsertWaitingTicket(
            UUID ticketId,
            UUID orderId,
            SourceType sourceType,
            String sourceSiteId,
            String fingerprint,
            OffsetDateTime now
    ) {
        return jdbc.sql("""
                INSERT INTO %s (
                    ticket_id, order_id, source_type, source_site_id, request_fingerprint,
                    status, created_at, completed_at, updated_at
                )
                VALUES (
                    :ticketId, :orderId, :sourceType, :sourceSiteId, :fingerprint,
                    'WAITING', :now, NULL, :now
                )
                ON CONFLICT (order_id) DO NOTHING
                RETURNING ticket_id
                """.formatted(tickets))
                .param("ticketId", ticketId)
                .param("orderId", orderId)
                .param("sourceType", sourceType.name())
                .param("sourceSiteId", sourceSiteId)
                .param("fingerprint", fingerprint)
                .param("now", now)
                .query(UUID.class)
                .optional()
                .isPresent();
    }

    public void insertTicketItem(UUID ticketId, int lineNo, String dishId, String dishName, int quantity) {
        jdbc.sql("""
                INSERT INTO %s (ticket_id, line_no, dish_id, dish_name_snapshot, quantity)
                VALUES (:ticketId, :lineNo, :dishId, :dishName, :quantity)
                """.formatted(ticketItems))
                .param("ticketId", ticketId)
                .param("lineNo", lineNo)
                .param("dishId", dishId)
                .param("dishName", dishName)
                .param("quantity", quantity)
                .update();
    }

    public Optional<TicketRecord> findTicketRecordByOrderId(UUID orderId) {
        return jdbc.sql("""
                SELECT ticket_id, order_id, source_type, source_site_id, request_fingerprint,
                       status, created_at, completed_at
                FROM %s
                WHERE order_id = :orderId
                """.formatted(tickets))
                .param("orderId", orderId)
                .query((rs, rowNum) -> new TicketRecord(
                        rs.getObject("ticket_id", UUID.class),
                        rs.getObject("order_id", UUID.class),
                        rs.getString("source_type"),
                        rs.getString("source_site_id"),
                        rs.getString("request_fingerprint").trim(),
                        rs.getString("status"),
                        rs.getObject("created_at", OffsetDateTime.class),
                        rs.getObject("completed_at", OffsetDateTime.class)
                ))
                .optional();
    }

    public Optional<TicketRecord> lockTicketForUpdate(UUID ticketId) {
        return jdbc.sql("""
                SELECT ticket_id, order_id, source_type, source_site_id, request_fingerprint,
                       status, created_at, completed_at
                FROM %s
                WHERE ticket_id = :ticketId
                FOR UPDATE
                """.formatted(tickets))
                .param("ticketId", ticketId)
                .query((rs, rowNum) -> new TicketRecord(
                        rs.getObject("ticket_id", UUID.class),
                        rs.getObject("order_id", UUID.class),
                        rs.getString("source_type"),
                        rs.getString("source_site_id"),
                        rs.getString("request_fingerprint").trim(),
                        rs.getString("status"),
                        rs.getObject("created_at", OffsetDateTime.class),
                        rs.getObject("completed_at", OffsetDateTime.class)
                ))
                .optional();
    }

    public void markTicketDone(UUID ticketId, OffsetDateTime now) {
        int updated = jdbc.sql("""
                UPDATE %s
                SET status = 'DONE',
                    completed_at = :now,
                    updated_at = :now
                WHERE ticket_id = :ticketId
                  AND status = 'WAITING'
                """.formatted(tickets))
                .param("now", now)
                .param("ticketId", ticketId)
                .update();
        if (updated != 1) {
            throw new IllegalStateException("Expected 1 WAITING ticket row to mark DONE for ticketId=" + ticketId
                    + ", updatedRows=" + updated);
        }
    }

    public void insertOutboxDoneEvent(
            UUID outboxEventId,
            UUID ticketId,
            UUID orderId,
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
                .param("aggregateId", ticketId)
                .param("eventType", EventType.Done.name())
                .param("topic", EventTopics.STATUS_EVENTS)
                .param("messageKey", orderId.toString())
                .param("payload", payloadJson)
                .param("now", now)
                .update();
    }

    public List<KitchenTicketResponse> listTickets(String statusFilter) {
        String sql;
        JdbcClient.StatementSpec spec;
        if (statusFilter != null && !statusFilter.isBlank()) {
            String orderBy = "DONE".equals(statusFilter)
                    ? "completed_at DESC, ticket_id DESC"
                    : "created_at ASC, ticket_id ASC";
            sql = """
                    SELECT ticket_id, order_id, source_type, source_site_id, status, created_at, completed_at
                    FROM %s
                    WHERE status = :status
                    ORDER BY %s
                    LIMIT 100
                    """.formatted(tickets, orderBy);
            spec = jdbc.sql(sql).param("status", statusFilter);
        } else {
            sql = """
                    SELECT ticket_id, order_id, source_type, source_site_id, status, created_at, completed_at
                    FROM %s
                    ORDER BY CASE WHEN status = 'WAITING' THEN 0 ELSE 1 END,
                             CASE WHEN status = 'WAITING' THEN created_at END ASC NULLS LAST,
                             completed_at DESC NULLS LAST,
                             ticket_id DESC
                    LIMIT 100
                    """.formatted(tickets);
            spec = jdbc.sql(sql);
        }

        List<TicketRow> rows = spec.query((rs, rowNum) -> new TicketRow(
                rs.getObject("ticket_id", UUID.class),
                rs.getObject("order_id", UUID.class),
                rs.getString("source_type"),
                rs.getString("source_site_id"),
                rs.getString("status"),
                rs.getObject("created_at", OffsetDateTime.class),
                rs.getObject("completed_at", OffsetDateTime.class)
        )).list();

        if (rows.isEmpty()) {
            return List.of();
        }

        List<UUID> ticketIds = rows.stream().map(TicketRow::ticketId).toList();
        Map<UUID, List<KitchenTicketResponse.Item>> itemsByTicket = new LinkedHashMap<>();
        for (UUID id : ticketIds) {
            itemsByTicket.put(id, new ArrayList<>());
        }

        jdbc.sql("""
                SELECT ticket_id, line_no, dish_id, dish_name_snapshot, quantity
                FROM %s
                WHERE ticket_id IN (:ids)
                ORDER BY ticket_id, line_no
                """.formatted(ticketItems))
                .param("ids", ticketIds)
                .query((rs, rowNum) -> {
                    UUID tid = rs.getObject("ticket_id", UUID.class);
                    var item = new KitchenTicketResponse.Item(
                            rs.getInt("line_no"),
                            rs.getString("dish_id"),
                            rs.getString("dish_name_snapshot"),
                            rs.getInt("quantity")
                    );
                    itemsByTicket.computeIfAbsent(tid, k -> new ArrayList<>()).add(item);
                    return null;
                }).list();

        List<KitchenTicketResponse> result = new ArrayList<>(rows.size());
        for (TicketRow row : rows) {
            result.add(new KitchenTicketResponse(
                    row.ticketId(),
                    row.orderId(),
                    row.sourceType(),
                    row.sourceSiteId(),
                    row.status(),
                    row.createdAt().toInstant(),
                    row.completedAt() == null ? null : row.completedAt().toInstant(),
                    itemsByTicket.getOrDefault(row.ticketId(), Collections.emptyList())
            ));
        }
        return Collections.unmodifiableList(result);
    }

    public Optional<KitchenTicketResponse> findTicketById(UUID ticketId) {
        Optional<TicketRow> opt = jdbc.sql("""
                SELECT ticket_id, order_id, source_type, source_site_id, status, created_at, completed_at
                FROM %s
                WHERE ticket_id = :id
                """.formatted(tickets))
                .param("id", ticketId)
                .query((rs, rowNum) -> new TicketRow(
                        rs.getObject("ticket_id", UUID.class),
                        rs.getObject("order_id", UUID.class),
                        rs.getString("source_type"),
                        rs.getString("source_site_id"),
                        rs.getString("status"),
                        rs.getObject("created_at", OffsetDateTime.class),
                        rs.getObject("completed_at", OffsetDateTime.class)
                )).optional();

        if (opt.isEmpty()) {
            return Optional.empty();
        }

        TicketRow row = opt.get();
        List<KitchenTicketResponse.Item> items = jdbc.sql("""
                SELECT line_no, dish_id, dish_name_snapshot, quantity
                FROM %s
                WHERE ticket_id = :id
                ORDER BY line_no
                """.formatted(ticketItems))
                .param("id", ticketId)
                .query((rs, rowNum) -> new KitchenTicketResponse.Item(
                        rs.getInt("line_no"),
                        rs.getString("dish_id"),
                        rs.getString("dish_name_snapshot"),
                        rs.getInt("quantity")
                )).list();

        return Optional.of(new KitchenTicketResponse(
                row.ticketId(),
                row.orderId(),
                row.sourceType(),
                row.sourceSiteId(),
                row.status(),
                row.createdAt().toInstant(),
                row.completedAt() == null ? null : row.completedAt().toInstant(),
                items
        ));
    }

    public record TicketRecord(
            UUID ticketId,
            UUID orderId,
            String sourceType,
            String sourceSiteId,
            String requestFingerprint,
            String status,
            OffsetDateTime createdAt,
            OffsetDateTime completedAt
    ) {
    }

    private record TicketRow(
            UUID ticketId,
            UUID orderId,
            String sourceType,
            String sourceSiteId,
            String status,
            OffsetDateTime createdAt,
            OffsetDateTime completedAt
    ) {
    }
}
