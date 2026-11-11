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

    public List<KitchenTicketResponse> listTickets(String statusFilter) {
        String sql;
        JdbcClient.StatementSpec spec;
        if (statusFilter != null && !statusFilter.isBlank()) {
            sql = """
                    SELECT ticket_id, order_id, source_type, source_site_id, status, created_at, completed_at
                    FROM %s
                    WHERE status = :status
                    ORDER BY CASE WHEN status = 'WAITING' THEN created_at END ASC,
                             completed_at DESC,
                             ticket_id DESC
                    LIMIT 100
                    """.formatted(tickets);
            spec = jdbc.sql(sql).param("status", statusFilter);
        } else {
            sql = """
                    SELECT ticket_id, order_id, source_type, source_site_id, status, created_at, completed_at
                    FROM %s
                    ORDER BY CASE WHEN status = 'WAITING' THEN 0 ELSE 1 END,
                             created_at ASC,
                             completed_at DESC,
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
