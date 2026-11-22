package vn.edu.orders.query.infrastructure;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import vn.edu.orders.query.api.BranchSummary;
import vn.edu.orders.query.api.ReplicatedOrderItem;
import vn.edu.orders.query.api.ReplicatedOrderSummary;
import vn.edu.orders.query.api.SubscriptionStatus;

@Repository
public class ReportingStore {
    private static final List<String> KNOWN_SITES = List.of("POS01", "POS02", "ONLINE");
    private static final List<String> KNOWN_STATUSES = List.of(
            "PENDING_INVENTORY", "RESERVED", "DONE", "CANCELLED", "FAILED");

    private static final String ORDER_COLUMNS = """
            order_id, site_id, source_type, status, status_rank, failure_reason, close_reason,
            cancel_flag, cancel_requested_at, cancel_message, version, created_at, updated_at
            """;

    private static final RowMapper<ReplicatedOrderSummary> ORDER_MAPPER = (rs, rowNum) -> new ReplicatedOrderSummary(
            rs.getObject("order_id", UUID.class),
            rs.getString("site_id"),
            rs.getString("source_type"),
            rs.getString("status"),
            rs.getInt("status_rank"),
            rs.getString("failure_reason"),
            rs.getString("close_reason"),
            rs.getString("cancel_flag"),
            toInstant(rs.getObject("cancel_requested_at", OffsetDateTime.class)),
            rs.getString("cancel_message"),
            rs.getLong("version"),
            rs.getObject("created_at", OffsetDateTime.class).toInstant(),
            rs.getObject("updated_at", OffsetDateTime.class).toInstant()
    );

    private static final RowMapper<ReplicatedOrderItem> ITEM_MAPPER = (rs, rowNum) -> new ReplicatedOrderItem(
            rs.getInt("line_no"),
            rs.getString("dish_id"),
            rs.getString("dish_name_snapshot"),
            rs.getInt("quantity")
    );

    private final JdbcClient jdbc;
    private final TransactionTemplate readOnlyTx;
    private final String allOrdersView;
    private final String allOrderItemsView;
    private final String allHeartbeatsView;

    public ReportingStore(
            JdbcClient jdbc,
            PlatformTransactionManager transactionManager,
            @Value("${query.reporting-schema:reporting}") String reportingSchema
    ) {
        this.jdbc = jdbc;
        this.readOnlyTx = new TransactionTemplate(transactionManager);
        this.readOnlyTx.setReadOnly(true);
        // Single MVCC snapshot keeps status totals, reason breakdown, and dataAsOf mutually consistent.
        this.readOnlyTx.setIsolationLevel(TransactionDefinition.ISOLATION_REPEATABLE_READ);
        String schemaPrefix = "\"" + reportingSchema.replace("\"", "\"\"") + "\".";
        this.allOrdersView = schemaPrefix + "all_orders";
        this.allOrderItemsView = schemaPrefix + "all_order_items";
        this.allHeartbeatsView = schemaPrefix + "all_heartbeats";
    }

    public record OrderDetails(ReplicatedOrderSummary order, List<ReplicatedOrderItem> items) {
    }

    public record SummarySnapshot(
            long totalOrders,
            Map<String, Long> bySite,
            Map<String, Long> byStatus,
            Map<String, Long> byCancelledReason,
            Map<String, Long> byFailedReason,
            List<BranchSummary> branches,
            List<SubscriptionStatus> subscriptions
    ) {
    }

    public List<ReplicatedOrderSummary> findOrders(String siteId, String status, int limit) {
        return readOnlyTx.execute(tx -> queryOrders(siteId, status, limit));
    }

    public Optional<OrderDetails> findOrderDetails(UUID orderId) {
        return readOnlyTx.execute(tx -> {
            Optional<ReplicatedOrderSummary> order = queryOrderById(orderId);
            if (order.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(new OrderDetails(order.get(), queryOrderItems(orderId)));
        });
    }

    public SummarySnapshot loadSummary() {
        return readOnlyTx.execute(tx -> {
            Map<String, Long> bySite = emptySiteCounts();
            Map<String, Long> byStatus = emptyStatusCounts();
            Map<String, Long> byCancelledReason = new LinkedHashMap<>();
            Map<String, Long> byFailedReason = new LinkedHashMap<>();
            Map<String, BranchAccumulator> branchMap = emptyBranchAccumulators();
            long total = 0L;

            for (var row : queryGroupedCounts()) {
                bySite.merge(row.siteId(), row.count(), Long::sum);
                byStatus.merge(row.status(), row.count(), Long::sum);
                total += row.count();

                BranchAccumulator branch = branchMap.get(row.siteId());
                if (branch != null) {
                    branch.addStatusCount(row.status(), row.count());
                }
            }

            for (var row : queryReasonCounts()) {
                BranchAccumulator branch = branchMap.get(row.siteId());
                if ("CANCELLED".equals(row.status())) {
                    byCancelledReason.merge(row.reason(), row.count(), Long::sum);
                    if (branch != null) {
                        branch.cancelledByReason.merge(row.reason(), row.count(), Long::sum);
                    }
                } else if ("FAILED".equals(row.status())) {
                    byFailedReason.merge(row.reason(), row.count(), Long::sum);
                    if (branch != null) {
                        branch.failedByReason.merge(row.reason(), row.count(), Long::sum);
                    }
                }
            }

            for (var heartbeat : queryHeartbeats()) {
                BranchAccumulator branch = branchMap.get(heartbeat.branchId());
                if (branch != null) {
                    branch.dataAsOf = heartbeat.beatAt();
                }
            }

            List<BranchSummary> branches = new ArrayList<>(KNOWN_SITES.size());
            for (String site : KNOWN_SITES) {
                branches.add(branchMap.get(site).toSummary());
            }

            return new SummarySnapshot(
                    total,
                    bySite,
                    byStatus,
                    sortReasonMap(byCancelledReason),
                    sortReasonMap(byFailedReason),
                    List.copyOf(branches),
                    querySubscriptions()
            );
        });
    }

    private List<ReplicatedOrderSummary> queryOrders(String siteId, String status, int limit) {
        StringBuilder sql = new StringBuilder("""
                SELECT %s
                FROM %s
                WHERE 1 = 1
                """.formatted(ORDER_COLUMNS, allOrdersView));

        if (siteId != null) {
            sql.append(" AND site_id = :siteId");
        }
        if (status != null) {
            sql.append(" AND status = :status");
        }
        sql.append(" ORDER BY created_at DESC, order_id DESC LIMIT :limit");

        var spec = jdbc.sql(sql.toString()).param("limit", limit);
        if (siteId != null) {
            spec = spec.param("siteId", siteId);
        }
        if (status != null) {
            spec = spec.param("status", status);
        }
        return spec.query(ORDER_MAPPER).list();
    }

    private Optional<ReplicatedOrderSummary> queryOrderById(UUID orderId) {
        return jdbc.sql("""
                SELECT %s
                FROM %s
                WHERE order_id = :orderId
                """.formatted(ORDER_COLUMNS, allOrdersView))
                .param("orderId", orderId)
                .query(ORDER_MAPPER)
                .optional();
    }

    private List<ReplicatedOrderItem> queryOrderItems(UUID orderId) {
        return jdbc.sql("""
                SELECT line_no, dish_id, dish_name_snapshot, quantity
                FROM %s
                WHERE order_id = :orderId
                ORDER BY line_no
                """.formatted(allOrderItemsView))
                .param("orderId", orderId)
                .query(ITEM_MAPPER)
                .list();
    }

    private record SiteStatusCount(String siteId, String status, long count) {
    }

    private record SiteReasonCount(String siteId, String status, String reason, long count) {
    }

    private record BranchHeartbeat(String branchId, Instant beatAt) {
    }

    private List<SiteStatusCount> queryGroupedCounts() {
        return jdbc.sql("""
                SELECT site_id, status, COUNT(*) AS cnt
                FROM %s
                WHERE status IN ('PENDING_INVENTORY', 'RESERVED', 'DONE', 'CANCELLED', 'FAILED')
                GROUP BY site_id, status
                ORDER BY site_id, status
                """.formatted(allOrdersView))
                .query((rs, rowNum) -> new SiteStatusCount(
                        rs.getString("site_id"),
                        rs.getString("status"),
                        rs.getLong("cnt")))
                .list();
    }

    private List<SiteReasonCount> queryReasonCounts() {
        return jdbc.sql("""
                SELECT site_id,
                       status,
                       COALESCE(NULLIF(BTRIM(close_reason), ''), NULLIF(BTRIM(failure_reason), ''), 'UNKNOWN') AS reason,
                       COUNT(*) AS cnt
                FROM %s
                WHERE status IN ('CANCELLED', 'FAILED')
                GROUP BY site_id, status, reason
                ORDER BY site_id, status, cnt DESC, reason ASC
                """.formatted(allOrdersView))
                .query((rs, rowNum) -> new SiteReasonCount(
                        rs.getString("site_id"),
                        rs.getString("status"),
                        rs.getString("reason"),
                        rs.getLong("cnt")))
                .list();
    }

    private List<BranchHeartbeat> queryHeartbeats() {
        return jdbc.sql("""
                SELECT branch_id, beat_at
                FROM %s
                ORDER BY branch_id
                """.formatted(allHeartbeatsView))
                .query((rs, rowNum) -> new BranchHeartbeat(
                        rs.getString("branch_id"),
                        toInstant(rs.getObject("beat_at", OffsetDateTime.class))))
                .list();
    }

    private List<SubscriptionStatus> querySubscriptions() {
        return jdbc.sql("""
                SELECT s.subname,
                       s.subenabled,
                       st.pid,
                       st.last_msg_send_time,
                       st.last_msg_receipt_time,
                       st.latest_end_time
                FROM pg_subscription s
                LEFT JOIN pg_stat_subscription st
                       ON st.subname = s.subname
                      AND st.relid IS NULL
                      AND st.leader_pid IS NULL
                ORDER BY s.subname
                """)
                .query((rs, rowNum) -> new SubscriptionStatus(
                        rs.getString("subname"),
                        rs.getBoolean("subenabled"),
                        rs.getObject("pid", Integer.class),
                        toInstant(rs.getObject("last_msg_send_time", OffsetDateTime.class)),
                        toInstant(rs.getObject("last_msg_receipt_time", OffsetDateTime.class)),
                        toInstant(rs.getObject("latest_end_time", OffsetDateTime.class))
                ))
                .list();
    }

    private static Map<String, Long> emptySiteCounts() {
        Map<String, Long> result = new LinkedHashMap<>();
        for (String site : KNOWN_SITES) {
            result.put(site, 0L);
        }
        return result;
    }

    private static Map<String, Long> emptyStatusCounts() {
        Map<String, Long> result = new LinkedHashMap<>();
        for (String status : KNOWN_STATUSES) {
            result.put(status, 0L);
        }
        return result;
    }

    private static Map<String, BranchAccumulator> emptyBranchAccumulators() {
        Map<String, BranchAccumulator> map = new LinkedHashMap<>();
        for (String site : KNOWN_SITES) {
            map.put(site, new BranchAccumulator(site));
        }
        return map;
    }

    private static Map<String, Long> sortReasonMap(Map<String, Long> source) {
        Map<String, Long> sorted = new LinkedHashMap<>();
        source.entrySet().stream()
                .sorted(Map.Entry.<String, Long>comparingByValue(Comparator.reverseOrder())
                        .thenComparing(Map.Entry.comparingByKey()))
                .forEach(entry -> sorted.put(entry.getKey(), entry.getValue()));
        return sorted;
    }

    private static Instant toInstant(OffsetDateTime odt) {
        return odt != null ? odt.toInstant() : null;
    }

    private static final class BranchAccumulator {
        private final String branchId;
        private Instant dataAsOf;
        private long totalOrders;
        private final Map<String, Long> byStatus = emptyStatusCounts();
        private final Map<String, Long> cancelledByReason = new LinkedHashMap<>();
        private final Map<String, Long> failedByReason = new LinkedHashMap<>();

        private BranchAccumulator(String branchId) {
            this.branchId = branchId;
        }

        private void addStatusCount(String status, long count) {
            totalOrders += count;
            byStatus.merge(status, count, Long::sum);
        }

        private BranchSummary toSummary() {
            return new BranchSummary(
                    branchId,
                    dataAsOf,
                    totalOrders,
                    byStatus,
                    sortReasonMap(cancelledByReason),
                    sortReasonMap(failedByReason)
            );
        }
    }
}
