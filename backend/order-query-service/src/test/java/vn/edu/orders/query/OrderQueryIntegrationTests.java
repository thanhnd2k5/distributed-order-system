package vn.edu.orders.query;

import com.jayway.jsonpath.JsonPath;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.simple.JdbcClient;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "spring.flyway.default-schema=b5_query_test_reporting",
        "spring.flyway.schemas=b5_query_test_reporting",
        "spring.flyway.placeholders.pos01_schema=b5_query_test_pos01",
        "spring.flyway.placeholders.pos02_schema=b5_query_test_pos02",
        "spring.flyway.placeholders.online_schema=b5_query_test_online",
        "spring.flyway.placeholders.reporting_schema=b5_query_test_reporting",
        "query.reporting-schema=b5_query_test_reporting"
})
class OrderQueryIntegrationTests {
    static {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    }

    @Value("${local.server.port}")
    private int port;

    @Value("${spring.datasource.url}")
    private String dbUrl;

    @Value("${spring.flyway.user}")
    private String adminUser;

    @Value("${spring.flyway.password}")
    private String adminPassword;

    @Autowired
    private JdbcClient runtimeJdbc;

    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();

    @AfterEach
    void cleanupTestFragments() throws Exception {
        try (var conn = DriverManager.getConnection(dbUrl, adminUser, adminPassword);
             var stmt = conn.createStatement()) {
            stmt.execute("""
                    DELETE FROM b5_query_test_pos01.order_items;
                    DELETE FROM b5_query_test_pos01.orders;
                    DELETE FROM b5_query_test_pos01.replication_heartbeat;
                    DELETE FROM b5_query_test_pos02.order_items;
                    DELETE FROM b5_query_test_pos02.orders;
                    DELETE FROM b5_query_test_pos02.replication_heartbeat;
                    DELETE FROM b5_query_test_online.order_items;
                    DELETE FROM b5_query_test_online.orders;
                    DELETE FROM b5_query_test_online.replication_heartbeat;
                    """);
        }
    }

    @Test
    void healthAndReadOnlyPrivilegeEnforced() {
        assertThat(get("/actuator/health").statusCode()).isEqualTo(200);
        assertThat(JsonPath.<String>read(get("/actuator/health").body(), "$.status")).isEqualTo("UP");

        UUID id = UUID.randomUUID();
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        assertThatThrownBy(() -> runtimeJdbc.sql("""
                INSERT INTO b5_query_test_pos01.orders
                (order_id, client_request_id, site_id, source_type, status, version, created_at, updated_at)
                VALUES (:id, :req, 'POS01', 'POS', 'PENDING_INVENTORY', 0, :now, :now)
                """)
                .param("id", id)
                .param("req", UUID.randomUUID())
                .param("now", now)
                .update())
                .isInstanceOf(DataAccessException.class)
                .hasRootCauseInstanceOf(SQLException.class)
                .satisfies(ex -> {
                    DataAccessException dae = (DataAccessException) ex;
                    SQLException sqlEx = (SQLException) dae.getMostSpecificCause();
                    assertThat(sqlEx.getSQLState()).isEqualTo("42501");
                });

        assertThatThrownBy(() -> runtimeJdbc.sql("""
                INSERT INTO b5_query_test_pos01.replication_heartbeat (branch_id, beat_at)
                VALUES ('POS01', :now)
                """)
                .param("now", now)
                .update())
                .isInstanceOf(DataAccessException.class)
                .hasRootCauseInstanceOf(SQLException.class)
                .satisfies(ex -> {
                    DataAccessException dae = (DataAccessException) ex;
                    SQLException sqlEx = (SQLException) dae.getMostSpecificCause();
                    assertThat(sqlEx.getSQLState()).isEqualTo("42501");
                });

        assertThat(runtimeJdbc.sql("SELECT COUNT(*) FROM b5_query_test_reporting.all_orders")
                .query(Long.class).single()).isZero();
        assertThat(runtimeJdbc.sql("SELECT COUNT(*) FROM b5_query_test_reporting.all_heartbeats")
                .query(Long.class).single()).isZero();
    }

    @Test
    void fragmentConstraintsRejectCrossSiteWritesAndInvalidLifecycleStates() throws Exception {
        assertOrderConstraintRejects(
                "b5_query_test_pos01", "POS02", "POS", "PENDING_INVENTORY", "NONE", "chk_pos01_fragment_site");
        assertOrderConstraintRejects(
                "b5_query_test_pos02", "POS01", "POS", "PENDING_INVENTORY", "NONE", "chk_pos02_fragment_site");
        assertOrderConstraintRejects(
                "b5_query_test_online", "ONLINE", "POS", "PENDING_INVENTORY", "NONE", "chk_online_fragment_site");

        assertOrderConstraintRejects(
                "b5_query_test_pos01", "POS01", "POS", "CANCEL_PENDING", "NONE", "chk_pos01_orders_status");
        assertOrderConstraintRejects(
                "b5_query_test_pos01", "POS01", "POS", "PENDING_INVENTORY", "UNKNOWN_FLAG", "chk_pos01_orders_cancel_flag");

        assertHeartbeatConstraintRejects("b5_query_test_pos01", "POS02", "chk_pos01_heartbeat_branch");
        assertHeartbeatConstraintRejects("b5_query_test_pos02", "ONLINE", "chk_pos02_heartbeat_branch");
        assertHeartbeatConstraintRejects("b5_query_test_online", "POS01", "chk_online_heartbeat_branch");
    }

    @Test
    void reportingEndpointsReconstructAllFragmentsExposeLifecycleFieldsAndSupportFilters() throws Exception {
        UUID pos01DoneId = UUID.randomUUID();
        UUID pos01CancelledId = UUID.randomUUID();
        UUID pos02FailedId = UUID.randomUUID();
        UUID onlineReservedId = UUID.randomUUID();

        OffsetDateTime t1 = OffsetDateTime.parse("2026-11-22T01:00:00Z");
        OffsetDateTime t2 = OffsetDateTime.parse("2026-11-22T01:01:00Z");
        OffsetDateTime t3 = OffsetDateTime.parse("2026-11-22T01:02:00Z");
        OffsetDateTime t4 = OffsetDateTime.parse("2026-11-22T01:03:00Z");
        OffsetDateTime cancelReqAt = OffsetDateTime.parse("2026-11-22T01:02:15Z");
        OffsetDateTime hbPos01 = OffsetDateTime.parse("2026-11-22T01:05:00Z");
        OffsetDateTime hbPos02 = OffsetDateTime.parse("2026-11-22T01:05:05Z");

        try (var conn = DriverManager.getConnection(dbUrl, adminUser, adminPassword)) {
            insertOrder(conn, "b5_query_test_pos01", pos01DoneId, "POS01", "POS",
                    "DONE", 2, null, null, "NONE", null,
                    "Cannot cancel: the kitchen has started cooking", 3, t1, t1);
            insertOrderItem(conn, "b5_query_test_pos01", pos01DoneId, 1, "COM_GA", "Cơm gà", 2);

            insertOrder(conn, "b5_query_test_pos01", pos01CancelledId, "POS01", "POS",
                    "CANCELLED", 2, null, "CUSTOMER_CHANGED_MIND", "NONE", null, null, 2, t2, t2);
            insertOrderItem(conn, "b5_query_test_pos01", pos01CancelledId, 1, "CHAO_GA", "Cháo gà", 1);

            insertOrder(conn, "b5_query_test_pos02", pos02FailedId, "POS02", "POS",
                    "FAILED", 2, "INSUFFICIENT_INGREDIENTS: Thiếu gạo", "INSUFFICIENT_INGREDIENTS",
                    "NONE", null, null, 1, t3, t3);
            insertOrderItem(conn, "b5_query_test_pos02", pos02FailedId, 1, "CHAO_GA", "Cháo gà", 1);

            insertOrder(conn, "b5_query_test_online", onlineReservedId, "ONLINE", "ONLINE",
                    "RESERVED", 1, null, null, "KITCHEN_WILL_NOT_COOK", cancelReqAt, null, 2, t4, t4);
            insertOrderItem(conn, "b5_query_test_online", onlineReservedId, 1, "CHAO_GA", "Cháo gà", 1);
            insertOrderItem(conn, "b5_query_test_online", onlineReservedId, 2, "COM_GA", "Cơm gà", 1);

            upsertHeartbeat(conn, "b5_query_test_pos01", "POS01", hbPos01);
            upsertHeartbeat(conn, "b5_query_test_pos02", "POS02", hbPos02);
        }

        var allResp = get("/api/reporting/orders");
        assertThat(allResp.statusCode()).isEqualTo(200);
        assertThat(JsonPath.<Integer>read(allResp.body(), "$.count")).isEqualTo(4);
        assertThat(JsonPath.<String>read(allResp.body(), "$.replicaNote")).contains("asynchronous logical replication");
        assertThat(JsonPath.<List<String>>read(allResp.body(), "$.orders[*].orderId"))
                .containsExactly(
                        onlineReservedId.toString(),
                        pos02FailedId.toString(),
                        pos01CancelledId.toString(),
                        pos01DoneId.toString());
        assertThat(JsonPath.<Integer>read(allResp.body(), "$.orders[0].statusRank")).isEqualTo(1);
        assertThat(JsonPath.<String>read(allResp.body(), "$.orders[0].cancelFlag")).isEqualTo("KITCHEN_WILL_NOT_COOK");
        assertThat(Instant.parse(JsonPath.read(allResp.body(), "$.orders[0].cancelRequestedAt")))
                .isEqualTo(cancelReqAt.toInstant());
        assertThat(JsonPath.<String>read(allResp.body(), "$.orders[2].closeReason")).isEqualTo("CUSTOMER_CHANGED_MIND");
        assertThat(JsonPath.<String>read(allResp.body(), "$.orders[3].cancelMessage"))
                .isEqualTo("Cannot cancel: the kitchen has started cooking");

        var bySite = get("/api/reporting/orders?siteId=pos02");
        assertThat(bySite.statusCode()).isEqualTo(200);
        assertThat(JsonPath.<List<String>>read(bySite.body(), "$.orders[*].orderId"))
                .containsExactly(pos02FailedId.toString());

        var byCancelled = get("/api/reporting/orders?status=CANCELLED");
        assertThat(byCancelled.statusCode()).isEqualTo(200);
        assertThat(JsonPath.<List<String>>read(byCancelled.body(), "$.orders[*].orderId"))
                .containsExactly(pos01CancelledId.toString());

        var details = get("/api/reporting/orders/" + onlineReservedId);
        assertThat(details.statusCode()).isEqualTo(200);
        assertThat(JsonPath.<String>read(details.body(), "$.order.sourceSiteId")).isEqualTo("ONLINE");
        assertThat(JsonPath.<Integer>read(details.body(), "$.order.statusRank")).isEqualTo(1);
        assertThat(JsonPath.<String>read(details.body(), "$.order.cancelFlag")).isEqualTo("KITCHEN_WILL_NOT_COOK");
        assertThat(Instant.parse(JsonPath.read(details.body(), "$.order.cancelRequestedAt")))
                .isEqualTo(cancelReqAt.toInstant());
        assertThat(JsonPath.<List<String>>read(details.body(), "$.items[*].dishId"))
                .containsExactly("CHAO_GA", "COM_GA");

        var summary = get("/api/reporting/summary");
        assertThat(summary.statusCode()).isEqualTo(200);
        assertThat(JsonPath.<Integer>read(summary.body(), "$.totalOrders")).isEqualTo(4);
        assertThat(JsonPath.<Integer>read(summary.body(), "$.bySite.POS01")).isEqualTo(2);
        assertThat(JsonPath.<Integer>read(summary.body(), "$.bySite.POS02")).isEqualTo(1);
        assertThat(JsonPath.<Integer>read(summary.body(), "$.bySite.ONLINE")).isEqualTo(1);
        assertThat(JsonPath.<Integer>read(summary.body(), "$.byStatus.DONE")).isEqualTo(1);
        assertThat(JsonPath.<Integer>read(summary.body(), "$.byStatus.CANCELLED")).isEqualTo(1);
        assertThat(JsonPath.<Integer>read(summary.body(), "$.byStatus.FAILED")).isEqualTo(1);
        assertThat(JsonPath.<Integer>read(summary.body(), "$.byStatus.RESERVED")).isEqualTo(1);
        assertThat(JsonPath.<Integer>read(summary.body(), "$.byStatus.PENDING_INVENTORY")).isEqualTo(0);
        assertThat(JsonPath.<Map<String, Object>>read(summary.body(), "$.byStatus"))
                .doesNotContainKey("CANCEL_PENDING");
        assertThat(JsonPath.<Integer>read(summary.body(), "$.byCancelledReason.CUSTOMER_CHANGED_MIND")).isEqualTo(1);
        assertThat(JsonPath.<Integer>read(summary.body(), "$.byFailedReason.INSUFFICIENT_INGREDIENTS")).isEqualTo(1);

        List<Map<String, Object>> branches = JsonPath.read(summary.body(), "$.branches");
        assertThat(branches).hasSize(3);
        assertThat(JsonPath.<String>read(summary.body(), "$.branches[0].branchId")).isEqualTo("POS01");
        assertThat(Instant.parse(JsonPath.read(summary.body(), "$.branches[0].dataAsOf")))
                .isEqualTo(hbPos01.toInstant());
        assertThat(JsonPath.<Integer>read(summary.body(), "$.branches[0].totalOrders")).isEqualTo(2);
        assertThat(JsonPath.<Integer>read(summary.body(), "$.branches[0].cancelledByReason.CUSTOMER_CHANGED_MIND"))
                .isEqualTo(1);

        assertThat(JsonPath.<String>read(summary.body(), "$.branches[1].branchId")).isEqualTo("POS02");
        assertThat(Instant.parse(JsonPath.read(summary.body(), "$.branches[1].dataAsOf")))
                .isEqualTo(hbPos02.toInstant());
        assertThat(JsonPath.<Integer>read(summary.body(), "$.branches[1].failedByReason.INSUFFICIENT_INGREDIENTS"))
                .isEqualTo(1);

        assertThat(JsonPath.<String>read(summary.body(), "$.branches[2].branchId")).isEqualTo("ONLINE");
        assertThat(JsonPath.<Object>read(summary.body(), "$.branches[2].dataAsOf")).isNull();
        assertThat(JsonPath.<Integer>read(summary.body(), "$.branches[2].byStatus.RESERVED")).isEqualTo(1);
        assertThat(JsonPath.<Integer>read(summary.body(), "$.branches[2].byStatus.CANCELLED")).isEqualTo(0);
        assertThat(JsonPath.<List<Object>>read(summary.body(), "$.subscriptions")).isNotNull();

        assertThat(get("/api/reporting/orders?siteId=UNKNOWN").statusCode()).isEqualTo(400);
        assertThat(get("/api/reporting/orders?status=INVALID").statusCode()).isEqualTo(400);
        assertThat(get("/api/reporting/orders?status=CANCEL_PENDING").statusCode()).isEqualTo(400);
        assertThat(get("/api/reporting/orders?limit=0").statusCode()).isEqualTo(400);
        assertThat(get("/api/reporting/orders?limit=501").statusCode()).isEqualTo(400);
        assertThat(get("/api/reporting/orders/" + UUID.randomUUID()).statusCode()).isEqualTo(404);
    }

    @Test
    @Tag("kb-55")
    void kb55_branchDataAsOfFreezesWhenReplicationCutAndCatchesUpAfterReconnect() throws Exception {
        OffsetDateTime t1 = OffsetDateTime.parse("2026-11-22T02:00:00Z");
        OffsetDateTime t2 = OffsetDateTime.parse("2026-11-22T02:01:00Z");
        OffsetDateTime t3 = OffsetDateTime.parse("2026-11-22T02:02:30Z");

        UUID pos01DoneId = UUID.randomUUID();
        UUID pos01CancelPendingAtKitchenId = UUID.randomUUID();
        UUID pos02ReservedId = UUID.randomUUID();

        // Phase 1: Initial replicated state for POS01, POS02, ONLINE at T1.
        // pos01CancelPendingAtKitchenId has cancel_flag='KITCHEN_WILL_NOT_COOK' while branch status is PENDING_INVENTORY.
        try (var conn = DriverManager.getConnection(dbUrl, adminUser, adminPassword)) {
            upsertHeartbeat(conn, "b5_query_test_pos01", "POS01", t1);
            upsertHeartbeat(conn, "b5_query_test_pos02", "POS02", t1);
            upsertHeartbeat(conn, "b5_query_test_online", "ONLINE", t1);

            insertOrder(conn, "b5_query_test_pos01", pos01DoneId, "POS01", "POS",
                    "DONE", 2, null, null, "NONE", null, null, 2, t1, t1);
            insertOrder(conn, "b5_query_test_pos01", pos01CancelPendingAtKitchenId, "POS01", "POS",
                    "PENDING_INVENTORY", 0, null, null, "KITCHEN_WILL_NOT_COOK", t1, null, 0, t1, t1);
            insertOrder(conn, "b5_query_test_pos02", pos02ReservedId, "POS02", "POS",
                    "RESERVED", 1, null, null, "NONE", null, null, 1, t1, t1);
        }

        var phase1 = get("/api/reporting/summary");
        assertThat(phase1.statusCode()).isEqualTo(200);
        assertThat(Instant.parse(JsonPath.read(phase1.body(), "$.branches[0].dataAsOf"))).isEqualTo(t1.toInstant());
        assertThat(Instant.parse(JsonPath.read(phase1.body(), "$.branches[1].dataAsOf"))).isEqualTo(t1.toInstant());
        assertThat(JsonPath.<Integer>read(phase1.body(), "$.branches[0].totalOrders")).isEqualTo(2);
        assertThat(JsonPath.<Integer>read(phase1.body(), "$.branches[0].byStatus.PENDING_INVENTORY")).isEqualTo(1);
        assertThat(JsonPath.<Integer>read(phase1.body(), "$.branches[0].byStatus.CANCELLED")).isEqualTo(0);

        // Phase 2: POS01 replication is cut (no POS01 updates reach central-db), while POS02 keeps replicating to T2.
        UUID pos02FailedId = UUID.randomUUID();
        UUID pos02RejectedId = UUID.randomUUID();
        try (var conn = DriverManager.getConnection(dbUrl, adminUser, adminPassword)) {
            upsertHeartbeat(conn, "b5_query_test_pos02", "POS02", t2);
            upsertHeartbeat(conn, "b5_query_test_online", "ONLINE", t2);

            insertOrder(conn, "b5_query_test_pos02", pos02FailedId, "POS02", "POS",
                    "FAILED", 2, null, "INSUFFICIENT_INGREDIENTS", "NONE", null, null, 1, t2, t2);
            insertOrder(conn, "b5_query_test_pos02", pos02RejectedId, "POS02", "POS",
                    "CANCELLED", 2, null, "EQUIPMENT_FAILURE", "NONE", null, null, 2, t2, t2);
        }

        var phase2 = get("/api/reporting/summary");
        assertThat(phase2.statusCode()).isEqualTo(200);
        // POS01 dataAsOf stays frozen at T1 while POS02 moves to T2.
        assertThat(Instant.parse(JsonPath.read(phase2.body(), "$.branches[0].dataAsOf"))).isEqualTo(t1.toInstant());
        assertThat(Instant.parse(JsonPath.read(phase2.body(), "$.branches[1].dataAsOf"))).isEqualTo(t2.toInstant());
        assertThat(Instant.parse(JsonPath.read(phase2.body(), "$.branches[2].dataAsOf"))).isEqualTo(t2.toInstant());
        assertThat(JsonPath.<Integer>read(phase2.body(), "$.branches[0].totalOrders")).isEqualTo(2);
        assertThat(JsonPath.<Integer>read(phase2.body(), "$.branches[1].totalOrders")).isEqualTo(3);
        assertThat(JsonPath.<Integer>read(phase2.body(), "$.branches[1].failedByReason.INSUFFICIENT_INGREDIENTS"))
                .isEqualTo(1);
        assertThat(JsonPath.<Integer>read(phase2.body(), "$.branches[1].cancelledByReason.EQUIPMENT_FAILURE"))
                .isEqualTo(1);

        // Phase 3: POS01 replication reconnects and buffered changes catch up at T3.
        try (var conn = DriverManager.getConnection(dbUrl, adminUser, adminPassword)) {
            upsertHeartbeat(conn, "b5_query_test_pos01", "POS01", t3);
            upsertHeartbeat(conn, "b5_query_test_pos02", "POS02", t3);
            upsertHeartbeat(conn, "b5_query_test_online", "ONLINE", t3);

            try (var ps = conn.prepareStatement("""
                    UPDATE b5_query_test_pos01.orders
                    SET status = 'CANCELLED',
                        status_rank = 2,
                        close_reason = 'CUSTOMER_WAITED_TOO_LONG',
                        cancel_flag = 'NONE',
                        version = version + 1,
                        updated_at = ?
                    WHERE order_id = ?
                    """)) {
                ps.setObject(1, t3);
                ps.setObject(2, pos01CancelPendingAtKitchenId);
                ps.executeUpdate();
            }

            insertOrder(conn, "b5_query_test_pos01", UUID.randomUUID(), "POS01", "POS",
                    "CANCELLED", 2, null, "CUSTOMER_CHANGED_MIND", "NONE", null, null, 1, t3, t3);
            insertOrder(conn, "b5_query_test_pos01", UUID.randomUUID(), "POS01", "POS",
                    "CANCELLED", 2, null, "CUSTOMER_CHANGED_MIND", "NONE", null, null, 1, t3, t3);
            insertOrder(conn, "b5_query_test_pos01", UUID.randomUUID(), "POS01", "POS",
                    "CANCELLED", 2, null, "SPOILED_INGREDIENT", "NONE", null, null, 2, t3, t3);
            insertOrder(conn, "b5_query_test_pos01", UUID.randomUUID(), "POS01", "POS",
                    "FAILED", 2, null, "ORDER_EXPIRED", "NONE", null, null, 1, t3, t3);
            insertOrder(conn, "b5_query_test_pos01", UUID.randomUUID(), "POS01", "POS",
                    "FAILED", 2, null, "INGREDIENT_AWAITING_STOCK_CHECK", "NONE", null, null, 1, t3, t3);
        }

        var phase3 = get("/api/reporting/summary");
        assertThat(phase3.statusCode()).isEqualTo(200);
        assertThat(Instant.parse(JsonPath.read(phase3.body(), "$.branches[0].dataAsOf"))).isEqualTo(t3.toInstant());
        assertThat(JsonPath.<Integer>read(phase3.body(), "$.totalOrders")).isEqualTo(10);
        assertThat(JsonPath.<Integer>read(phase3.body(), "$.branches[0].totalOrders")).isEqualTo(7);
        assertThat(JsonPath.<Integer>read(phase3.body(), "$.branches[0].byStatus.PENDING_INVENTORY")).isEqualTo(0);
        assertThat(JsonPath.<Integer>read(phase3.body(), "$.branches[0].byStatus.DONE")).isEqualTo(1);
        assertThat(JsonPath.<Integer>read(phase3.body(), "$.branches[0].byStatus.CANCELLED")).isEqualTo(4);
        assertThat(JsonPath.<Integer>read(phase3.body(), "$.branches[0].byStatus.FAILED")).isEqualTo(2);
        assertThat(JsonPath.<Integer>read(phase3.body(), "$.branches[0].cancelledByReason.CUSTOMER_CHANGED_MIND"))
                .isEqualTo(2);
        assertThat(JsonPath.<Integer>read(phase3.body(), "$.branches[0].cancelledByReason.CUSTOMER_WAITED_TOO_LONG"))
                .isEqualTo(1);
        assertThat(JsonPath.<Integer>read(phase3.body(), "$.branches[0].cancelledByReason.SPOILED_INGREDIENT"))
                .isEqualTo(1);
        assertThat(JsonPath.<Integer>read(phase3.body(), "$.branches[0].failedByReason.ORDER_EXPIRED")).isEqualTo(1);
        assertThat(JsonPath.<Integer>read(phase3.body(), "$.branches[0].failedByReason.INGREDIENT_AWAITING_STOCK_CHECK"))
                .isEqualTo(1);

        assertThat(JsonPath.<Integer>read(phase3.body(), "$.byCancelledReason.CUSTOMER_CHANGED_MIND")).isEqualTo(2);
        assertThat(JsonPath.<Integer>read(phase3.body(), "$.byCancelledReason.CUSTOMER_WAITED_TOO_LONG")).isEqualTo(1);
        assertThat(JsonPath.<Integer>read(phase3.body(), "$.byCancelledReason.EQUIPMENT_FAILURE")).isEqualTo(1);
        assertThat(JsonPath.<Integer>read(phase3.body(), "$.byCancelledReason.SPOILED_INGREDIENT")).isEqualTo(1);
        assertThat(JsonPath.<Integer>read(phase3.body(), "$.byFailedReason.INSUFFICIENT_INGREDIENTS")).isEqualTo(1);
        assertThat(JsonPath.<Integer>read(phase3.body(), "$.byFailedReason.ORDER_EXPIRED")).isEqualTo(1);
        assertThat(JsonPath.<Integer>read(phase3.body(), "$.byFailedReason.INGREDIENT_AWAITING_STOCK_CHECK"))
                .isEqualTo(1);
    }

    private static void insertOrder(
            java.sql.Connection conn,
            String schema,
            UUID orderId,
            String siteId,
            String sourceType,
            String status,
            int statusRank,
            String failureReason,
            String closeReason,
            String cancelFlag,
            OffsetDateTime cancelRequestedAt,
            String cancelMessage,
            long version,
            OffsetDateTime createdAt,
            OffsetDateTime updatedAt
    ) throws SQLException {
        try (var ps = conn.prepareStatement("""
                INSERT INTO %s.orders
                (order_id, client_request_id, site_id, source_type, status, status_rank, failure_reason,
                 close_reason, cancel_flag, cancel_requested_at, cancel_message, version, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """.formatted(schema))) {
            ps.setObject(1, orderId);
            ps.setObject(2, UUID.randomUUID());
            ps.setString(3, siteId);
            ps.setString(4, sourceType);
            ps.setString(5, status);
            ps.setInt(6, statusRank);
            ps.setString(7, failureReason);
            ps.setString(8, closeReason);
            ps.setString(9, cancelFlag);
            ps.setObject(10, cancelRequestedAt);
            ps.setString(11, cancelMessage);
            ps.setLong(12, version);
            ps.setObject(13, createdAt);
            ps.setObject(14, updatedAt);
            ps.executeUpdate();
        }
    }

    private static void insertOrderItem(
            java.sql.Connection conn,
            String schema,
            UUID orderId,
            int lineNo,
            String dishId,
            String dishName,
            int quantity
    ) throws SQLException {
        try (var ps = conn.prepareStatement("""
                INSERT INTO %s.order_items (order_id, line_no, dish_id, dish_name_snapshot, quantity)
                VALUES (?, ?, ?, ?, ?)
                """.formatted(schema))) {
            ps.setObject(1, orderId);
            ps.setInt(2, lineNo);
            ps.setString(3, dishId);
            ps.setString(4, dishName);
            ps.setInt(5, quantity);
            ps.executeUpdate();
        }
    }

    private static void upsertHeartbeat(
            java.sql.Connection conn,
            String schema,
            String branchId,
            OffsetDateTime beatAt
    ) throws SQLException {
        try (var ps = conn.prepareStatement("""
                INSERT INTO %s.replication_heartbeat (branch_id, beat_at)
                VALUES (?, ?)
                ON CONFLICT (branch_id) DO UPDATE SET beat_at = EXCLUDED.beat_at
                """.formatted(schema))) {
            ps.setString(1, branchId);
            ps.setObject(2, beatAt);
            ps.executeUpdate();
        }
    }

    private void assertOrderConstraintRejects(
            String schema,
            String siteId,
            String sourceType,
            String status,
            String cancelFlag,
            String constraintName
    ) throws Exception {
        try (var conn = DriverManager.getConnection(dbUrl, adminUser, adminPassword);
             var ps = conn.prepareStatement("""
                     INSERT INTO %s.orders
                     (order_id, client_request_id, site_id, source_type, status, cancel_flag, version, created_at, updated_at)
                     VALUES (?, ?, ?, ?, ?, ?, 0, now(), now())
                     """.formatted(schema))) {
            ps.setObject(1, UUID.randomUUID());
            ps.setObject(2, UUID.randomUUID());
            ps.setString(3, siteId);
            ps.setString(4, sourceType);
            ps.setString(5, status);
            ps.setString(6, cancelFlag);
            assertThatThrownBy(ps::executeUpdate)
                    .hasMessageContaining(constraintName);
        }
    }

    private void assertHeartbeatConstraintRejects(
            String schema,
            String branchId,
            String constraintName
    ) throws Exception {
        try (var conn = DriverManager.getConnection(dbUrl, adminUser, adminPassword);
             var ps = conn.prepareStatement("""
                     INSERT INTO %s.replication_heartbeat (branch_id, beat_at)
                     VALUES (?, now())
                     """.formatted(schema))) {
            ps.setString(1, branchId);
            assertThatThrownBy(ps::executeUpdate)
                    .hasMessageContaining(constraintName);
        }
    }

    private HttpResponse<String> get(String path) {
        try {
            var request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                    .timeout(Duration.ofSeconds(10))
                    .GET()
                    .build();
            return http.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
