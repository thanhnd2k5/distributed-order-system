package vn.edu.orders.query;

import com.jayway.jsonpath.JsonPath;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
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
                    DELETE FROM b5_query_test_pos02.order_items;
                    DELETE FROM b5_query_test_pos02.orders;
                    DELETE FROM b5_query_test_online.order_items;
                    DELETE FROM b5_query_test_online.orders;
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
    }

    @Test
    void fragmentConstraintsRejectCrossSiteWrites() throws Exception {
        assertConstraintRejects(
                "b5_query_test_pos01", "POS02", "POS", "chk_pos01_fragment_site");
        assertConstraintRejects(
                "b5_query_test_pos02", "POS01", "POS", "chk_pos02_fragment_site");
        assertConstraintRejects(
                "b5_query_test_online", "ONLINE", "POS", "chk_online_fragment_site");
    }

    @Test
    void reportingEndpointsReconstructAllFragmentsAndSupportFilters() throws Exception {
        UUID pos01OrderId = UUID.randomUUID();
        UUID pos02OrderId = UUID.randomUUID();
        UUID onlineOrderId = UUID.randomUUID();

        OffsetDateTime t1 = OffsetDateTime.parse("2026-11-12T10:00:00Z");
        OffsetDateTime t2 = OffsetDateTime.parse("2026-11-12T10:01:00Z");
        OffsetDateTime t3 = OffsetDateTime.parse("2026-11-12T10:02:00Z");

        try (var conn = DriverManager.getConnection(dbUrl, adminUser, adminPassword);
             var stmt = conn.createStatement()) {
            stmt.execute("""
                    INSERT INTO b5_query_test_pos01.orders
                    (order_id, client_request_id, site_id, source_type, status, failure_reason, version, created_at, updated_at)
                    VALUES ('%s', '%s', 'POS01', 'POS', 'DONE', NULL, 2, '%s', '%s');
                    INSERT INTO b5_query_test_pos01.order_items
                    (order_id, line_no, dish_id, dish_name_snapshot, quantity)
                    VALUES ('%s', 1, 'COM_GA', 'Cơm gà', 2);

                    INSERT INTO b5_query_test_pos02.orders
                    (order_id, client_request_id, site_id, source_type, status, failure_reason, version, created_at, updated_at)
                    VALUES ('%s', '%s', 'POS02', 'POS', 'FAILED', 'INSUFFICIENT_INGREDIENT: Thiếu gạo', 1, '%s', '%s');
                    INSERT INTO b5_query_test_pos02.order_items
                    (order_id, line_no, dish_id, dish_name_snapshot, quantity)
                    VALUES ('%s', 1, 'CHAO_GA', 'Cháo gà', 1);

                    INSERT INTO b5_query_test_online.orders
                    (order_id, client_request_id, site_id, source_type, status, failure_reason, version, created_at, updated_at)
                    VALUES ('%s', '%s', 'ONLINE', 'ONLINE', 'RESERVED', NULL, 1, '%s', '%s');
                    INSERT INTO b5_query_test_online.order_items
                    (order_id, line_no, dish_id, dish_name_snapshot, quantity)
                    VALUES ('%s', 1, 'CHAO_GA', 'Cháo gà', 1),
                           ('%s', 2, 'COM_GA', 'Cơm gà', 1);
                    """.formatted(
                    pos01OrderId, UUID.randomUUID(), t1, t1, pos01OrderId,
                    pos02OrderId, UUID.randomUUID(), t2, t2, pos02OrderId,
                    onlineOrderId, UUID.randomUUID(), t3, t3, onlineOrderId, onlineOrderId));
        }

        var allResp = get("/api/reporting/orders");
        assertThat(allResp.statusCode()).isEqualTo(200);
        assertThat(JsonPath.<Integer>read(allResp.body(), "$.count")).isEqualTo(3);
        assertThat(JsonPath.<String>read(allResp.body(), "$.replicaNote")).contains("asynchronous logical replication");
        assertThat(JsonPath.<List<String>>read(allResp.body(), "$.orders[*].orderId"))
                .containsExactly(onlineOrderId.toString(), pos02OrderId.toString(), pos01OrderId.toString());

        var bySite = get("/api/reporting/orders?siteId=pos02");
        assertThat(bySite.statusCode()).isEqualTo(200);
        assertThat(JsonPath.<List<String>>read(bySite.body(), "$.orders[*].orderId"))
                .containsExactly(pos02OrderId.toString());

        var byStatus = get("/api/reporting/orders?status=DONE");
        assertThat(byStatus.statusCode()).isEqualTo(200);
        assertThat(JsonPath.<List<String>>read(byStatus.body(), "$.orders[*].orderId"))
                .containsExactly(pos01OrderId.toString());

        var details = get("/api/reporting/orders/" + onlineOrderId);
        assertThat(details.statusCode()).isEqualTo(200);
        assertThat(JsonPath.<String>read(details.body(), "$.order.sourceSiteId")).isEqualTo("ONLINE");
        assertThat(JsonPath.<List<String>>read(details.body(), "$.items[*].dishId"))
                .containsExactly("CHAO_GA", "COM_GA");

        var summary = get("/api/reporting/summary");
        assertThat(summary.statusCode()).isEqualTo(200);
        assertThat(JsonPath.<Integer>read(summary.body(), "$.totalOrders")).isEqualTo(3);
        assertThat(JsonPath.<Integer>read(summary.body(), "$.bySite.POS01")).isEqualTo(1);
        assertThat(JsonPath.<Integer>read(summary.body(), "$.bySite.POS02")).isEqualTo(1);
        assertThat(JsonPath.<Integer>read(summary.body(), "$.bySite.ONLINE")).isEqualTo(1);
        assertThat(JsonPath.<Integer>read(summary.body(), "$.byStatus.DONE")).isEqualTo(1);
        assertThat(JsonPath.<Integer>read(summary.body(), "$.byStatus.FAILED")).isEqualTo(1);
        assertThat(JsonPath.<Integer>read(summary.body(), "$.byStatus.RESERVED")).isEqualTo(1);
        assertThat(JsonPath.<Integer>read(summary.body(), "$.byStatus.PENDING_INVENTORY")).isEqualTo(0);
        assertThat(JsonPath.<List<Object>>read(summary.body(), "$.subscriptions")).isNotNull();

        assertThat(get("/api/reporting/orders?siteId=UNKNOWN").statusCode()).isEqualTo(400);
        assertThat(get("/api/reporting/orders?status=INVALID").statusCode()).isEqualTo(400);
        assertThat(get("/api/reporting/orders?limit=0").statusCode()).isEqualTo(400);
        assertThat(get("/api/reporting/orders?limit=501").statusCode()).isEqualTo(400);
        assertThat(get("/api/reporting/orders/" + UUID.randomUUID()).statusCode()).isEqualTo(404);
    }

    private void assertConstraintRejects(
            String schema, String siteId, String sourceType, String constraintName
    ) throws Exception {
        try (var conn = DriverManager.getConnection(dbUrl, adminUser, adminPassword);
             var ps = conn.prepareStatement("""
                     INSERT INTO %s.orders
                     (order_id, client_request_id, site_id, source_type, status, version, created_at, updated_at)
                     VALUES (?, ?, ?, ?, 'PENDING_INVENTORY', 0, now(), now())
                     """.formatted(schema))) {
            ps.setObject(1, UUID.randomUUID());
            ps.setObject(2, UUID.randomUUID());
            ps.setString(3, siteId);
            ps.setString(4, sourceType);
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
