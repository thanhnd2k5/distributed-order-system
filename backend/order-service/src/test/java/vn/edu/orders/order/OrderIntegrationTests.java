package vn.edu.orders.order;

import com.jayway.jsonpath.JsonPath;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "outbox.enabled=false",
        "spring.jpa.properties.hibernate.default_schema=b1_test",
        "spring.flyway.default-schema=b1_test",
        "spring.flyway.schemas=b1_test",
        "site.id=POS_TEST", "site.source-type=POS"
})
class OrderIntegrationTests {
    @Value("${local.server.port}")
    private int port;

    @Autowired
    private JdbcTemplate jdbc;

    private final UUID requestId = UUID.randomUUID();
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    @AfterEach
    void deleteTestOrder() {
        jdbc.update("""
                DELETE FROM b1_test.outbox_events WHERE aggregate_id IN (
                    SELECT order_id FROM b1_test.orders WHERE client_request_id = ?
                )
                """, requestId);
        jdbc.update("""
                DELETE FROM b1_test.order_items WHERE order_id IN (
                    SELECT order_id FROM b1_test.orders WHERE client_request_id = ?
                )
                """, requestId);
        jdbc.update("DELETE FROM b1_test.orders WHERE client_request_id = ?", requestId);
    }

    @Test
    void concurrentRetriesCreateOneOrderAndOneVersionedEvent() throws Exception {
        var request = post("""
                {"clientRequestId":"%s","sourceSiteId":"UNTRUSTED","items":[
                  {"dishId":"COM_GA","quantity":2}, {"dishId":"CHAO_GA","quantity":1}
                ]}
                """.formatted(requestId));
        var first = http.sendAsync(request, HttpResponse.BodyHandlers.ofString());
        var second = http.sendAsync(post("""
                {"clientRequestId":"%s","items":[
                  {"dishId":"CHAO_GA","quantity":1}, {"dishId":"COM_GA","quantity":2}
                ]}
                """.formatted(requestId)), HttpResponse.BodyHandlers.ofString());
        var a = first.get(15, TimeUnit.SECONDS);
        var b = second.get(15, TimeUnit.SECONDS);

        assertThat(List.of(a.statusCode(), b.statusCode())).containsExactlyInAnyOrder(201, 200);
        String orderId = JsonPath.read(a.body(), "$.orderId");
        assertThat(JsonPath.<String>read(b.body(), "$.orderId")).isEqualTo(orderId);
        assertThat(JsonPath.<String>read(a.body(), "$.status")).isEqualTo("PENDING_INVENTORY");
        assertThat(JsonPath.<String>read(a.body(), "$.sourceSiteId")).isEqualTo("POS_TEST");
        String location = "/api/orders/" + orderId;
        assertThat(a.headers().firstValue("Location")).contains(location);
        assertThat(b.headers().firstValue("Location")).contains(location);

        var detail = send(request(location).GET().build());
        assertThat(detail.statusCode()).isEqualTo(200);
        assertThat(JsonPath.<String>read(detail.body(), "$.order.orderId")).isEqualTo(orderId);
        assertThat(JsonPath.<List<String>>read(detail.body(), "$.items[*].dishName"))
                .containsExactly("Cháo gà", "Cơm gà");
        var list = send(request("/api/orders").GET().build());
        assertThat(list.statusCode()).isEqualTo(200);
        assertThat(JsonPath.<List<String>>read(list.body(), "$[*].orderId")).contains(orderId);

        UUID id = UUID.fromString(orderId);
        String event = jdbc.queryForObject("""
                SELECT payload::text FROM b1_test.outbox_events
                WHERE aggregate_id = ? AND topic = 'order-events' AND message_key = ? AND sent_at IS NULL
                """, String.class, id, orderId);
        assertThat(JsonPath.<Integer>read(event, "$.schemaVersion")).isEqualTo(1);
        assertThat(JsonPath.<String>read(event, "$.eventType")).isEqualTo("OrderCreated");
        assertThat(JsonPath.<String>read(event, "$.orderId")).isEqualTo(orderId);
        assertThat(JsonPath.<String>read(event, "$.sourceType")).isEqualTo("POS");
        assertThat(JsonPath.<String>read(event, "$.sourceSiteId")).isEqualTo("POS_TEST");
        assertThat(Instant.parse(JsonPath.<String>read(event, "$.occurredAt"))).isNotNull();
        assertThat(JsonPath.<List<Integer>>read(event, "$.payload.items[*].quantity")).containsExactly(1, 2);
        assertThat(send(post(singleItem("COM_GA", "3"))).statusCode()).isEqualTo(409);
    }

    @Test
    void failedOutboxInsertRollsBackOrderAndItemsAndAllowsRetry() throws Exception {
        long itemsBefore = countItems();
        jdbc.execute("ALTER TABLE b1_test.outbox_events ADD CONSTRAINT reject_b1_outbox CHECK (false) NOT VALID");
        try {
            assertThat(send(post(singleItem("COM_GA", "2"))).statusCode()).isEqualTo(500);
            assertThat(orderCount()).isZero();
            assertThat(countItems()).isEqualTo(itemsBefore);
        } finally {
            jdbc.execute("ALTER TABLE b1_test.outbox_events DROP CONSTRAINT reject_b1_outbox");
        }
        assertThat(send(post(singleItem("COM_GA", "2"))).statusCode()).isEqualTo(201);
    }

    @Test
    void rejectsInvalidItemsAndReportsMissingOrder() throws Exception {
        assertThat(send(post(singleItem("NO_SUCH_DISH", "1"))).statusCode()).isEqualTo(400);
        assertThat(send(post(singleItem("COM_GA", "0"))).statusCode()).isEqualTo(400);
        assertThat(send(post(singleItem("COM_GA", "1.5"))).statusCode()).isEqualTo(400);
        assertThat(send(post("""
                {"clientRequestId":"%s","items":[{"dishId":"COM_GA","quantity":1},{"dishId":"COM_GA","quantity":2}]}
                """.formatted(requestId))).statusCode()).isEqualTo(400);
        assertThat(orderCount()).isZero();
        assertThat(send(request("/api/orders/" + UUID.randomUUID()).GET().build()).statusCode()).isEqualTo(404);
    }

    @Test
    void allowsJsonPostFromViteOrigin() throws Exception {
        var preflight = send(request("/api/orders")
                .header("Origin", "http://localhost:5173")
                .header("Access-Control-Request-Method", "POST")
                .header("Access-Control-Request-Headers", "Content-Type")
                .method("OPTIONS", HttpRequest.BodyPublishers.noBody()).build());
        assertThat(preflight.statusCode()).isEqualTo(200);
        assertThat(preflight.headers().firstValue("Access-Control-Allow-Origin")).contains("http://localhost:5173");
    }

    private long countItems() {
        return jdbc.queryForObject("SELECT count(*) FROM b1_test.order_items", Long.class);
    }

    private long orderCount() {
        return jdbc.queryForObject("SELECT count(*) FROM b1_test.orders WHERE client_request_id = ?",
                Long.class, requestId);
    }

    private String singleItem(String dishId, String quantity) {
        return """
                {"clientRequestId":"%s","items":[{"dishId":"%s","quantity":%s}]}
                """.formatted(requestId, dishId, quantity);
    }

    private HttpRequest post(String json) {
        return request("/api/orders").header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json)).build();
    }

    private HttpRequest.Builder request(String path) {
        return HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                .timeout(Duration.ofSeconds(10));
    }

    private HttpResponse<String> send(HttpRequest request) throws Exception {
        return http.send(request, HttpResponse.BodyHandlers.ofString());
    }
}
