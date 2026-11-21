package vn.edu.orders.kitchen;

import com.jayway.jsonpath.JsonPath;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "outbox.enabled=false",
        "spring.kafka.listener.auto-startup=false",
        "spring.kafka.bootstrap-servers=localhost:19999",
        "spring.jpa.properties.hibernate.default_schema=v15_kitchen_skeleton_test",
        "spring.flyway.default-schema=v15_kitchen_skeleton_test",
        "spring.flyway.schemas=v15_kitchen_skeleton_test",
        "kitchen.instance-id=kitchen-skeleton-test",
        "kitchen.monitor-admin-timeout-ms=300"
})
class KitchenSkeletonTests {
    @Value("${local.server.port}")
    private int port;

    @Autowired
    private JdbcTemplate jdbc;

    private final HttpClient http = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    @Test
    void migrationsAndHealthWorkWithoutKafka() throws Exception {
        assertThat(jdbc.queryForList("""
                SELECT version FROM v15_kitchen_skeleton_test.flyway_schema_history
                WHERE success AND version IS NOT NULL ORDER BY installed_rank
                """, String.class)).containsExactly("1", "3");

        var health = get("/actuator/health");
        assertThat(health.statusCode()).isEqualTo(200);
        assertThat(JsonPath.<String>read(health.body(), "$.status")).isEqualTo("UP");
    }

    @Test
    void emptyTicketsAlertsAndMonitorWorkGracefully() throws Exception {
        var all = get("/api/kitchen/tickets");
        assertThat(all.statusCode()).isEqualTo(200);
        assertThat(JsonPath.<List<?>>read(all.body(), "$")).isEmpty();

        var queued = get("/api/kitchen/tickets?status=QUEUED");
        assertThat(queued.statusCode()).isEqualTo(200);
        assertThat(JsonPath.<List<?>>read(queued.body(), "$")).isEmpty();

        var cooking = get("/api/kitchen/tickets?status=cooking");
        assertThat(cooking.statusCode()).isEqualTo(200);
        assertThat(JsonPath.<List<?>>read(cooking.body(), "$")).isEmpty();

        var invalid = get("/api/kitchen/tickets?status=UNKNOWN");
        assertThat(invalid.statusCode()).isEqualTo(400);

        var notExistsFilter = get("/api/kitchen/tickets?status=NOT_EXISTS");
        assertThat(notExistsFilter.statusCode()).isEqualTo(400);

        var alerts = get("/api/kitchen/alerts");
        assertThat(alerts.statusCode()).isEqualTo(200);
        assertThat(JsonPath.<List<?>>read(alerts.body(), "$")).isEmpty();

        var badAlertKind = get("/api/kitchen/alerts?kind=BOGUS");
        assertThat(badAlertKind.statusCode()).isEqualTo(400);

        var monitor = get("/api/kitchen/monitor");
        assertThat(monitor.statusCode()).isEqualTo(200);
        assertThat(JsonPath.<String>read(monitor.body(), "$.instanceId")).isEqualTo("kitchen-skeleton-test");
        assertThat(JsonPath.<Boolean>read(monitor.body(), "$.kafkaConnected")).isFalse();
        assertThat(JsonPath.<Boolean>read(monitor.body(), "$.anySlowConsumerWarning")).isFalse();
    }

    @Test
    void missingTicketReturns404InvalidIdReturns400AndCorsAllowsGetAndPost() throws Exception {
        UUID missingId = UUID.randomUUID();
        assertThat(get("/api/kitchen/tickets/" + missingId).statusCode()).isEqualTo(404);
        assertThat(get("/api/kitchen/tickets/not-a-uuid").statusCode()).isEqualTo(400);
        assertThat(postJson("/api/kitchen/tickets/" + missingId + "/start", null).statusCode()).isEqualTo(404);
        assertThat(postJson("/api/kitchen/tickets/" + missingId + "/complete", null).statusCode()).isEqualTo(404);
        assertThat(postJson("/api/kitchen/tickets/" + missingId + "/reject",
                "{\"reasonCode\":\"EQUIPMENT_FAILURE\",\"spoiledIngredients\":[]}").statusCode()).isEqualTo(404);

        var getPreflight = http.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/api/kitchen/tickets"))
                .header("Origin", "http://localhost:5173")
                .header("Access-Control-Request-Method", "GET")
                .method("OPTIONS", HttpRequest.BodyPublishers.noBody())
                .timeout(Duration.ofSeconds(10))
                .build(), HttpResponse.BodyHandlers.ofString());
        assertThat(getPreflight.statusCode()).isEqualTo(200);
        assertThat(getPreflight.headers().firstValue("Access-Control-Allow-Origin"))
                .contains("http://localhost:5173");

        var postPreflight = http.send(HttpRequest.newBuilder(
                        URI.create("http://localhost:" + port + "/api/kitchen/tickets/" + UUID.randomUUID() + "/start"))
                .header("Origin", "http://localhost:5173")
                .header("Access-Control-Request-Method", "POST")
                .method("OPTIONS", HttpRequest.BodyPublishers.noBody())
                .timeout(Duration.ofSeconds(10))
                .build(), HttpResponse.BodyHandlers.ofString());
        assertThat(postPreflight.statusCode()).isEqualTo(200);
        assertThat(postPreflight.headers().firstValue("Access-Control-Allow-Origin"))
                .contains("http://localhost:5173");

        var deletePreflight = http.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/api/kitchen/tickets"))
                .header("Origin", "http://localhost:5173")
                .header("Access-Control-Request-Method", "DELETE")
                .method("OPTIONS", HttpRequest.BodyPublishers.noBody())
                .timeout(Duration.ofSeconds(10))
                .build(), HttpResponse.BodyHandlers.ofString());
        assertThat(deletePreflight.statusCode()).isEqualTo(403);
    }

    private HttpResponse<String> get(String path) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                .timeout(Duration.ofSeconds(10))
                .GET()
                .build(), HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> postJson(String path, String jsonBody) throws Exception {
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                .timeout(Duration.ofSeconds(10));
        if (jsonBody != null) {
            builder.header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(jsonBody));
        } else {
            builder.POST(HttpRequest.BodyPublishers.noBody());
        }
        return http.send(builder.build(), HttpResponse.BodyHandlers.ofString());
    }
}
