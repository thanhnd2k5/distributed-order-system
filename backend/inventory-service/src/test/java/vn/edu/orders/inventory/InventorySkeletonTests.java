package vn.edu.orders.inventory;

import com.jayway.jsonpath.JsonPath;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "outbox.enabled=false",
        "inventory.heartbeat-enabled=false",
        "spring.kafka.listener.auto-startup=false",
        "spring.kafka.bootstrap-servers=localhost:19999",
        "spring.flyway.default-schema=inventory_test",
        "spring.flyway.schemas=inventory_test",
        "inventory.instance-id=inventory-test"
})
@Import(InventoryTestSupport.MutableClockConfig.class)
class InventorySkeletonTests {
    private static final String SCHEMA = "inventory_test";

    @Value("${local.server.port}")
    private int port;

    @Autowired
    private JdbcTemplate jdbc;

    private final HttpClient http = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    @BeforeEach
    void setup() {
        InventoryTestSupport.resetSchema(jdbc, SCHEMA);
    }

    @AfterEach
    void verifyInvariants() {
        InventoryTestSupport.assertInventoryInvariants(jdbc, SCHEMA);
    }

    @Test
    void migrationsAndHealthWorkWithoutKafka() throws Exception {
        assertThat(jdbc.queryForList("""
                SELECT version FROM inventory_test.flyway_schema_history
                WHERE success AND version IS NOT NULL ORDER BY installed_rank
                """, String.class)).containsSubsequence("1", "2", "3");

        var health = get("/actuator/health");
        assertThat(health.statusCode()).isEqualTo(200);
        assertThat(JsonPath.<String>read(health.body(), "$.status")).isEqualTo("UP");
    }

    @Test
    void seededStockRecipesAndMenuSnapshotMatchV15() throws Exception {
        var stock = get("/api/stock");
        assertThat(stock.statusCode()).isEqualTo(200);
        assertThat(JsonPath.<List<String>>read(stock.body(), "$[*].ingredientId"))
                .containsExactly("BANH_PHO", "GAO", "HANH_LA", "THIT_BO", "THIT_GA");
        assertThat(JsonPath.<List<Integer>>read(stock.body(), "$[*].available"))
                .containsExactly(1000, 1000, 500, 1000, 1500);

        var recipes = get("/api/recipes");
        assertThat(recipes.statusCode()).isEqualTo(200);
        assertThat(JsonPath.<List<String>>read(recipes.body(), "$[*].dishId"))
                .containsExactly("CHAO_GA", "COM_GA", "PHO_BO");
        assertThat(JsonPath.<List<Integer>>read(recipes.body(), "$[0].ingredients[*].quantityPerDish"))
                .containsExactly(50, 100);
        assertThat(JsonPath.<List<Integer>>read(recipes.body(), "$[1].ingredients[*].quantityPerDish"))
                .containsExactly(100, 150);

        var snapshot = get("/api/menu/snapshot");
        assertThat(snapshot.statusCode()).isEqualTo(200);
        assertThat(JsonPath.<Integer>read(snapshot.body(), "$.epoch")).isEqualTo(1);
        assertThat(JsonPath.<Integer>read(snapshot.body(), "$.seq")).isEqualTo(1);
        assertThat(JsonPath.<List<String>>read(snapshot.body(), "$.dishes[*].dishId"))
                .containsExactly("CHAO_GA", "COM_GA", "PHO_BO");
        assertThat(JsonPath.<List<String>>read(snapshot.body(), "$.dishes[*].status"))
                .containsExactly("AVAILABLE", "AVAILABLE", "AVAILABLE");
    }

    @Test
    void missingReservationReturns404InvalidIdReturns400AndCorsAllowsGetAndPost() throws Exception {
        assertThat(get("/api/reservations/" + UUID.randomUUID()).statusCode()).isEqualTo(404);
        assertThat(get("/api/reservations/not-a-uuid").statusCode()).isEqualTo(400);

        var monitor = get("/api/monitor");
        assertThat(monitor.statusCode()).isEqualTo(200);
        assertThat(JsonPath.<Boolean>read(monitor.body(), "$.kafkaReachable")).isFalse();

        var getPreflight = http.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/api/stock"))
                .header("Origin", "http://localhost:5173")
                .header("Access-Control-Request-Method", "GET")
                .method("OPTIONS", HttpRequest.BodyPublishers.noBody())
                .timeout(Duration.ofSeconds(10))
                .build(), HttpResponse.BodyHandlers.ofString());
        assertThat(getPreflight.statusCode()).isEqualTo(200);
        assertThat(getPreflight.headers().firstValue("Access-Control-Allow-Origin"))
                .contains("http://localhost:5173");

        var postPreflight = http.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/api/stock/receipts"))
                .header("Origin", "http://localhost:5173")
                .header("Access-Control-Request-Method", "POST")
                .method("OPTIONS", HttpRequest.BodyPublishers.noBody())
                .timeout(Duration.ofSeconds(10))
                .build(), HttpResponse.BodyHandlers.ofString());
        assertThat(postPreflight.statusCode()).isEqualTo(200);
    }

    private HttpResponse<String> get(String path) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                .timeout(Duration.ofSeconds(10))
                .GET()
                .build(), HttpResponse.BodyHandlers.ofString());
    }
}
