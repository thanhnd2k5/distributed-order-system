package vn.edu.orders.inventory;

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
        "spring.jpa.properties.hibernate.default_schema=b3_test",
        "spring.flyway.default-schema=b3_test",
        "spring.flyway.schemas=b3_test",
        "inventory.instance-id=inventory-test"
})
class InventorySkeletonTests {
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
                SELECT version FROM b3_test.flyway_schema_history
                WHERE success AND version IS NOT NULL ORDER BY installed_rank
                """, String.class)).containsSubsequence("1", "2");

        var health = get("/actuator/health");
        assertThat(health.statusCode()).isEqualTo(200);
        assertThat(JsonPath.<String>read(health.body(), "$.status")).isEqualTo("UP");
    }

    @Test
    void seededIngredientsAndRecipesMatchDemoPlan() throws Exception {
        var ingredients = get("/api/ingredients");
        assertThat(ingredients.statusCode()).isEqualTo(200);
        assertThat(JsonPath.<List<String>>read(ingredients.body(), "$[*].ingredientId"))
                .containsExactly("GAO", "THIT_GA");
        assertThat(JsonPath.<List<Integer>>read(ingredients.body(), "$[*].availableQuantity"))
                .containsExactly(1000, 1500);

        var recipes = get("/api/recipes");
        assertThat(recipes.statusCode()).isEqualTo(200);
        assertThat(JsonPath.<List<String>>read(recipes.body(), "$[*].dishId"))
                .containsExactly("CHAO_GA", "COM_GA");
        assertThat(JsonPath.<List<Integer>>read(recipes.body(), "$[0].ingredients[*].quantityPerDish"))
                .containsExactly(50, 100);
        assertThat(JsonPath.<List<Integer>>read(recipes.body(), "$[1].ingredients[*].quantityPerDish"))
                .containsExactly(100, 150);
    }

    @Test
    void missingReservationReturns404AndInvalidIdReturns400AndCorsAllowsGetOnly() throws Exception {
        assertThat(get("/api/reservations/" + UUID.randomUUID()).statusCode()).isEqualTo(404);
        assertThat(get("/api/reservations/not-a-uuid").statusCode()).isEqualTo(400);

        var getPreflight = http.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/api/ingredients"))
                .header("Origin", "http://localhost:5173")
                .header("Access-Control-Request-Method", "GET")
                .method("OPTIONS", HttpRequest.BodyPublishers.noBody())
                .timeout(Duration.ofSeconds(10))
                .build(), HttpResponse.BodyHandlers.ofString());
        assertThat(getPreflight.statusCode()).isEqualTo(200);
        assertThat(getPreflight.headers().firstValue("Access-Control-Allow-Origin"))
                .contains("http://localhost:5173");

        var postPreflight = http.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/api/ingredients"))
                .header("Origin", "http://localhost:5173")
                .header("Access-Control-Request-Method", "POST")
                .method("OPTIONS", HttpRequest.BodyPublishers.noBody())
                .timeout(Duration.ofSeconds(10))
                .build(), HttpResponse.BodyHandlers.ofString());
        assertThat(postPreflight.statusCode()).isEqualTo(403);
    }

    private HttpResponse<String> get(String path) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                .timeout(Duration.ofSeconds(10))
                .GET()
                .build(), HttpResponse.BodyHandlers.ofString());
    }
}
