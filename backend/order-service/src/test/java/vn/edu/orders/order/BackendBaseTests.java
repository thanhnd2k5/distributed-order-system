package vn.edu.orders.order;

import com.jayway.jsonpath.JsonPath;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.assertj.core.api.Assertions.assertThat;

// Uses real PostgreSQL in a dedicated test schema.
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "outbox.enabled=false",
        "order.heartbeat-enabled=false",
        "spring.kafka.listener.auto-startup=false",
        "spring.jpa.properties.hibernate.default_schema=b0_test",
        "spring.flyway.default-schema=b0_test",
        "spring.flyway.schemas=b0_test"
})
class BackendBaseTests {
    @Value("${local.server.port}")
    private int port;

    @Autowired
    private JdbcTemplate jdbc;

    private final HttpClient client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5)).build();

    @Test
    void migrationsAndHealthUseRealPostgres() throws Exception {
        assertThat(jdbc.queryForList("""
                SELECT version FROM b0_test.flyway_schema_history
                WHERE success AND version IS NOT NULL ORDER BY installed_rank
                """, String.class)).containsSubsequence("1", "2", "3", "4", "5", "6", "7");
        var response = get("/actuator/health");
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(JsonPath.<String>read(response.body(), "$.status")).isEqualTo("UP");
    }

    @Test
    void menuReturnsSeededDishesInOrderAndExcludesInactiveOrHiddenDishes() throws Exception {
        jdbc.update("INSERT INTO b0_test.menu_items VALUES (?, ?, ?)",
                "INACTIVE_TEST", "Món tạm ngừng", false);
        try {
            var response = get("/api/menu");
            assertThat(response.statusCode()).isEqualTo(200);
            assertThat(JsonPath.<Boolean>read(response.body(), "$.stale")).isTrue();
            assertThat(JsonPath.<List<String>>read(response.body(), "$.dishes[*].dishId"))
                    .containsExactly("CHAO_GA", "COM_GA", "PHO_BO");
            assertThat(JsonPath.<List<String>>read(response.body(), "$.dishes[*].name"))
                    .containsExactly("Cháo gà", "Cơm gà", "Phở bò");
        } finally {
            jdbc.update("DELETE FROM b0_test.menu_items WHERE dish_id = ?", "INACTIVE_TEST");
        }
    }

    private HttpResponse<String> get(String path) throws Exception {
        var request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                .timeout(Duration.ofSeconds(10)).GET().build();
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }
}
