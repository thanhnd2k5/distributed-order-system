package vn.edu.orders.contracts.events;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JavaType;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;

class EventContractFixtureTests {
    private final ObjectMapper mapper = JsonMapper.builder().build();

    @Test
    void v15EventFixturesRoundTripCleanly() throws Exception {
        assertV15RoundTrip("order-created.json", OrderCreatedPayload.class, EventType.OrderCreated);
        assertV15RoundTrip("cancel-requested.json", CancelRequestedPayload.class, EventType.CancelRequested);
        assertV15RoundTrip("order-reserved.kitchen.json", OrderReservedPayload.class, EventType.OrderReserved);
        assertV15RoundTrip("order-reserved.branch.json", OrderReservedPayload.class, EventType.OrderReserved);
        assertV15RoundTrip("order-failed.json", OrderFailedPayload.class, EventType.OrderFailed);
        assertV15RoundTrip("stock-consumed.json", StockConsumedPayload.class, EventType.StockConsumed);
        assertV15RoundTrip("release-requested.json", ReleaseRequestedPayload.class, EventType.ReleaseRequested);
        assertV15RoundTrip("cancel-pending.json", CancelPendingPayload.class, EventType.CancelPending);
        assertV15RoundTrip("cancel-rejected.json", CancelRejectedPayload.class, EventType.CancelRejected);
        assertV15RoundTrip("order-cancelled.json", OrderCancelledPayload.class, EventType.OrderCancelled);
        assertV15RoundTrip("order-completed.json", OrderCompletedPayload.class, EventType.OrderCompleted);
        assertV15RoundTrip("dish-status-changed.json", DishStatusChangedPayload.class, EventType.DishStatusChanged);
        assertV15RoundTrip("ingredient-check-changed.json", IngredientCheckChangedPayload.class, EventType.IngredientCheckChanged);
        assertV15RoundTrip("heartbeat.json", HeartbeatPayload.class, EventType.Heartbeat);
    }

    @Test
    @SuppressWarnings("deprecation")
    void legacyDoneEventMapsCleanlyToOrderCompletedEnvelope() throws Exception {
        String raw = Files.readString(resolveFixture("done.v1.json"));
        JsonNode expectedTree = mapper.readTree(raw);
        DoneEvent parsed = mapper.readValue(raw, DoneEvent.class);

        assertThat(parsed.schemaVersion()).isEqualTo(EventTopics.SCHEMA_VERSION_1);
        assertThat(parsed.eventType()).isEqualTo("Done");
        assertThat(parsed.payload().ticketId()).isNotNull();
        assertThat(parsed.payload().completedAt()).isNotNull();
        assertThat(mapper.readTree(mapper.writeValueAsString(parsed))).isEqualTo(expectedTree);

        UUID causationId = UUID.randomUUID();
        EventEnvelope<OrderCompletedPayload> mapped = parsed.toOrderCompletedEnvelope(causationId);
        mapped.validateEnvelope();
        assertThat(mapped.eventType()).isEqualTo(EventType.OrderCompleted);
        assertThat(mapped.branchId()).isEqualTo("POS01");
        assertThat(mapped.causationId()).isEqualTo(causationId);
    }

    private <T> void assertV15RoundTrip(String fileName, Class<T> payloadClass, EventType expectedType)
            throws Exception {
        String raw = Files.readString(resolveFixture("v1.5/" + fileName));
        JsonNode expectedTree = mapper.readTree(raw);
        JavaType type = mapper.getTypeFactory().constructParametricType(EventEnvelope.class, payloadClass);
        EventEnvelope<T> parsed = mapper.readValue(raw, type);
        parsed.validateEnvelope();
        assertThat(parsed.eventType()).isEqualTo(expectedType);
        JsonNode actualTree = mapper.readTree(mapper.writeValueAsString(parsed));
        assertThat(actualTree).isEqualTo(expectedTree);
    }

    private Path resolveFixture(String relativePath) {
        Path direct = Path.of("docs", "contracts", "events", relativePath);
        if (Files.exists(direct)) {
            return direct;
        }
        Path fromModule = Path.of("..", "docs", "contracts", "events", relativePath);
        if (Files.exists(fromModule)) {
            return fromModule;
        }
        Path fromBackend = Path.of("..", "..", "docs", "contracts", "events", relativePath);
        if (Files.exists(fromBackend)) {
            return fromBackend;
        }
        throw new IllegalStateException("Fixture not found: " + relativePath + " (cwd=" + Path.of("").toAbsolutePath() + ")");
    }
}
