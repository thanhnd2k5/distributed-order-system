package vn.edu.orders.contracts.events;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;

class EventContractFixtureTests {
    private final ObjectMapper mapper = JsonMapper.builder().build();

    @Test
    void orderCreatedMatchesFixture() throws Exception {
        assertRoundTrip("order-created.v1.json", OrderCreatedEvent.class, event -> {
            assertThat(event.schemaVersion()).isEqualTo(EventTopics.SCHEMA_VERSION_1);
            assertThat(event.eventType()).isEqualTo(EventType.OrderCreated);
            assertThat(event.sourceType()).isEqualTo(SourceType.POS);
            assertThat(event.sourceSiteId()).isEqualTo("POS01");
            assertThat(event.payload().items()).hasSize(1);
            assertThat(event.payload().items().getFirst().dishId()).isEqualTo("COM_GA");
        });
    }

    @Test
    void reservedMatchesFixture() throws Exception {
        assertRoundTrip("reserved.v1.json", ReservedEvent.class, event -> {
            assertThat(event.schemaVersion()).isEqualTo(EventTopics.SCHEMA_VERSION_1);
            assertThat(event.eventType()).isEqualTo(EventType.Reserved);
            assertThat(event.payload().reservationId()).isEqualTo(event.orderId());
            assertThat(event.payload().items()).hasSize(1);
        });
    }

    @Test
    void failedMatchesFixture() throws Exception {
        assertRoundTrip("failed.v1.json", FailedEvent.class, event -> {
            assertThat(event.schemaVersion()).isEqualTo(EventTopics.SCHEMA_VERSION_1);
            assertThat(event.eventType()).isEqualTo(EventType.Failed);
            assertThat(event.payload().reasonCode()).isEqualTo(FailureReasonCode.INSUFFICIENT_INGREDIENT);
            assertThat(event.payload().reasonMessage()).isNotBlank();
        });
    }

    @Test
    void doneMatchesFixture() throws Exception {
        assertRoundTrip("done.v1.json", DoneEvent.class, event -> {
            assertThat(event.schemaVersion()).isEqualTo(EventTopics.SCHEMA_VERSION_1);
            assertThat(event.eventType()).isEqualTo(EventType.Done);
            assertThat(event.payload().ticketId()).isNotNull();
            assertThat(event.payload().completedAt()).isNotNull();
        });
    }

    private <T> void assertRoundTrip(String fileName, Class<T> type, java.util.function.Consumer<T> assertions)
            throws Exception {
        String raw = Files.readString(resolveFixture(fileName));
        JsonNode expectedTree = mapper.readTree(raw);
        T parsed = mapper.readValue(raw, type);
        assertions.accept(parsed);
        JsonNode actualTree = mapper.readTree(mapper.writeValueAsString(parsed));
        assertThat(actualTree).isEqualTo(expectedTree);
    }

    private Path resolveFixture(String fileName) {
        Path direct = Path.of("docs", "contracts", "events", fileName);
        if (Files.exists(direct)) {
            return direct;
        }
        Path fromModule = Path.of("..", "docs", "contracts", "events", fileName);
        if (Files.exists(fromModule)) {
            return fromModule;
        }
        Path fromBackend = Path.of("..", "..", "docs", "contracts", "events", fileName);
        if (Files.exists(fromBackend)) {
            return fromBackend;
        }
        throw new IllegalStateException("Fixture not found: " + fileName + " (cwd=" + Path.of("").toAbsolutePath() + ")");
    }
}
