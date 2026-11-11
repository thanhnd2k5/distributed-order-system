package vn.edu.orders.inventory;

import com.jayway.jsonpath.JsonPath;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.ObjectMapper;
import vn.edu.orders.contracts.events.EventTopics;
import vn.edu.orders.contracts.events.EventType;
import vn.edu.orders.contracts.events.FailedEvent;
import vn.edu.orders.contracts.events.FailureReasonCode;
import vn.edu.orders.contracts.events.OrderCreatedEvent;
import vn.edu.orders.contracts.events.OrderItemLine;
import vn.edu.orders.contracts.events.ReservedEvent;
import vn.edu.orders.contracts.events.SourceType;
import vn.edu.orders.inventory.application.EventProcessingContext;
import vn.edu.orders.inventory.application.InventoryReservationService;
import vn.edu.orders.inventory.application.InvalidEventException;
import vn.edu.orders.inventory.application.ReservationResult;
import vn.edu.orders.inventory.application.ReservationResult.Outcome;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "outbox.enabled=false",
        "spring.kafka.listener.auto-startup=false",
        "spring.kafka.bootstrap-servers=localhost:19999",
        "spring.jpa.properties.hibernate.default_schema=b3_test",
        "spring.flyway.default-schema=b3_test",
        "spring.flyway.schemas=b3_test",
        "inventory.instance-id=inventory-test"
})
class InventoryIntegrationTests {
    @Value("${local.server.port}")
    private int port;

    @Autowired
    private InventoryReservationService reservationService;

    @Autowired
    private JdbcTemplate jdbc;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Autowired
    private ObjectMapper mapper;

    private final HttpClient http = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    @BeforeEach
    @AfterEach
    void resetTestSchemaToNormal() {
        jdbc.update("DELETE FROM b3_test.outbox_events");
        jdbc.update("DELETE FROM b3_test.reservation_items");
        jdbc.update("DELETE FROM b3_test.inventory_order_decisions");
        jdbc.update("DELETE FROM b3_test.processed_events");
        jdbc.update("UPDATE b3_test.ingredients SET available_quantity = 1000, updated_at = now() WHERE ingredient_id = 'GAO'");
        jdbc.update("UPDATE b3_test.ingredients SET available_quantity = 1500, updated_at = now() WHERE ingredient_id = 'THIT_GA'");
    }

    @Test
    void aggregatesSharedIngredientsAcrossLinesAndWritesReservedOutbox() throws Exception {
        UUID eventId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();
        OrderCreatedEvent event = OrderCreatedEvent.v1(
                eventId,
                orderId,
                SourceType.POS,
                "POS01",
                Instant.now(),
                List.of(
                        new OrderItemLine(1, "CHAO_GA", "Cháo gà", 3),
                        new OrderItemLine(2, "COM_GA", "Cơm gà", 2)));

        ReservationResult result = reservationService.processOrderCreated(event, EventProcessingContext.DIRECT);
        assertThat(result).isEqualTo(new ReservationResult(eventId, orderId, Outcome.APPLIED, "RESERVED", null));

        // 3 * (50 GAO, 100 THIT_GA) + 2 * (100 GAO, 150 THIT_GA) = 350 GAO, 600 THIT_GA
        assertThat(stock("GAO")).isEqualTo(650L);
        assertThat(stock("THIT_GA")).isEqualTo(900L);

        var reservation = get("/api/reservations/" + orderId);
        assertThat(reservation.statusCode()).isEqualTo(200);
        assertThat(JsonPath.<String>read(reservation.body(), "$.decision")).isEqualTo("RESERVED");
        assertThat(JsonPath.<String>read(reservation.body(), "$.reasonCode")).isNull();
        assertThat(JsonPath.<List<String>>read(reservation.body(), "$.items[*].ingredientId"))
                .containsExactly("GAO", "THIT_GA");
        assertThat(JsonPath.<List<Integer>>read(reservation.body(), "$.items[*].reservedQuantity"))
                .containsExactly(350, 600);

        String outboxJson = jdbc.queryForObject("""
                SELECT payload::text FROM b3_test.outbox_events
                WHERE aggregate_id = ? AND topic = ? AND event_type = ? AND message_key = ? AND sent_at IS NULL
                """, String.class, orderId, EventTopics.STATUS_EVENTS, EventType.Reserved.name(), orderId.toString());
        ReservedEvent parsed = mapper.readValue(outboxJson, ReservedEvent.class);
        assertThat(parsed.orderId()).isEqualTo(orderId);
        assertThat(parsed.payload().reservationId()).isEqualTo(orderId);
        assertThat(parsed.payload().items()).hasSize(2);
    }

    @Test
    void missingOneIngredientIsAtomicAndCommitsFailedOutbox() throws Exception {
        jdbc.update("UPDATE b3_test.ingredients SET available_quantity = 50 WHERE ingredient_id = 'GAO'");
        UUID eventId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();
        OrderCreatedEvent event = singleDishEvent(eventId, orderId, "COM_GA", "Cơm gà", 1);

        ReservationResult result = reservationService.processOrderCreated(event, EventProcessingContext.DIRECT);
        assertThat(result.outcome()).isEqualTo(Outcome.APPLIED);
        assertThat(result.decision()).isEqualTo("FAILED");
        assertThat(result.reasonCode()).isEqualTo(FailureReasonCode.INSUFFICIENT_INGREDIENT);

        assertThat(stock("GAO")).isEqualTo(50L);
        assertThat(stock("THIT_GA")).isEqualTo(1500L);
        assertThat(count("SELECT count(*) FROM b3_test.reservation_items WHERE order_id = ?", orderId)).isZero();

        var reservation = get("/api/reservations/" + orderId);
        assertThat(reservation.statusCode()).isEqualTo(200);
        assertThat(JsonPath.<String>read(reservation.body(), "$.decision")).isEqualTo("FAILED");
        assertThat(JsonPath.<String>read(reservation.body(), "$.reasonCode")).isEqualTo("INSUFFICIENT_INGREDIENT");
        assertThat(JsonPath.<List<?>>read(reservation.body(), "$.items")).isEmpty();

        String outboxJson = jdbc.queryForObject("""
                SELECT payload::text FROM b3_test.outbox_events
                WHERE aggregate_id = ? AND event_type = 'Failed' AND sent_at IS NULL
                """, String.class, orderId);
        FailedEvent failed = mapper.readValue(outboxJson, FailedEvent.class);
        assertThat(failed.payload().reasonCode()).isEqualTo(FailureReasonCode.INSUFFICIENT_INGREDIENT);
        assertThat(failed.payload().reasonMessage()).contains("GAO");
    }

    @Test
    void missingRecipeCommitsFailedWithoutDeductingStock() throws Exception {
        UUID eventId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();
        OrderCreatedEvent event = singleDishEvent(eventId, orderId, "PHO_BO", "Phở bò", 1);

        ReservationResult result = reservationService.processOrderCreated(event, EventProcessingContext.DIRECT);
        assertThat(result.outcome()).isEqualTo(Outcome.APPLIED);
        assertThat(result.decision()).isEqualTo("FAILED");
        assertThat(result.reasonCode()).isEqualTo(FailureReasonCode.RECIPE_NOT_FOUND);

        assertThat(stock("GAO")).isEqualTo(1000L);
        assertThat(stock("THIT_GA")).isEqualTo(1500L);
        String outboxJson = jdbc.queryForObject(
                "SELECT payload::text FROM b3_test.outbox_events WHERE aggregate_id = ?", String.class, orderId);
        FailedEvent failed = mapper.readValue(outboxJson, FailedEvent.class);
        assertThat(failed.payload().reasonCode()).isEqualTo(FailureReasonCode.RECIPE_NOT_FOUND);
    }

    @Test
    void concurrentLastPortionOverlappingTransactionsPermitExactlyOneWinner() throws Exception {
        jdbc.update("UPDATE b3_test.ingredients SET available_quantity = 150 WHERE ingredient_id = 'THIT_GA'");

        OrderCreatedEvent first = singleDishEvent(UUID.randomUUID(), UUID.randomUUID(), "COM_GA", "Cơm gà", 1);
        OrderCreatedEvent second = singleDishEvent(UUID.randomUUID(), UUID.randomUUID(), "COM_GA", "Cơm gà", 1);

        CountDownLatch holderLocked = new CountDownLatch(1);
        CountDownLatch releaseHolder = new CountDownLatch(1);
        TransactionTemplate tx = new TransactionTemplate(transactionManager);

        try (ExecutorService pool = Executors.newFixedThreadPool(3)) {
            Future<?> holderFuture = pool.submit(() -> tx.executeWithoutResult(status -> {
                jdbc.queryForList("SELECT ingredient_id FROM b3_test.ingredients WHERE ingredient_id = 'GAO' FOR UPDATE");
                holderLocked.countDown();
                try {
                    if (!releaseHolder.await(15, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("Timed out waiting to release test lock");
                    }
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(ex);
                }
            }));

            assertThat(holderLocked.await(10, TimeUnit.SECONDS)).isTrue();

            Future<ReservationResult> workerA = pool.submit(
                    () -> reservationService.processOrderCreated(first, new EventProcessingContext(0, 1)));
            Future<ReservationResult> workerB = pool.submit(
                    () -> reservationService.processOrderCreated(second, new EventProcessingContext(1, 1)));

            // Wait until both real transactions are genuinely blocked in PostgreSQL on the same row lock
            long waitDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            int waitingLocks = 0;
            while (System.nanoTime() < waitDeadline) {
                waitingLocks = jdbc.queryForObject("""
                        SELECT count(*) FROM pg_stat_activity
                        WHERE datname = current_database()
                          AND state = 'active'
                          AND wait_event_type = 'Lock'
                          AND query LIKE '%b3_test%ingredients%'
                        """, Integer.class);
                if (waitingLocks >= 2) {
                    break;
                }
                Thread.sleep(25);
            }
            assertThat(waitingLocks).isGreaterThanOrEqualTo(2);

            releaseHolder.countDown();
            holderFuture.get(10, TimeUnit.SECONDS);
            ReservationResult rA = workerA.get(15, TimeUnit.SECONDS);
            ReservationResult rB = workerB.get(15, TimeUnit.SECONDS);

            assertThat(List.of(rA.outcome(), rB.outcome())).containsExactly(Outcome.APPLIED, Outcome.APPLIED);
            assertThat(List.of(rA.decision(), rB.decision())).containsExactlyInAnyOrder("RESERVED", "FAILED");
            assertThat(stock("GAO")).isEqualTo(900L);
            assertThat(stock("THIT_GA")).isZero();
            assertThat(count("SELECT count(*) FROM b3_test.inventory_order_decisions WHERE decision = 'RESERVED'"))
                    .isEqualTo(1L);
            assertThat(count("SELECT count(*) FROM b3_test.inventory_order_decisions WHERE decision = 'FAILED'"))
                    .isEqualTo(1L);
            assertThat(count("SELECT count(*) FROM b3_test.reservation_items")).isEqualTo(2L);
            assertThat(count("SELECT count(*) FROM b3_test.outbox_events")).isEqualTo(2L);
        } finally {
            releaseHolder.countDown();
        }
    }

    @Test
    void duplicateEventIdAndDuplicateOrConflictingOrderIdAreHandledIdempotently() {
        UUID orderId = UUID.randomUUID();
        UUID event1 = UUID.randomUUID();
        OrderCreatedEvent original = OrderCreatedEvent.v1(
                event1,
                orderId,
                SourceType.POS,
                "POS01",
                Instant.now(),
                List.of(
                        new OrderItemLine(1, "CHAO_GA", "Cháo gà", 1),
                        new OrderItemLine(2, "COM_GA", "Cơm gà", 2)));

        assertThat(reservationService.processOrderCreated(original, EventProcessingContext.DIRECT).outcome())
                .isEqualTo(Outcome.APPLIED);
        long expectedGao = 1000L - 50L - 200L;
        long expectedGa = 1500L - 100L - 300L;
        assertThat(stock("GAO")).isEqualTo(expectedGao);
        assertThat(stock("THIT_GA")).isEqualTo(expectedGa);

        // D04: replay identical eventId multiple times
        for (int i = 0; i < 3; i++) {
            ReservationResult dup = reservationService.processOrderCreated(original, EventProcessingContext.DIRECT);
            assertThat(dup.outcome()).isEqualTo(Outcome.DUPLICATE_EVENT);
            assertThat(dup.decision()).isEqualTo("RESERVED");
        }
        assertThat(stock("GAO")).isEqualTo(expectedGao);
        assertThat(stock("THIT_GA")).isEqualTo(expectedGa);
        assertThat(count("SELECT count(*) FROM b3_test.outbox_events")).isEqualTo(1L);

        // D05 same payload (different item order -> identical canonical fingerprint), new eventId
        UUID event2 = UUID.randomUUID();
        OrderCreatedEvent reorderedSameOrder = OrderCreatedEvent.v1(
                event2,
                orderId,
                SourceType.POS,
                "POS01",
                Instant.now(),
                List.of(
                        new OrderItemLine(1, "COM_GA", "Cơm gà", 2),
                        new OrderItemLine(2, "CHAO_GA", "Cháo gà", 1)));
        ReservationResult dupOrder = reservationService.processOrderCreated(
                reorderedSameOrder, EventProcessingContext.DIRECT);
        assertThat(dupOrder.outcome()).isEqualTo(Outcome.DUPLICATE_ORDER);
        assertThat(dupOrder.decision()).isEqualTo("RESERVED");
        assertThat(outcomeOf(event2)).isEqualTo("DUPLICATE_ORDER");

        // D05 conflicting payload, new eventId
        UUID event3 = UUID.randomUUID();
        OrderCreatedEvent conflictingOrder = singleDishEvent(event3, orderId, "COM_GA", "Cơm gà", 5);
        ReservationResult conflict = reservationService.processOrderCreated(
                conflictingOrder, EventProcessingContext.DIRECT);
        assertThat(conflict.outcome()).isEqualTo(Outcome.PAYLOAD_CONFLICT);
        assertThat(conflict.decision()).isEqualTo("RESERVED");
        assertThat(outcomeOf(event3)).isEqualTo("PAYLOAD_CONFLICT");

        assertThat(stock("GAO")).isEqualTo(expectedGao);
        assertThat(stock("THIT_GA")).isEqualTo(expectedGa);
        assertThat(count("SELECT count(*) FROM b3_test.outbox_events")).isEqualTo(1L);
        assertThat(count("SELECT count(*) FROM b3_test.inventory_order_decisions")).isEqualTo(1L);
    }

    @Test
    void failedUpdateRollsBackStockDeductionsAndAllowsCleanRetry() {
        UUID eventId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();
        OrderCreatedEvent event = singleDishEvent(eventId, orderId, "COM_GA", "Cơm gà", 1);

        jdbc.execute("ALTER TABLE b3_test.outbox_events ADD CONSTRAINT reject_b3_outbox CHECK (false) NOT VALID");
        try {
            assertThatThrownBy(() -> reservationService.processOrderCreated(event, EventProcessingContext.DIRECT))
                    .isInstanceOf(DataIntegrityViolationException.class);

            assertThat(stock("GAO")).isEqualTo(1000L);
            assertThat(stock("THIT_GA")).isEqualTo(1500L);
            assertThat(count("SELECT count(*) FROM b3_test.processed_events WHERE event_id = ?", eventId)).isZero();
            assertThat(count("SELECT count(*) FROM b3_test.inventory_order_decisions WHERE order_id = ?", orderId))
                    .isZero();
            assertThat(count("SELECT count(*) FROM b3_test.reservation_items WHERE order_id = ?", orderId)).isZero();
            assertThat(count("SELECT count(*) FROM b3_test.outbox_events WHERE aggregate_id = ?", orderId)).isZero();
        } finally {
            jdbc.execute("ALTER TABLE b3_test.outbox_events DROP CONSTRAINT reject_b3_outbox");
        }

        ReservationResult retried = reservationService.processOrderCreated(event, EventProcessingContext.DIRECT);
        assertThat(retried.outcome()).isEqualTo(Outcome.APPLIED);
        assertThat(retried.decision()).isEqualTo("RESERVED");
        assertThat(stock("GAO")).isEqualTo(900L);
        assertThat(stock("THIT_GA")).isEqualTo(1350L);
    }

    @Test
    void rejectsMalformedOrderCreatedBeforeOpeningTransaction() {
        OrderCreatedEvent duplicateDish = new OrderCreatedEvent(
                UUID.randomUUID(),
                1,
                EventType.OrderCreated,
                UUID.randomUUID(),
                SourceType.POS,
                "POS01",
                Instant.now(),
                new OrderCreatedEvent.Payload(List.of(
                        new OrderItemLine(1, "COM_GA", "Cơm gà", 1),
                        new OrderItemLine(2, "COM_GA", "Cơm gà", 2))));

        assertThatThrownBy(() -> reservationService.processOrderCreated(duplicateDish, EventProcessingContext.DIRECT))
                .isInstanceOf(InvalidEventException.class);
        assertThat(count("SELECT count(*) FROM b3_test.processed_events")).isZero();
    }

    private OrderCreatedEvent singleDishEvent(
            UUID eventId, UUID orderId, String dishId, String dishName, int quantity) {
        return OrderCreatedEvent.v1(
                eventId,
                orderId,
                SourceType.POS,
                "POS01",
                Instant.now(),
                List.of(new OrderItemLine(1, dishId, dishName, quantity)));
    }

    private long stock(String ingredientId) {
        return jdbc.queryForObject(
                "SELECT available_quantity FROM b3_test.ingredients WHERE ingredient_id = ?",
                Long.class,
                ingredientId);
    }

    private long count(String sql, Object... args) {
        return jdbc.queryForObject(sql, Long.class, args);
    }

    private String outcomeOf(UUID eventId) {
        return jdbc.queryForObject(
                "SELECT outcome FROM b3_test.processed_events WHERE event_id = ?",
                String.class,
                eventId);
    }

    private HttpResponse<String> get(String path) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                .timeout(Duration.ofSeconds(10))
                .GET()
                .build(), HttpResponse.BodyHandlers.ofString());
    }
}
