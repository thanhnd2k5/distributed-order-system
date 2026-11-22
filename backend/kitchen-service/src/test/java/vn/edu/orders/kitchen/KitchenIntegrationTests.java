package vn.edu.orders.kitchen;

import com.jayway.jsonpath.JsonPath;
import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import tools.jackson.databind.ObjectMapper;
import vn.edu.orders.contracts.events.CancelRejectedPayload;
import vn.edu.orders.contracts.events.CancelRequestedPayload;
import vn.edu.orders.contracts.events.CancelledBy;
import vn.edu.orders.contracts.events.EventEnvelope;
import vn.edu.orders.contracts.events.EventTopics;
import vn.edu.orders.contracts.events.EventType;
import vn.edu.orders.contracts.events.IngredientCheckChangedPayload;
import vn.edu.orders.contracts.events.IngredientQuantity;
import vn.edu.orders.contracts.events.KitchenStatus;
import vn.edu.orders.contracts.events.OrderCancelledPayload;
import vn.edu.orders.contracts.events.OrderFailedPayload;
import vn.edu.orders.contracts.events.OrderItemLine;
import vn.edu.orders.contracts.events.OrderReservedPayload;
import vn.edu.orders.contracts.events.ReasonCode;
import vn.edu.orders.contracts.events.ReleaseRequestedPayload;
import vn.edu.orders.kitchen.api.KitchenTicketResponse;
import vn.edu.orders.kitchen.api.RejectTicketRequest;
import vn.edu.orders.kitchen.application.EventProcessingContext;
import vn.edu.orders.kitchen.application.KitchenOrderService;
import vn.edu.orders.kitchen.application.KitchenProcessingResult.Outcome;
import vn.edu.orders.kitchen.application.TicketConflictException;
import vn.edu.orders.kitchen.config.KitchenProperties;
import vn.edu.orders.kitchen.infrastructure.KitchenOrderStore;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "outbox.enabled=false",
        "spring.kafka.listener.auto-startup=false",
        "spring.kafka.bootstrap-servers=localhost:19999",
        "spring.jpa.properties.hibernate.default_schema=v15_kitchen_test",
        "spring.flyway.default-schema=v15_kitchen_test",
        "spring.flyway.schemas=v15_kitchen_test",
        "kitchen.instance-id=kitchen-integration-test"
})
class KitchenIntegrationTests {
    private static final String SCHEMA = "v15_kitchen_test";

    @Value("${local.server.port}")
    private int port;

    @Autowired
    private JdbcTemplate jdbc;

    @Autowired
    private KitchenOrderService orderService;

    @Autowired
    private KitchenOrderStore store;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Autowired
    private ObjectMapper mapper;

    @Autowired
    private Clock clock;

    private final HttpClient http = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    @BeforeEach
    void resetTestSchema() {
        jdbc.update("DELETE FROM " + SCHEMA + ".outbox_events");
        jdbc.update("DELETE FROM " + SCHEMA + ".alerts");
        jdbc.update("DELETE FROM " + SCHEMA + ".flagged_ingredients");
        jdbc.update("DELETE FROM " + SCHEMA + ".kitchen_orders");
        jdbc.update("DELETE FROM " + SCHEMA + ".processed_events");
    }

    @Test
    @Tag("kb-1")
    void cancelArrivesBeforeOrderThenOrderReservedCancelsAndEmitsReleaseOnce() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID cancelEventId = UUID.randomUUID();

        var cancelRes = orderService.processCancelRequested(
                EventTopics.cancelEventsTopic("POS01"),
                cancelEnvelope(cancelEventId, orderId, "POS01", ReasonCode.CUSTOMER_CHANGED_MIND),
                EventProcessingContext.direct(EventTopics.cancelEventsTopic("POS01")));
        assertThat(cancelRes.outcome()).isEqualTo(Outcome.APPLIED);
        assertThat(cancelRes.status()).isEqualTo(KitchenStatus.CANCEL_PENDING);

        // No ticket shown in GET /tickets while CANCEL_PENDING
        var ticketsWhilePending = get("/api/kitchen/tickets");
        assertThat(ticketsWhilePending.statusCode()).isEqualTo(200);
        assertThat(JsonPath.<List<?>>read(ticketsWhilePending.body(), "$")).isEmpty();

        // CancelPending emitted to status-events.POS01
        assertThat(countOutbox(orderId, EventType.CancelPending)).isEqualTo(1L);
        assertThat(outboxTopics(orderId, EventType.CancelPending)).containsExactly("status-events.POS01");

        // OrderReserved arrives from Inventory
        var reservedRes = orderService.processOrderReserved(
                reservedEnvelope(UUID.randomUUID(), orderId, "POS01",
                        List.of(new OrderItemLine(1, "COM_GA", "Cơm gà", 2)),
                        List.of(new IngredientQuantity("GAO", new BigDecimal("200.000")),
                                new IngredientQuantity("THIT_GA", new BigDecimal("300.000")))),
                EventProcessingContext.direct(EventTopics.KITCHEN_ORDERS));
        assertThat(reservedRes.outcome()).isEqualTo(Outcome.APPLIED);
        assertThat(reservedRes.status()).isEqualTo(KitchenStatus.CANCELLED);

        // Still no active ticket
        var ticketsAfter = get("/api/kitchen/tickets");
        assertThat(JsonPath.<List<?>>read(ticketsAfter.body(), "$")).isEmpty();

        // DB state verified
        Map<String, Object> row = jdbc.queryForMap(
                "SELECT status, origin_branch, cancelled_by, close_reason FROM " + SCHEMA + ".kitchen_orders WHERE order_id = ?",
                orderId);
        assertThat(row.get("status")).isEqualTo("CANCELLED");
        assertThat(row.get("origin_branch")).isEqualTo("POS01");
        assertThat(row.get("cancelled_by")).isEqualTo("CUSTOMER");
        assertThat(row.get("close_reason")).isEqualTo("CUSTOMER_CHANGED_MIND");

        // Exactly 1 OrderCancelled and 1 ReleaseRequested emitted in same tx
        assertThat(countOutbox(orderId, EventType.OrderCancelled)).isEqualTo(1L);
        assertThat(outboxTopics(orderId, EventType.OrderCancelled)).containsExactly("status-events.POS01");
        assertThat(countOutbox(orderId, EventType.ReleaseRequested)).isEqualTo(1L);
        assertThat(outboxTopics(orderId, EventType.ReleaseRequested)).containsExactly(EventTopics.STOCK_COMMANDS);

        ReleaseRequestedPayload releasePayload = readOutboxPayload(orderId, EventType.ReleaseRequested, ReleaseRequestedPayload.class);
        assertThat(releasePayload.cancelledBy()).isEqualTo(CancelledBy.CUSTOMER);
        assertThat(releasePayload.reasonCode()).isEqualTo(ReasonCode.CUSTOMER_CHANGED_MIND);
        assertThat(releasePayload.spoiledIngredients()).isEmpty();
    }

    @Test
    @Tag("kb-2")
    void duplicateOrderReservedAfterCancelPendingCancellationCreatesNoTicketAndNoSecondRelease() {
        UUID orderId = UUID.randomUUID();
        UUID reservedEventId1 = UUID.randomUUID();

        orderService.processCancelRequested(
                EventTopics.cancelEventsTopic("POS01"),
                cancelEnvelope(UUID.randomUUID(), orderId, "POS01", ReasonCode.CUSTOMER_WAITED_TOO_LONG),
                EventProcessingContext.direct(EventTopics.cancelEventsTopic("POS01")));

        var firstReserved = reservedEnvelope(reservedEventId1, orderId, "POS01",
                List.of(new OrderItemLine(1, "COM_GA", "Cơm gà", 1)),
                List.of(new IngredientQuantity("THIT_GA", new BigDecimal("150.000"))));
        orderService.processOrderReserved(firstReserved, EventProcessingContext.direct(EventTopics.KITCHEN_ORDERS));

        UUID originalCancelOutboxId = jdbc.queryForObject(
                "SELECT event_id FROM " + SCHEMA + ".outbox_events WHERE aggregate_id = ? AND event_type = 'OrderCancelled'",
                UUID.class, orderId);

        // Case 1: same event_id -> DUPLICATE_EVENT, publishes nothing
        var dupEventRes = orderService.processOrderReserved(firstReserved, EventProcessingContext.direct(EventTopics.KITCHEN_ORDERS));
        assertThat(dupEventRes.outcome()).isEqualTo(Outcome.DUPLICATE_EVENT);
        assertThat(countOutbox(orderId, EventType.OrderCancelled)).isEqualTo(1L);
        assertThat(countOutbox(orderId, EventType.ReleaseRequested)).isEqualTo(1L);

        // Case 2: new event_id for same order_id -> STORED_RESULT re-emits OrderCancelled with NEW event_id, NO second ReleaseRequested
        var resentReserved = reservedEnvelope(UUID.randomUUID(), orderId, "POS01",
                List.of(new OrderItemLine(1, "COM_GA", "Cơm gà", 1)),
                List.of(new IngredientQuantity("THIT_GA", new BigDecimal("150.000"))));
        var dupOrderRes = orderService.processOrderReserved(resentReserved, EventProcessingContext.direct(EventTopics.KITCHEN_ORDERS));
        assertThat(dupOrderRes.outcome()).isEqualTo(Outcome.STORED_RESULT);
        assertThat(dupOrderRes.status()).isEqualTo(KitchenStatus.CANCELLED);

        assertThat(orderService.listTickets(null)).isEmpty();
        assertThat(countOutbox(orderId, EventType.ReleaseRequested)).isEqualTo(1L);
        List<UUID> cancelOutboxIds = jdbc.queryForList(
                "SELECT event_id FROM " + SCHEMA + ".outbox_events WHERE aggregate_id = ? AND event_type = 'OrderCancelled' ORDER BY seq_id",
                UUID.class, orderId);
        assertThat(cancelOutboxIds).hasSize(2);
        assertThat(cancelOutboxIds.get(1)).isNotEqualTo(originalCancelOutboxId);
    }

    @Test
    @Tag("kb-3")
    void raceBetweenCancelRequestedAndStartCookingExactlyOneWins() throws Exception {
        int iterations = 60;
        AtomicInteger startWins = new AtomicInteger();
        AtomicInteger cancelWins = new AtomicInteger();
        ExecutorService pool = Executors.newFixedThreadPool(2);

        try {
            for (int i = 0; i < iterations; i++) {
                UUID orderId = UUID.randomUUID();
                orderService.processOrderReserved(
                        reservedEnvelope(UUID.randomUUID(), orderId, "POS01",
                                List.of(new OrderItemLine(1, "COM_GA", "Cơm gà", 1)),
                                List.of(new IngredientQuantity("THIT_GA", new BigDecimal("150.000")))),
                        EventProcessingContext.direct(EventTopics.KITCHEN_ORDERS));

                CountDownLatch ready = new CountDownLatch(2);
                CountDownLatch go = new CountDownLatch(1);
                CountDownLatch done = new CountDownLatch(2);

                AtomicReference<Throwable> startErr = new AtomicReference<>();
                AtomicReference<KitchenTicketResponse> startRes = new AtomicReference<>();
                AtomicReference<Outcome> cancelOutcome = new AtomicReference<>();

                pool.submit(() -> {
                    ready.countDown();
                    try {
                        go.await();
                        startRes.set(orderService.startTicket(orderId));
                    } catch (Throwable t) {
                        startErr.set(t);
                    } finally {
                        done.countDown();
                    }
                });

                pool.submit(() -> {
                    ready.countDown();
                    try {
                        go.await();
                        var res = orderService.processCancelRequested(
                                EventTopics.cancelEventsTopic("POS01"),
                                cancelEnvelope(UUID.randomUUID(), orderId, "POS01", ReasonCode.CUSTOMER_CHANGED_MIND),
                                EventProcessingContext.direct(EventTopics.cancelEventsTopic("POS01")));
                        cancelOutcome.set(res.outcome());
                    } catch (Throwable t) {
                        throw new RuntimeException(t);
                    } finally {
                        done.countDown();
                    }
                });

                assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
                go.countDown();
                assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();

                String dbStatus = jdbc.queryForObject(
                        "SELECT status FROM " + SCHEMA + ".kitchen_orders WHERE order_id = ?", String.class, orderId);

                if (startRes.get() != null) {
                    startWins.incrementAndGet();
                    assertThat(startErr.get()).isNull();
                    assertThat(cancelOutcome.get()).isEqualTo(Outcome.CANCEL_REJECTED);
                    assertThat(dbStatus).isEqualTo("COOKING");
                    assertThat(countOutbox(orderId, EventType.StockConsumed)).isEqualTo(1L);
                    assertThat(countOutbox(orderId, EventType.CancelRejected)).isEqualTo(1L);
                    assertThat(countOutbox(orderId, EventType.OrderCancelled)).isZero();
                    assertThat(countOutbox(orderId, EventType.ReleaseRequested)).isZero();
                } else {
                    cancelWins.incrementAndGet();
                    assertThat(startErr.get()).isInstanceOf(TicketConflictException.class);
                    assertThat(((TicketConflictException) startErr.get()).currentStatus()).isEqualTo("CANCELLED");
                    assertThat(cancelOutcome.get()).isEqualTo(Outcome.APPLIED);
                    assertThat(dbStatus).isEqualTo("CANCELLED");
                    assertThat(countOutbox(orderId, EventType.OrderCancelled)).isEqualTo(1L);
                    assertThat(countOutbox(orderId, EventType.ReleaseRequested)).isEqualTo(1L);
                    assertThat(countOutbox(orderId, EventType.StockConsumed)).isZero();
                    assertThat(countOutbox(orderId, EventType.CancelRejected)).isZero();
                }
            }
        } finally {
            pool.shutdownNow();
        }
        assertThat(startWins.get() + cancelWins.get()).isEqualTo(iterations);
    }

    @Test
    @Tag("kb-6")
    void rejectWithSpoiledIngredientValidatesEmitsReleaseAndShowsWarningOnQueuedTickets() throws Exception {
        UUID orderToReject = UUID.randomUUID();
        UUID queuedWithMeat = UUID.randomUUID();
        UUID queuedWithoutMeat = UUID.randomUUID();

        orderService.processOrderReserved(
                reservedEnvelope(UUID.randomUUID(), orderToReject, "POS01",
                        List.of(new OrderItemLine(1, "COM_GA", "Cơm gà", 1)),
                        List.of(new IngredientQuantity("GAO", new BigDecimal("100.000")),
                                new IngredientQuantity("THIT_GA", new BigDecimal("150.000")))),
                EventProcessingContext.direct(EventTopics.KITCHEN_ORDERS));

        orderService.processOrderReserved(
                reservedEnvelope(UUID.randomUUID(), queuedWithMeat, "POS02",
                        List.of(new OrderItemLine(1, "CHAO_GA", "Cháo gà", 1)),
                        List.of(new IngredientQuantity("GAO", new BigDecimal("50.000")),
                                new IngredientQuantity("THIT_GA", new BigDecimal("100.000")))),
                EventProcessingContext.direct(EventTopics.KITCHEN_ORDERS));

        orderService.processOrderReserved(
                reservedEnvelope(UUID.randomUUID(), queuedWithoutMeat, "POS01",
                        List.of(new OrderItemLine(1, "COM_TRANG", "Cơm trắng", 1)),
                        List.of(new IngredientQuantity("GAO", new BigDecimal("100.000")))),
                EventProcessingContext.direct(EventTopics.KITCHEN_ORDERS));

        // 400 when SPOILED_INGREDIENT missing spoiledIngredients
        var missingSpoiled = postJson("/api/kitchen/tickets/" + orderToReject + "/reject",
                "{\"reasonCode\":\"SPOILED_INGREDIENT\",\"spoiledIngredients\":[]}");
        assertThat(missingSpoiled.statusCode()).isEqualTo(400);

        // 400 when spoiled ingredient does not belong to ticket's reserved ingredients
        var invalidSpoiled = postJson("/api/kitchen/tickets/" + orderToReject + "/reject",
                "{\"reasonCode\":\"SPOILED_INGREDIENT\",\"spoiledIngredients\":[\"THIT_BO\"]}");
        assertThat(invalidSpoiled.statusCode()).isEqualTo(400);

        // 200 valid reject with THIT_GA
        var rejectResp = postJson("/api/kitchen/tickets/" + orderToReject + "/reject",
                "{\"reasonCode\":\"SPOILED_INGREDIENT\",\"spoiledIngredients\":[\"THIT_GA\"]}");
        assertThat(rejectResp.statusCode()).isEqualTo(200);
        assertThat(JsonPath.<String>read(rejectResp.body(), "$.status")).isEqualTo("CANCELLED");
        assertThat(JsonPath.<String>read(rejectResp.body(), "$.cancelledBy")).isEqualTo("KITCHEN");
        assertThat(JsonPath.<String>read(rejectResp.body(), "$.closeReason")).isEqualTo("SPOILED_INGREDIENT");

        assertThat(countOutbox(orderToReject, EventType.OrderCancelled)).isEqualTo(1L);
        assertThat(countOutbox(orderToReject, EventType.ReleaseRequested)).isEqualTo(1L);
        ReleaseRequestedPayload releasePayload = readOutboxPayload(orderToReject, EventType.ReleaseRequested, ReleaseRequestedPayload.class);
        assertThat(releasePayload.cancelledBy()).isEqualTo(CancelledBy.KITCHEN);
        assertThat(releasePayload.reasonCode()).isEqualTo(ReasonCode.SPOILED_INGREDIENT);
        assertThat(releasePayload.spoiledIngredients()).containsExactly("THIT_GA");

        // Receive IngredientCheckChanged(THIT_GA, needsCheck=true, epoch=1, seq=5)
        var flagRes = orderService.processIngredientCheckChanged(
                ingredientCheckEnvelope(UUID.randomUUID(), "THIT_GA", true, 1, 5L),
                mapper.readTree("{\"ingredientId\":\"THIT_GA\",\"needsCheck\":true,\"epoch\":1,\"seq\":5}"),
                EventProcessingContext.direct(EventTopics.MENU_EVENTS));
        assertThat(flagRes.outcome()).isEqualTo(Outcome.APPLIED);

        // Stale version (epoch=1, seq=4, needsCheck=false) must be ignored
        var staleRes = orderService.processIngredientCheckChanged(
                ingredientCheckEnvelope(UUID.randomUUID(), "THIT_GA", false, 1, 4L),
                mapper.readTree("{\"ingredientId\":\"THIT_GA\",\"needsCheck\":false,\"epoch\":1,\"seq\":4}"),
                EventProcessingContext.direct(EventTopics.MENU_EVENTS));
        assertThat(staleRes.outcome()).isEqualTo(Outcome.STALE_VERSION);

        // GET /tickets shows warning=true on queuedWithMeat, warning=false on queuedWithoutMeat
        var ticketsResp = get("/api/kitchen/tickets");
        assertThat(ticketsResp.statusCode()).isEqualTo(200);
        List<Map<String, Object>> list = mapper.readValue(
                ticketsResp.body(), new tools.jackson.core.type.TypeReference<List<Map<String, Object>>>() {});
        assertThat(list).hasSize(2);
        Map<String, Object> meatTicket = list.stream()
                .filter(t -> queuedWithMeat.toString().equals(t.get("orderId")))
                .findFirst().orElseThrow();
        Map<String, Object> riceTicket = list.stream()
                .filter(t -> queuedWithoutMeat.toString().equals(t.get("orderId")))
                .findFirst().orElseThrow();

        assertThat(meatTicket.get("warning")).isEqualTo(true);
        assertThat(meatTicket.get("flaggedIngredients")).isEqualTo(List.of("THIT_GA"));
        assertThat(riceTicket.get("warning")).isEqualTo(false);
        assertThat(riceTicket.get("flaggedIngredients")).isEqualTo(List.of());
    }

    @Test
    @Tag("kb-8")
    void cancelPendingOrderRejectedByInventoryMovesToFailed() {
        UUID orderId = UUID.randomUUID();
        orderService.processCancelRequested(
                EventTopics.cancelEventsTopic("POS01"),
                cancelEnvelope(UUID.randomUUID(), orderId, "POS01", ReasonCode.CUSTOMER_CHANGED_MIND),
                EventProcessingContext.direct(EventTopics.cancelEventsTopic("POS01")));

        var failRes = orderService.processOrderFailed(
                failedEnvelope(UUID.randomUUID(), orderId, "POS01", ReasonCode.INSUFFICIENT_INGREDIENTS),
                EventProcessingContext.direct(EventTopics.KITCHEN_ORDERS));
        assertThat(failRes.outcome()).isEqualTo(Outcome.APPLIED);
        assertThat(failRes.status()).isEqualTo(KitchenStatus.FAILED);

        Map<String, Object> row = jdbc.queryForMap(
                "SELECT status, origin_branch, close_reason FROM " + SCHEMA + ".kitchen_orders WHERE order_id = ?",
                orderId);
        assertThat(row.get("status")).isEqualTo("FAILED");
        assertThat(row.get("origin_branch")).isEqualTo("POS01");
        assertThat(row.get("close_reason")).isEqualTo("INSUFFICIENT_INGREDIENTS");

        // No OrderCancelled or ReleaseRequested
        assertThat(countOutbox(orderId, EventType.OrderCancelled)).isZero();
        assertThat(countOutbox(orderId, EventType.ReleaseRequested)).isZero();
    }

    @Test
    @Tag("kb-23")
    void cancelRequestOnCookingAndDoneTicketEmitsCancelRejectedWithKitchenStatus() throws Exception {
        UUID orderId = UUID.randomUUID();
        orderService.processOrderReserved(
                reservedEnvelope(UUID.randomUUID(), orderId, "POS01",
                        List.of(new OrderItemLine(1, "PHO_BO", "Phở bò", 1)),
                        List.of(new IngredientQuantity("THIT_BO", new BigDecimal("150.000")))),
                EventProcessingContext.direct(EventTopics.KITCHEN_ORDERS));

        var startResp = postJson("/api/kitchen/tickets/" + orderId + "/start", null);
        assertThat(startResp.statusCode()).isEqualTo(200);
        assertThat(JsonPath.<String>read(startResp.body(), "$.status")).isEqualTo("COOKING");

        // Cancel arrives while COOKING
        var cancelWhileCooking = orderService.processCancelRequested(
                EventTopics.cancelEventsTopic("POS01"),
                cancelEnvelope(UUID.randomUUID(), orderId, "POS01", ReasonCode.CUSTOMER_CHANGED_MIND),
                EventProcessingContext.direct(EventTopics.cancelEventsTopic("POS01")));
        assertThat(cancelWhileCooking.outcome()).isEqualTo(Outcome.CANCEL_REJECTED);
        assertThat(cancelWhileCooking.status()).isEqualTo(KitchenStatus.COOKING);

        CancelRejectedPayload rejectedCooking = readOutboxPayload(orderId, EventType.CancelRejected, CancelRejectedPayload.class);
        assertThat(rejectedCooking.kitchenStatus()).isEqualTo(KitchenStatus.COOKING);
        assertThat(rejectedCooking.message()).contains("started cooking");
        assertThat(countOutbox(orderId, EventType.OrderCancelled)).isZero();
        assertThat(countOutbox(orderId, EventType.ReleaseRequested)).isZero();

        // Complete ticket -> DONE
        var completeResp = postJson("/api/kitchen/tickets/" + orderId + "/complete", null);
        assertThat(completeResp.statusCode()).isEqualTo(200);
        assertThat(JsonPath.<String>read(completeResp.body(), "$.status")).isEqualTo("DONE");
        assertThat(countOutbox(orderId, EventType.OrderCompleted)).isEqualTo(1L);

        // Precedence Rule 2: Cancel from origin branch on DONE emits CancelRejected(DONE), NOT OrderCompleted!
        var cancelWhileDone = orderService.processCancelRequested(
                EventTopics.cancelEventsTopic("POS01"),
                cancelEnvelope(UUID.randomUUID(), orderId, "POS01", ReasonCode.CUSTOMER_CHANGED_MIND),
                EventProcessingContext.direct(EventTopics.cancelEventsTopic("POS01")));
        assertThat(cancelWhileDone.outcome()).isEqualTo(Outcome.CANCEL_REJECTED);
        assertThat(cancelWhileDone.status()).isEqualTo(KitchenStatus.DONE);
        assertThat(countOutbox(orderId, EventType.CancelRejected)).isEqualTo(2L);
        assertThat(countOutbox(orderId, EventType.OrderCompleted)).isEqualTo(1L);

        String latestRejectedJson = jdbc.queryForObject(
                "SELECT payload::text FROM " + SCHEMA + ".outbox_events WHERE aggregate_id = ? AND event_type = 'CancelRejected' ORDER BY seq_id DESC LIMIT 1",
                String.class, orderId);
        EventEnvelope<CancelRejectedPayload> env = mapper.readValue(latestRejectedJson,
                mapper.getTypeFactory().constructParametricType(EventEnvelope.class, CancelRejectedPayload.class));
        assertThat(env.payload().kitchenStatus()).isEqualTo(KitchenStatus.DONE);
    }

    @Test
    @Tag("kb-27")
    void cancelFromWrongBranchOnExistingTicketIsIgnoredAndRaisesAlertEvenIfBodySpoofed() throws Exception {
        UUID orderId = UUID.randomUUID();
        orderService.processOrderReserved(
                reservedEnvelope(UUID.randomUUID(), orderId, "POS02",
                        List.of(new OrderItemLine(1, "COM_GA", "Cơm gà", 1)),
                        List.of(new IngredientQuantity("THIT_GA", new BigDecimal("150.000")))),
                EventProcessingContext.direct(EventTopics.KITCHEN_ORDERS));

        // Topic is cancel-events.POS01 while body branchId is spoofed as POS02 -> must parse POS01 from topic!
        var wrongBranchRes = orderService.processCancelRequested(
                EventTopics.cancelEventsTopic("POS01"),
                cancelEnvelope(UUID.randomUUID(), orderId, "POS02", ReasonCode.CUSTOMER_CHANGED_MIND),
                EventProcessingContext.direct(EventTopics.cancelEventsTopic("POS01")));
        assertThat(wrongBranchRes.outcome()).isEqualTo(Outcome.IGNORED_WRONG_BRANCH);
        assertThat(wrongBranchRes.status()).isEqualTo(KitchenStatus.QUEUED);

        // Status unchanged, no reply emitted
        String status = jdbc.queryForObject(
                "SELECT status FROM " + SCHEMA + ".kitchen_orders WHERE order_id = ?", String.class, orderId);
        assertThat(status).isEqualTo("QUEUED");
        assertThat(countAllOutbox(orderId)).isZero();

        var alertsResp = get("/api/kitchen/alerts?kind=WRONG_BRANCH_CANCEL");
        assertThat(alertsResp.statusCode()).isEqualTo(200);
        assertThat(JsonPath.<List<String>>read(alertsResp.body(), "$[*].orderId")).contains(orderId.toString());
        assertThat(JsonPath.<String>read(alertsResp.body(), "$[0].detail.requestingBranch")).isEqualTo("POS01");
        assertThat(JsonPath.<String>read(alertsResp.body(), "$[0].detail.originBranch")).isEqualTo("POS02");
    }

    @Test
    @Tag("kb-28")
    void twoKitchenInstancesRaceOnOrderReservedAndCancelRequestedConvergeToTable32() throws Exception {
        KitchenOrderService kitchen1 = new KitchenOrderService(
                store, transactionManager, mapper, clock, new KitchenProperties("kitchen-1", 300, 2000));
        KitchenOrderService kitchen2 = new KitchenOrderService(
                store, transactionManager, mapper, clock, new KitchenProperties("kitchen-2", 300, 2000));

        int iterations = 50;
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            for (int i = 0; i < iterations; i++) {
                UUID orderId = UUID.randomUUID();
                CountDownLatch ready = new CountDownLatch(2);
                CountDownLatch go = new CountDownLatch(1);
                CountDownLatch done = new CountDownLatch(2);

                pool.submit(() -> {
                    ready.countDown();
                    try {
                        go.await();
                        kitchen1.processOrderReserved(
                                reservedEnvelope(UUID.randomUUID(), orderId, "POS01",
                                        List.of(new OrderItemLine(1, "COM_GA", "Cơm gà", 1)),
                                        List.of(new IngredientQuantity("THIT_GA", new BigDecimal("150.000")))),
                                EventProcessingContext.direct(EventTopics.KITCHEN_ORDERS));
                    } catch (Throwable t) {
                        throw new RuntimeException(t);
                    } finally {
                        done.countDown();
                    }
                });

                pool.submit(() -> {
                    ready.countDown();
                    try {
                        go.await();
                        kitchen2.processCancelRequested(
                                EventTopics.cancelEventsTopic("POS01"),
                                cancelEnvelope(UUID.randomUUID(), orderId, "POS01", ReasonCode.CUSTOMER_CHANGED_MIND),
                                EventProcessingContext.direct(EventTopics.cancelEventsTopic("POS01")));
                    } catch (Throwable t) {
                        throw new RuntimeException(t);
                    } finally {
                        done.countDown();
                    }
                });

                assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
                go.countDown();
                assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();

                long rowCount = jdbc.queryForObject(
                        "SELECT count(*) FROM " + SCHEMA + ".kitchen_orders WHERE order_id = ?", Long.class, orderId);
                assertThat(rowCount).isEqualTo(1L);

                Map<String, Object> row = jdbc.queryForMap(
                        "SELECT status, origin_branch, cancelled_by, close_reason FROM " + SCHEMA + ".kitchen_orders WHERE order_id = ?",
                        orderId);
                assertThat(row.get("status")).isEqualTo("CANCELLED");
                assertThat(row.get("origin_branch")).isEqualTo("POS01");
                assertThat(row.get("cancelled_by")).isEqualTo("CUSTOMER");
                assertThat(row.get("close_reason")).isEqualTo("CUSTOMER_CHANGED_MIND");

                assertThat(countOutbox(orderId, EventType.OrderCancelled)).isEqualTo(1L);
                assertThat(countOutbox(orderId, EventType.ReleaseRequested)).isEqualTo(1L);
            }
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    @Tag("kb-33")
    void failedOrderArrivesFirstThenCancelRequestNeverCreatesCancelPendingAndReturnsFailed() throws Exception {
        UUID orderId = UUID.randomUUID();
        var failRes = orderService.processOrderFailed(
                failedEnvelope(UUID.randomUUID(), orderId, "POS01", ReasonCode.DISH_HAS_NO_RECIPE),
                EventProcessingContext.direct(EventTopics.KITCHEN_ORDERS));
        assertThat(failRes.outcome()).isEqualTo(Outcome.APPLIED);
        assertThat(failRes.status()).isEqualTo(KitchenStatus.FAILED);
        assertThat(countAllOutbox(orderId)).isZero();

        // First origin-branch cancel request hitting FAILED -> Precedence Rule 3 returns stored OrderFailed
        var cancelRes = orderService.processCancelRequested(
                EventTopics.cancelEventsTopic("POS01"),
                cancelEnvelope(UUID.randomUUID(), orderId, "POS01", ReasonCode.CUSTOMER_CHANGED_MIND),
                EventProcessingContext.direct(EventTopics.cancelEventsTopic("POS01")));
        assertThat(cancelRes.outcome()).isEqualTo(Outcome.STORED_RESULT);
        assertThat(cancelRes.status()).isEqualTo(KitchenStatus.FAILED);

        String dbStatus = jdbc.queryForObject(
                "SELECT status FROM " + SCHEMA + ".kitchen_orders WHERE order_id = ?", String.class, orderId);
        assertThat(dbStatus).isEqualTo("FAILED");
        assertThat(countOutbox(orderId, EventType.CancelPending)).isZero();
        assertThat(countOutbox(orderId, EventType.OrderFailed)).isEqualTo(1L);

        OrderFailedPayload storedFailed = readOutboxPayload(orderId, EventType.OrderFailed, OrderFailedPayload.class);
        assertThat(storedFailed.originBranch()).isEqualTo("POS01");
        assertThat(storedFailed.reasonCode()).isEqualTo(ReasonCode.DISH_HAS_NO_RECIPE);

        // Wrong-branch cancel on FAILED -> Precedence Rule 1 ignored + alert, no reply
        var wrongCancel = orderService.processCancelRequested(
                EventTopics.cancelEventsTopic("POS02"),
                cancelEnvelope(UUID.randomUUID(), orderId, "POS02", ReasonCode.CUSTOMER_CHANGED_MIND),
                EventProcessingContext.direct(EventTopics.cancelEventsTopic("POS02")));
        assertThat(wrongCancel.outcome()).isEqualTo(Outcome.IGNORED_WRONG_BRANCH);
        assertThat(countOutbox(orderId, EventType.OrderFailed)).isEqualTo(1L);
        assertThat(orderService.listAlerts("WRONG_BRANCH_CANCEL")).hasSize(1);
    }

    @Test
    @Tag("kb-34")
    void wrongBranchCancelBeforeOrderReservedIsDismissedWithAlertAndOrderQueuesNormally() {
        UUID orderId = UUID.randomUUID();
        orderService.processCancelRequested(
                EventTopics.cancelEventsTopic("POS02"),
                cancelEnvelope(UUID.randomUUID(), orderId, "POS02", ReasonCode.CUSTOMER_CHANGED_MIND),
                EventProcessingContext.direct(EventTopics.cancelEventsTopic("POS02")));

        var reservedRes = orderService.processOrderReserved(
                reservedEnvelope(UUID.randomUUID(), orderId, "POS01",
                        List.of(new OrderItemLine(1, "COM_GA", "Cơm gà", 1)),
                        List.of(new IngredientQuantity("THIT_GA", new BigDecimal("150.000")))),
                EventProcessingContext.direct(EventTopics.KITCHEN_ORDERS));
        assertThat(reservedRes.outcome()).isEqualTo(Outcome.APPLIED);
        assertThat(reservedRes.status()).isEqualTo(KitchenStatus.QUEUED);

        Map<String, Object> row = jdbc.queryForMap(
                "SELECT status, origin_branch, cancel_branches::text, cancel_reasons::text FROM " + SCHEMA + ".kitchen_orders WHERE order_id = ?",
                orderId);
        assertThat(row.get("status")).isEqualTo("QUEUED");
        assertThat(row.get("origin_branch")).isEqualTo("POS01");
        assertThat(row.get("cancel_branches")).isEqualTo("{}");
        assertThat(row.get("cancel_reasons")).isEqualTo("{}");

        assertThat(orderService.listTickets(null)).hasSize(1);
        assertThat(countOutbox(orderId, EventType.OrderCancelled)).isZero();
        assertThat(countOutbox(orderId, EventType.ReleaseRequested)).isZero();

        var alerts = orderService.listAlerts("WRONG_BRANCH_CANCEL");
        assertThat(alerts).hasSize(1);
        assertThat(alerts.get(0).orderId()).isEqualTo(orderId);
        assertThat(alerts.get(0).detail().get("originBranch")).isEqualTo("POS01");
        assertThat(alerts.get(0).detail().get("dismissedBranches")).isEqualTo(List.of("POS02"));
    }

    @Test
    @Tag("kb-35")
    void wrongBranchCancelAndOriginBranchCancelBothBeforeOrderReservedUsesOriginReasonAndDoesNotOverwriteSameBranchReason() throws Exception {
        UUID orderId = UUID.randomUUID();

        // POS02 sends cancel first
        orderService.processCancelRequested(
                EventTopics.cancelEventsTopic("POS02"),
                cancelEnvelope(UUID.randomUUID(), orderId, "POS02", ReasonCode.CUSTOMER_CHANGED_MIND),
                EventProcessingContext.direct(EventTopics.cancelEventsTopic("POS02")));

        // POS01 sends cancel second with WRONG_ORDER_ENTERED
        orderService.processCancelRequested(
                EventTopics.cancelEventsTopic("POS01"),
                cancelEnvelope(UUID.randomUUID(), orderId, "POS01", ReasonCode.WRONG_ORDER_ENTERED),
                EventProcessingContext.direct(EventTopics.cancelEventsTopic("POS01")));

        // Repeat POS01 cancel with a DIFFERENT reason must NOT overwrite earlier WRONG_ORDER_ENTERED
        var repeatPos01 = orderService.processCancelRequested(
                EventTopics.cancelEventsTopic("POS01"),
                cancelEnvelope(UUID.randomUUID(), orderId, "POS01", ReasonCode.CUSTOMER_WAITED_TOO_LONG),
                EventProcessingContext.direct(EventTopics.cancelEventsTopic("POS01")));
        assertThat(repeatPos01.outcome()).isEqualTo(Outcome.STORED_RESULT);
        assertThat(countOutbox(orderId, EventType.CancelPending)).isEqualTo(3L);

        // OrderReserved from POS01 arrives
        var reservedRes = orderService.processOrderReserved(
                reservedEnvelope(UUID.randomUUID(), orderId, "POS01",
                        List.of(new OrderItemLine(1, "COM_GA", "Cơm gà", 1)),
                        List.of(new IngredientQuantity("THIT_GA", new BigDecimal("150.000")))),
                EventProcessingContext.direct(EventTopics.KITCHEN_ORDERS));
        assertThat(reservedRes.outcome()).isEqualTo(Outcome.APPLIED);
        assertThat(reservedRes.status()).isEqualTo(KitchenStatus.CANCELLED);

        OrderCancelledPayload cancelledPayload = readOutboxPayload(orderId, EventType.OrderCancelled, OrderCancelledPayload.class);
        assertThat(cancelledPayload.cancelledBy()).isEqualTo(CancelledBy.CUSTOMER);
        assertThat(cancelledPayload.reasonCode()).isEqualTo(ReasonCode.WRONG_ORDER_ENTERED);

        ReleaseRequestedPayload releasePayload = readOutboxPayload(orderId, EventType.ReleaseRequested, ReleaseRequestedPayload.class);
        assertThat(releasePayload.cancelledBy()).isEqualTo(CancelledBy.CUSTOMER);
        assertThat(releasePayload.reasonCode()).isEqualTo(ReasonCode.WRONG_ORDER_ENTERED);
        assertThat(countOutbox(orderId, EventType.ReleaseRequested)).isEqualTo(1L);
    }

    @Test
    @Tag("kb-48")
    void clearingFlaggedIngredientRemovesRedWarningOnQueuedTicketAndRespectsVersionOrder() throws Exception {
        UUID orderId = UUID.randomUUID();
        orderService.processOrderReserved(
                reservedEnvelope(UUID.randomUUID(), orderId, "POS01",
                        List.of(new OrderItemLine(1, "COM_GA", "Cơm gà", 1)),
                        List.of(new IngredientQuantity("THIT_GA", new BigDecimal("150.000")))),
                EventProcessingContext.direct(EventTopics.KITCHEN_ORDERS));

        orderService.processIngredientCheckChanged(
                ingredientCheckEnvelope(UUID.randomUUID(), "THIT_GA", true, 1, 10L),
                mapper.readTree("{\"ingredientId\":\"THIT_GA\",\"needsCheck\":true,\"epoch\":1,\"seq\":10}"),
                EventProcessingContext.direct(EventTopics.MENU_EVENTS));

        var warnedTickets = orderService.listTickets(null);
        assertThat(warnedTickets).hasSize(1);
        assertThat(warnedTickets.get(0).warning()).isTrue();
        assertThat(warnedTickets.get(0).flaggedIngredients()).containsExactly("THIT_GA");

        // Clear flag at (1, 11)
        orderService.processIngredientCheckChanged(
                ingredientCheckEnvelope(UUID.randomUUID(), "THIT_GA", false, 1, 11L),
                mapper.readTree("{\"ingredientId\":\"THIT_GA\",\"needsCheck\":false,\"epoch\":1,\"seq\":11}"),
                EventProcessingContext.direct(EventTopics.MENU_EVENTS));

        // Stale flag (1, 9) must be ignored
        var stale = orderService.processIngredientCheckChanged(
                ingredientCheckEnvelope(UUID.randomUUID(), "THIT_GA", true, 1, 9L),
                mapper.readTree("{\"ingredientId\":\"THIT_GA\",\"needsCheck\":true,\"epoch\":1,\"seq\":9}"),
                EventProcessingContext.direct(EventTopics.MENU_EVENTS));
        assertThat(stale.outcome()).isEqualTo(Outcome.STALE_VERSION);

        var clearedTickets = orderService.listTickets(null);
        assertThat(clearedTickets).hasSize(1);
        assertThat(clearedTickets.get(0).warning()).isFalse();
        assertThat(clearedTickets.get(0).flaggedIngredients()).isEmpty();

        // Higher epoch (2, 1) re-flags cleanly
        var higherEpoch = orderService.processIngredientCheckChanged(
                ingredientCheckEnvelope(UUID.randomUUID(), "THIT_GA", true, 2, 1L),
                mapper.readTree("{\"ingredientId\":\"THIT_GA\",\"needsCheck\":true,\"epoch\":2,\"seq\":1}"),
                EventProcessingContext.direct(EventTopics.MENU_EVENTS));
        assertThat(higherEpoch.outcome()).isEqualTo(Outcome.APPLIED);
        assertThat(orderService.listTickets(null).get(0).warning()).isTrue();
    }

    @Test
    @Tag("kb-50")
    void rejectOnCookingTicketReturns409WithCurrentStatusAndEmitsNoRelease() throws Exception {
        UUID orderId = UUID.randomUUID();
        orderService.processOrderReserved(
                reservedEnvelope(UUID.randomUUID(), orderId, "POS01",
                        List.of(new OrderItemLine(1, "COM_GA", "Cơm gà", 1)),
                        List.of(new IngredientQuantity("THIT_GA", new BigDecimal("150.000")))),
                EventProcessingContext.direct(EventTopics.KITCHEN_ORDERS));

        // Complete before Start -> 409 with currentStatus=QUEUED
        var completeQueuedResp = postJson("/api/kitchen/tickets/" + orderId + "/complete", null);
        assertThat(completeQueuedResp.statusCode()).isEqualTo(409);
        assertThat(JsonPath.<String>read(completeQueuedResp.body(), "$.currentStatus")).isEqualTo("QUEUED");

        // Start -> COOKING
        assertThat(postJson("/api/kitchen/tickets/" + orderId + "/start", null).statusCode()).isEqualTo(200);

        // Start again while COOKING -> 409 with currentStatus=COOKING
        var startAgainResp = postJson("/api/kitchen/tickets/" + orderId + "/start", null);
        assertThat(startAgainResp.statusCode()).isEqualTo(409);
        assertThat(JsonPath.<String>read(startAgainResp.body(), "$.currentStatus")).isEqualTo("COOKING");

        // Reject while COOKING -> 409 with currentStatus=COOKING, no ReleaseRequested
        var rejectResp = postJson("/api/kitchen/tickets/" + orderId + "/reject",
                "{\"reasonCode\":\"EQUIPMENT_FAILURE\",\"spoiledIngredients\":[]}");
        assertThat(rejectResp.statusCode()).isEqualTo(409);
        assertThat(JsonPath.<String>read(rejectResp.body(), "$.currentStatus")).isEqualTo("COOKING");

        String status = jdbc.queryForObject(
                "SELECT status FROM " + SCHEMA + ".kitchen_orders WHERE order_id = ?", String.class, orderId);
        assertThat(status).isEqualTo("COOKING");
        assertThat(countOutbox(orderId, EventType.ReleaseRequested)).isZero();
        assertThat(countOutbox(orderId, EventType.OrderCancelled)).isZero();
    }

    @Test
    @Tag("kb-51")
    void raceBetweenRejectSpoiledAndCustomerCancelExactlyOneWinsAndEmitsOneRelease() throws Exception {
        int iterations = 50;
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            for (int i = 0; i < iterations; i++) {
                UUID orderId = UUID.randomUUID();
                orderService.processOrderReserved(
                        reservedEnvelope(UUID.randomUUID(), orderId, "POS01",
                                List.of(new OrderItemLine(1, "COM_GA", "Cơm gà", 1)),
                                List.of(new IngredientQuantity("GAO", new BigDecimal("100.000")),
                                        new IngredientQuantity("THIT_GA", new BigDecimal("150.000")))),
                        EventProcessingContext.direct(EventTopics.KITCHEN_ORDERS));

                CountDownLatch ready = new CountDownLatch(2);
                CountDownLatch go = new CountDownLatch(1);
                CountDownLatch done = new CountDownLatch(2);

                AtomicReference<KitchenTicketResponse> rejectRes = new AtomicReference<>();
                AtomicReference<Throwable> rejectErr = new AtomicReference<>();
                AtomicReference<Outcome> cancelOutcome = new AtomicReference<>();

                pool.submit(() -> {
                    ready.countDown();
                    try {
                        go.await();
                        rejectRes.set(orderService.rejectTicket(
                                orderId, new RejectTicketRequest("SPOILED_INGREDIENT", List.of("THIT_GA"))));
                    } catch (Throwable t) {
                        rejectErr.set(t);
                    } finally {
                        done.countDown();
                    }
                });

                pool.submit(() -> {
                    ready.countDown();
                    try {
                        go.await();
                        var res = orderService.processCancelRequested(
                                EventTopics.cancelEventsTopic("POS01"),
                                cancelEnvelope(UUID.randomUUID(), orderId, "POS01", ReasonCode.CUSTOMER_CHANGED_MIND),
                                EventProcessingContext.direct(EventTopics.cancelEventsTopic("POS01")));
                        cancelOutcome.set(res.outcome());
                    } catch (Throwable t) {
                        throw new RuntimeException(t);
                    } finally {
                        done.countDown();
                    }
                });

                assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
                go.countDown();
                assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();

                assertThat(countOutbox(orderId, EventType.ReleaseRequested)).isEqualTo(1L);

                ReleaseRequestedPayload release = readOutboxPayload(orderId, EventType.ReleaseRequested, ReleaseRequestedPayload.class);
                OrderCancelledPayload cancelled = readOutboxPayload(orderId, EventType.OrderCancelled, OrderCancelledPayload.class);

                if (rejectRes.get() != null) {
                    assertThat(rejectErr.get()).isNull();
                    assertThat(cancelOutcome.get()).isEqualTo(Outcome.STORED_RESULT);
                    assertThat(countOutbox(orderId, EventType.OrderCancelled)).isEqualTo(2L);
                    assertThat(release.cancelledBy()).isEqualTo(CancelledBy.KITCHEN);
                    assertThat(release.reasonCode()).isEqualTo(ReasonCode.SPOILED_INGREDIENT);
                    assertThat(release.spoiledIngredients()).containsExactly("THIT_GA");
                    assertThat(cancelled.cancelledBy()).isEqualTo(CancelledBy.KITCHEN);
                    assertThat(cancelled.reasonCode()).isEqualTo(ReasonCode.SPOILED_INGREDIENT);
                } else {
                    assertThat(rejectErr.get()).isInstanceOf(TicketConflictException.class);
                    assertThat(cancelOutcome.get()).isEqualTo(Outcome.APPLIED);
                    assertThat(countOutbox(orderId, EventType.OrderCancelled)).isEqualTo(1L);
                    assertThat(release.cancelledBy()).isEqualTo(CancelledBy.CUSTOMER);
                    assertThat(release.reasonCode()).isEqualTo(ReasonCode.CUSTOMER_CHANGED_MIND);
                    assertThat(release.spoiledIngredients()).isEmpty();
                    assertThat(cancelled.cancelledBy()).isEqualTo(CancelledBy.CUSTOMER);
                    assertThat(cancelled.reasonCode()).isEqualTo(ReasonCode.CUSTOMER_CHANGED_MIND);
                }
            }
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    @Tag("kb-52")
    void raceBetweenRejectAndStartCookingExactlyOneWinsAndEmitsConsumedXorRelease() throws Exception {
        int iterations = 50;
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            for (int i = 0; i < iterations; i++) {
                UUID orderId = UUID.randomUUID();
                orderService.processOrderReserved(
                        reservedEnvelope(UUID.randomUUID(), orderId, "POS01",
                                List.of(new OrderItemLine(1, "COM_GA", "Cơm gà", 1)),
                                List.of(new IngredientQuantity("THIT_GA", new BigDecimal("150.000")))),
                        EventProcessingContext.direct(EventTopics.KITCHEN_ORDERS));

                CountDownLatch ready = new CountDownLatch(2);
                CountDownLatch go = new CountDownLatch(1);
                CountDownLatch done = new CountDownLatch(2);

                AtomicReference<KitchenTicketResponse> rejectRes = new AtomicReference<>();
                AtomicReference<Throwable> rejectErr = new AtomicReference<>();
                AtomicReference<KitchenTicketResponse> startRes = new AtomicReference<>();
                AtomicReference<Throwable> startErr = new AtomicReference<>();

                pool.submit(() -> {
                    ready.countDown();
                    try {
                        go.await();
                        rejectRes.set(orderService.rejectTicket(
                                orderId, new RejectTicketRequest("EQUIPMENT_FAILURE", List.of())));
                    } catch (Throwable t) {
                        rejectErr.set(t);
                    } finally {
                        done.countDown();
                    }
                });

                pool.submit(() -> {
                    ready.countDown();
                    try {
                        go.await();
                        startRes.set(orderService.startTicket(orderId));
                    } catch (Throwable t) {
                        startErr.set(t);
                    } finally {
                        done.countDown();
                    }
                });

                assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
                go.countDown();
                assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();

                String dbStatus = jdbc.queryForObject(
                        "SELECT status FROM " + SCHEMA + ".kitchen_orders WHERE order_id = ?", String.class, orderId);

                if (startRes.get() != null) {
                    assertThat(startErr.get()).isNull();
                    assertThat(rejectRes.get()).isNull();
                    assertThat(rejectErr.get()).isInstanceOf(TicketConflictException.class);
                    assertThat(dbStatus).isEqualTo("COOKING");
                    assertThat(countOutbox(orderId, EventType.StockConsumed)).isEqualTo(1L);
                    assertThat(countOutbox(orderId, EventType.ReleaseRequested)).isZero();
                    assertThat(countOutbox(orderId, EventType.OrderCancelled)).isZero();
                } else {
                    assertThat(rejectRes.get()).isNotNull();
                    assertThat(rejectErr.get()).isNull();
                    assertThat(startErr.get()).isInstanceOf(TicketConflictException.class);
                    assertThat(dbStatus).isEqualTo("CANCELLED");
                    assertThat(countOutbox(orderId, EventType.ReleaseRequested)).isEqualTo(1L);
                    assertThat(countOutbox(orderId, EventType.OrderCancelled)).isEqualTo(1L);
                    assertThat(countOutbox(orderId, EventType.StockConsumed)).isZero();
                }
            }
        } finally {
            pool.shutdownNow();
        }
    }

    private EventEnvelope<OrderReservedPayload> reservedEnvelope(
            UUID eventId,
            UUID orderId,
            String originBranch,
            List<OrderItemLine> items,
            List<IngredientQuantity> reserved
    ) {
        return EventEnvelope.v1(
                eventId,
                EventType.OrderReserved,
                orderId,
                originBranch,
                Instant.now(),
                OrderReservedPayload.forKitchen(originBranch, Instant.now(), items, reserved));
    }

    private EventEnvelope<OrderFailedPayload> failedEnvelope(
            UUID eventId,
            UUID orderId,
            String originBranch,
            ReasonCode reasonCode
    ) {
        return EventEnvelope.v1(
                eventId,
                EventType.OrderFailed,
                orderId,
                originBranch,
                Instant.now(),
                new OrderFailedPayload(originBranch, reasonCode, reasonCode.defaultMessage()));
    }

    private EventEnvelope<CancelRequestedPayload> cancelEnvelope(
            UUID eventId,
            UUID orderId,
            String bodyBranchId,
            ReasonCode reasonCode
    ) {
        return EventEnvelope.v1(
                eventId,
                EventType.CancelRequested,
                orderId,
                bodyBranchId,
                Instant.now(),
                new CancelRequestedPayload(reasonCode));
    }

    private EventEnvelope<IngredientCheckChangedPayload> ingredientCheckEnvelope(
            UUID eventId,
            String ingredientId,
            boolean needsCheck,
            int epoch,
            long seq
    ) {
        return EventEnvelope.v1Menu(
                eventId,
                EventType.IngredientCheckChanged,
                "INVENTORY",
                Instant.now(),
                null,
                new IngredientCheckChangedPayload(ingredientId, needsCheck, epoch, seq));
    }

    private long countOutbox(UUID orderId, EventType type) {
        Long count = jdbc.queryForObject(
                "SELECT count(*) FROM " + SCHEMA + ".outbox_events WHERE aggregate_id = ? AND event_type = ?",
                Long.class, orderId, type.name());
        return count == null ? 0L : count;
    }

    private long countAllOutbox(UUID orderId) {
        Long count = jdbc.queryForObject(
                "SELECT count(*) FROM " + SCHEMA + ".outbox_events WHERE aggregate_id = ?",
                Long.class, orderId);
        return count == null ? 0L : count;
    }

    private List<String> outboxTopics(UUID orderId, EventType type) {
        return jdbc.queryForList(
                "SELECT topic FROM " + SCHEMA + ".outbox_events WHERE aggregate_id = ? AND event_type = ? ORDER BY seq_id",
                String.class, orderId, type.name());
    }

    private <T> T readOutboxPayload(UUID orderId, EventType type, Class<T> payloadClass) {
        String raw = jdbc.queryForObject(
                "SELECT payload::text FROM " + SCHEMA + ".outbox_events WHERE aggregate_id = ? AND event_type = ? ORDER BY seq_id LIMIT 1",
                String.class, orderId, type.name());
        EventEnvelope<T> env = mapper.readValue(raw,
                mapper.getTypeFactory().constructParametricType(EventEnvelope.class, payloadClass));
        return env.payload();
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
