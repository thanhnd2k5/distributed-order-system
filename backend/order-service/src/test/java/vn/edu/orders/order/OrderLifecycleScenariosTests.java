package vn.edu.orders.order;

import com.jayway.jsonpath.JsonPath;
import com.sun.net.httpserver.HttpServer;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import vn.edu.orders.contracts.events.CancelPendingPayload;
import vn.edu.orders.contracts.events.CancelRejectedPayload;
import vn.edu.orders.contracts.events.CancelledBy;
import vn.edu.orders.contracts.events.DishStatus;
import vn.edu.orders.contracts.events.DishStatusChangedPayload;
import vn.edu.orders.contracts.events.EventEnvelope;
import vn.edu.orders.contracts.events.EventType;
import vn.edu.orders.contracts.events.HeartbeatPayload;
import vn.edu.orders.contracts.events.KitchenStatus;
import vn.edu.orders.contracts.events.OrderCancelledPayload;
import vn.edu.orders.contracts.events.OrderCompletedPayload;
import vn.edu.orders.contracts.events.OrderReservedPayload;
import vn.edu.orders.contracts.events.ReasonCode;
import vn.edu.orders.order.config.MutableClock;
import vn.edu.orders.order.menu.application.MenuService;
import vn.edu.orders.order.orders.CancelOrderRequest;
import vn.edu.orders.order.orders.CreateOrderRequest;
import vn.edu.orders.order.orders.OrderService;
import vn.edu.orders.order.orders.ReplicationHeartbeatService;
import vn.edu.orders.order.outbox.KafkaAdminConnectivityProbe;
import vn.edu.orders.order.outbox.OutboxRelayService;
import vn.edu.orders.order.status.OrderStatusSyncService;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tier 1 and Tier 2 scenario tests against real PostgreSQL (no DB or HTTP mocks).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "outbox.enabled=false",
        "order.heartbeat-enabled=false",
        "spring.kafka.listener.auto-startup=false",
        "spring.jpa.properties.hibernate.default_schema=v15_order_scenarios_test",
        "spring.flyway.default-schema=v15_order_scenarios_test",
        "spring.flyway.schemas=v15_order_scenarios_test",
        "site.id=POS01",
        "site.source-type=POS"
})
class OrderLifecycleScenariosTests {

    private static final String SCHEMA = "v15_order_scenarios_test";
    private static volatile String snapshotJsonBody = """
            {"epoch":1,"seq":5,"dishes":[{"dishId":"COM_GA","dishName":"Cơm gà","status":"AVAILABLE","epoch":1,"seq":5}]}
            """;
    private static HttpServer snapshotServer;

    @DynamicPropertySource
    static void registerSnapshotServer(DynamicPropertyRegistry registry) throws Exception {
        snapshotServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        snapshotServer.createContext("/api/menu/snapshot", exchange -> {
            byte[] bytes = snapshotJsonBody.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        });
        snapshotServer.start();
        registry.add("order.inventory-base-url",
                () -> "http://127.0.0.1:" + snapshotServer.getAddress().getPort());
    }

    @AfterAll
    static void stopSnapshotServer() {
        if (snapshotServer != null) {
            snapshotServer.stop(0);
        }
    }

    @Value("${local.server.port}")
    private int port;

    @Autowired
    private JdbcTemplate jdbc;

    @Autowired
    private MutableClock clock;

    @Autowired
    private OrderService orderService;

    @Autowired
    private OutboxRelayService outboxRelay;

    @Autowired
    private OrderStatusSyncService statusSyncService;

    @Autowired
    private MenuService menuService;

    @Autowired
    private ReplicationHeartbeatService heartbeatService;

    @Autowired
    private KafkaAdminConnectivityProbe connectivityProbe;

    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

    @BeforeEach
    void resetState() {
        clock.reset();
        jdbc.update("DELETE FROM " + SCHEMA + ".alerts");
        jdbc.update("DELETE FROM " + SCHEMA + ".processed_events");
        jdbc.update("DELETE FROM " + SCHEMA + ".outbox_events");
        jdbc.update("DELETE FROM " + SCHEMA + ".order_items");
        jdbc.update("DELETE FROM " + SCHEMA + ".orders");
        jdbc.update("DELETE FROM " + SCHEMA + ".replication_heartbeat");
        jdbc.update("UPDATE " + SCHEMA + ".menu_cache_meta SET epoch = 1, last_heard_at = NULL WHERE id = TRUE");
        jdbc.update("DELETE FROM " + SCHEMA + ".menu_cache");
        jdbc.update("""
                INSERT INTO %s.menu_cache (dish_id, dish_name, status, epoch, seq)
                SELECT dish_id, name, 'AVAILABLE', 1, 0 FROM %s.menu_items WHERE active = TRUE
                """.formatted(SCHEMA, SCHEMA));
    }

    @AfterEach
    void restoreClock() {
        clock.reset();
    }

    @Test
    @Tag("kb-7")
    void cancelledArrivingBeforeReservedKeepsCancelledAndIgnoresReserved() throws Exception {
        var created = orderService.create(new CreateOrderRequest(
                UUID.randomUUID(), List.of(new CreateOrderRequest.Item("COM_GA", 1))));
        UUID orderId = created.order().orderId();

        var cancelled = EventEnvelope.v1(
                UUID.randomUUID(),
                EventType.OrderCancelled,
                orderId,
                "POS01",
                clock.instant(),
                new OrderCancelledPayload(CancelledBy.CUSTOMER, ReasonCode.CUSTOMER_CHANGED_MIND)
        );
        assertThat(statusSyncService.processOrderCancelled(cancelled).outcome()).isEqualTo("SUCCESS");

        var lateReserved = EventEnvelope.v1(
                UUID.randomUUID(),
                EventType.OrderReserved,
                orderId,
                "POS01",
                clock.instant(),
                OrderReservedPayload.forBranch()
        );
        assertThat(statusSyncService.processOrderReserved(lateReserved).outcome()).isEqualTo("TERMINAL_STATE_IGNORED");

        var detail = get("/api/orders/" + orderId);
        assertThat(detail.statusCode()).isEqualTo(200);
        assertThat(JsonPath.<String>read(detail.body(), "$.order.status")).isEqualTo("CANCELLED");
        assertThat(JsonPath.<Integer>read(detail.body(), "$.order.statusRank")).isEqualTo(2);
        assertThat(JsonPath.<String>read(detail.body(), "$.order.closeReason")).isEqualTo("CUSTOMER_CHANGED_MIND");
        assertThat(JsonPath.<String>read(detail.body(), "$.order.cancelFlag")).isEqualTo("NONE");
    }

    @Test
    @Tag("kb-19")
    void soldOutV6IsNotOverwrittenByEarlierSnapshotV5() throws Exception {
        var soldOutV6 = EventEnvelope.v1Menu(
                UUID.randomUUID(),
                EventType.DishStatusChanged,
                "INVENTORY",
                clock.instant(),
                null,
                new DishStatusChangedPayload("COM_GA", DishStatus.SOLD_OUT, 1, 6L)
        );
        menuService.onDishStatusChanged(soldOutV6);

        snapshotJsonBody = """
                {
                  "epoch": 1,
                  "seq": 5,
                  "dishes": [
                    { "dishId": "COM_GA", "dishName": "Cơm gà", "status": "AVAILABLE", "epoch": 1, "seq": 5 }
                  ]
                }
                """;
        menuService.syncSnapshot();

        var menuResp = get("/api/menu");
        assertThat(menuResp.statusCode()).isEqualTo(200);
        List<String> comGaStatuses = JsonPath.read(menuResp.body(), "$.dishes[?(@.dishId=='COM_GA')].status");
        assertThat(comGaStatuses).containsExactly("SOLD_OUT");
        List<Integer> comGaSeqs = JsonPath.read(menuResp.body(), "$.dishes[?(@.dishId=='COM_GA')].seq");
        assertThat(comGaSeqs).containsExactly(6);
    }

    @Test
    @Tag("kb-21")
    void regularHeartbeatsOverTenMinutesKeepMenuFreshAndAbsenceMakesItStale() throws Exception {
        Instant t0 = Instant.parse("2026-11-22T08:00:00Z");
        clock.setFixed(t0);

        for (int minute = 0; minute <= 10; minute++) {
            clock.setFixed(t0.plus(Duration.ofMinutes(minute)));
            menuService.onHeartbeat(EventEnvelope.v1Menu(
                    UUID.randomUUID(), EventType.Heartbeat, "INVENTORY", clock.instant(), null,
                    new HeartbeatPayload(1, minute + 1L)));

            var menuResp = get("/api/menu");
            assertThat(menuResp.statusCode()).isEqualTo(200);
            assertThat(JsonPath.<Boolean>read(menuResp.body(), "$.stale")).isFalse();
        }

        // Advance past N=5m threshold without heartbeats -> stale becomes true
        clock.advance(Duration.ofMinutes(6));
        var staleResp = get("/api/menu");
        assertThat(staleResp.statusCode()).isEqualTo(200);
        assertThat(JsonPath.<Boolean>read(staleResp.body(), "$.stale")).isTrue();
    }

    @Test
    @Tag("kb-36")
    void attemptedOrderCancelledWhileKitchenAlreadyCookingShowsCannotCancelAndCompletes() throws Exception {
        // Case A: CancelRejected arrives before OrderCompleted
        UUID orderA = orderService.create(new CreateOrderRequest(
                UUID.randomUUID(), List.of(new CreateOrderRequest.Item("COM_GA", 1)))).order().orderId();
        jdbc.update("UPDATE " + SCHEMA + ".outbox_events SET attempted = TRUE, sent_at = now() WHERE order_id = ?", orderA);
        statusSyncService.processOrderReserved(EventEnvelope.v1(
                UUID.randomUUID(), EventType.OrderReserved, orderA, "POS01", clock.instant(),
                OrderReservedPayload.forBranch()));

        var cancelA = post("/api/orders/" + orderA + "/cancel", """
                {"reasonCode":"CUSTOMER_CHANGED_MIND"}
                """);
        assertThat(cancelA.statusCode()).isEqualTo(202);
        assertThat(JsonPath.<String>read(cancelA.body(), "$.cancelFlag")).isEqualTo("REQUESTED");

        statusSyncService.processCancelRejected(EventEnvelope.v1(
                UUID.randomUUID(), EventType.CancelRejected, orderA, "POS01", clock.instant(),
                new CancelRejectedPayload(KitchenStatus.COOKING, "Cannot cancel: the kitchen has started cooking")));

        var afterRejectA = get("/api/orders/" + orderA);
        assertThat(JsonPath.<String>read(afterRejectA.body(), "$.order.status")).isEqualTo("RESERVED");
        assertThat(JsonPath.<String>read(afterRejectA.body(), "$.order.cancelFlag")).isEqualTo("NONE");
        assertThat(JsonPath.<String>read(afterRejectA.body(), "$.order.cancelMessage"))
                .isEqualTo("Cannot cancel: the kitchen has started cooking");

        statusSyncService.processOrderCompleted(EventEnvelope.v1(
                UUID.randomUUID(), EventType.OrderCompleted, orderA, "POS01", clock.instant(),
                OrderCompletedPayload.empty()));

        var afterDoneA = get("/api/orders/" + orderA);
        assertThat(JsonPath.<String>read(afterDoneA.body(), "$.order.status")).isEqualTo("DONE");
        assertThat(JsonPath.<String>read(afterDoneA.body(), "$.order.cancelMessage"))
                .isEqualTo("Cannot cancel: the kitchen has started cooking");

        // Case B: OrderCompleted arrives before CancelRejected (Conditional Write 5 runs at any rank)
        UUID orderB = orderService.create(new CreateOrderRequest(
                UUID.randomUUID(), List.of(new CreateOrderRequest.Item("CHAO_GA", 1)))).order().orderId();
        jdbc.update("UPDATE " + SCHEMA + ".outbox_events SET attempted = TRUE, sent_at = now() WHERE order_id = ?", orderB);
        statusSyncService.processOrderReserved(EventEnvelope.v1(
                UUID.randomUUID(), EventType.OrderReserved, orderB, "POS01", clock.instant(),
                OrderReservedPayload.forBranch()));
        assertThat(post("/api/orders/" + orderB + "/cancel", """
                {"reasonCode":"CUSTOMER_WAITED_TOO_LONG"}
                """).statusCode()).isEqualTo(202);

        statusSyncService.processOrderCompleted(EventEnvelope.v1(
                UUID.randomUUID(), EventType.OrderCompleted, orderB, "POS01", clock.instant(),
                OrderCompletedPayload.empty()));
        statusSyncService.processCancelRejected(EventEnvelope.v1(
                UUID.randomUUID(), EventType.CancelRejected, orderB, "POS01", clock.instant(),
                new CancelRejectedPayload(KitchenStatus.DONE, "Cannot cancel: the kitchen has started cooking")));

        var afterDoneB = get("/api/orders/" + orderB);
        assertThat(JsonPath.<String>read(afterDoneB.body(), "$.order.status")).isEqualTo("DONE");
        assertThat(JsonPath.<String>read(afterDoneB.body(), "$.order.cancelFlag")).isEqualTo("NONE");
        assertThat(JsonPath.<String>read(afterDoneB.body(), "$.order.cancelMessage"))
                .isEqualTo("Cannot cancel: the kitchen has started cooking");
    }

    @Test
    @Tag("kb-37")
    void twoHundredIterationConcurrentRaceBetweenSenderAttemptedFlagAndLocalCancel() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(2);
        AtomicInteger localCancelWins = new AtomicInteger();
        AtomicInteger senderFlagWins = new AtomicInteger();

        try {
            for (int i = 0; i < 200; i++) {
                UUID orderId = orderService.create(new CreateOrderRequest(
                        UUID.randomUUID(), List.of(new CreateOrderRequest.Item("COM_GA", 1)))).order().orderId();
                long outboxId = jdbc.queryForObject(
                        "SELECT id FROM " + SCHEMA + ".outbox_events WHERE order_id = ? AND kind = 'CREATE_ORDER'",
                        Long.class, orderId);

                CyclicBarrier barrier = new CyclicBarrier(2);
                Callable<Boolean> senderTask = () -> {
                    barrier.await(5, TimeUnit.SECONDS);
                    return outboxRelay.claimForSend(outboxId).isPresent();
                };
                Callable<OrderService.CancelResult> cancelTask = () -> {
                    barrier.await(5, TimeUnit.SECONDS);
                    return orderService.cancel(orderId, new CancelOrderRequest(ReasonCode.CUSTOMER_CHANGED_MIND));
                };

                Future<Boolean> senderFuture = pool.submit(senderTask);
                Future<OrderService.CancelResult> cancelFuture = pool.submit(cancelTask);

                boolean senderClaimed = senderFuture.get(10, TimeUnit.SECONDS);
                OrderService.CancelResult cancelResult = cancelFuture.get(10, TimeUnit.SECONDS);

                if (cancelResult.localCancelled()) {
                    assertThat(senderClaimed).isFalse();
                    assertThat(cancelResult.httpStatus()).isEqualTo(200);
                    assertThat(cancelResult.order().status()).isEqualTo("CANCELLED");
                    assertThat(cancelResult.order().statusRank()).isEqualTo(2);
                    assertThat(jdbc.queryForObject(
                            "SELECT count(*) FROM " + SCHEMA + ".outbox_events WHERE order_id = ?",
                            Long.class, orderId)).isZero();
                    localCancelWins.incrementAndGet();
                } else {
                    assertThat(senderClaimed).isTrue();
                    assertThat(cancelResult.httpStatus()).isEqualTo(202);
                    assertThat(cancelResult.order().status()).isEqualTo("PENDING_INVENTORY");
                    assertThat(cancelResult.order().cancelFlag()).isEqualTo("REQUESTED");
                    assertThat(jdbc.queryForObject(
                            "SELECT count(*) FROM " + SCHEMA + ".outbox_events WHERE order_id = ? AND kind = 'CREATE_ORDER' AND attempted = TRUE",
                            Long.class, orderId)).isEqualTo(1L);
                    assertThat(jdbc.queryForObject(
                            "SELECT count(*) FROM " + SCHEMA + ".outbox_events WHERE order_id = ? AND kind = 'CANCEL_REQUEST'",
                            Long.class, orderId)).isEqualTo(1L);
                    senderFlagWins.incrementAndGet();
                }
            }
        } finally {
            pool.shutdownNow();
        }

        assertThat(localCancelWins.get() + senderFlagWins.get()).isEqualTo(200);
    }

    @Test
    @Tag("kb-53")
    void pendingInventoryOlderThanNMinutesRaisesMonitorAlertWithoutAutoCancelling() throws Exception {
        Instant t0 = Instant.parse("2026-11-22T09:00:00Z");
        clock.setFixed(t0);

        UUID orderId = orderService.create(new CreateOrderRequest(
                UUID.randomUUID(), List.of(new CreateOrderRequest.Item("COM_GA", 1)))).order().orderId();

        // At 9 minutes, no alert yet
        clock.setFixed(t0.plus(Duration.ofMinutes(9)));
        var beforeResp = get("/api/monitor");
        assertThat(beforeResp.statusCode()).isEqualTo(200);
        assertThat(JsonPath.<List<String>>read(beforeResp.body(), "$.pendingInventoryAlerts[*].orderId")).isEmpty();

        // At 11 minutes (> N=10m), alert appears and order stays PENDING_INVENTORY
        clock.setFixed(t0.plus(Duration.ofMinutes(11)));
        var afterResp = get("/api/monitor");
        assertThat(afterResp.statusCode()).isEqualTo(200);
        assertThat(JsonPath.<List<String>>read(afterResp.body(), "$.pendingInventoryAlerts[*].orderId"))
                .containsExactly(orderId.toString());
        assertThat(JsonPath.<List<Number>>read(afterResp.body(), "$.pendingInventoryAlerts[*].waitingSeconds")
                .getFirst().longValue()).isGreaterThanOrEqualTo(660L);

        var orderResp = get("/api/orders/" + orderId);
        assertThat(JsonPath.<String>read(orderResp.body(), "$.order.status")).isEqualTo("PENDING_INVENTORY");

        // Operator can request cancellation from the alert
        assertThat(post("/api/orders/" + orderId + "/cancel", """
                {"reasonCode":"CUSTOMER_WAITED_TOO_LONG"}
                """).statusCode()).isEqualTo(200);
    }

    @Test
    @Tag("kb-54")
    void offlineUnsentOrderWithinOneHourOfExpiryShowsConnectionLostPendingCountAndExpiryAlert() throws Exception {
        Instant t0 = Instant.parse("2026-11-22T06:00:00Z");
        clock.setFixed(t0);

        // Simulate real probe failure against an unreachable Kafka port
        var offlineProbe = new KafkaAdminConnectivityProbe("127.0.0.1:19999",
                new vn.edu.orders.order.config.OrderLifecycleProperties(
                        null, null, null, null, 5, 300L, 1000L, null, 5000L, false, null),
                clock);
        try {
            assertThat(offlineProbe.probe()).isFalse();
            connectivityProbe.recordFailure(t0, offlineProbe.latestStatus().lastError());
        } finally {
            offlineProbe.shutdown();
        }

        UUID orderId = orderService.create(new CreateOrderRequest(
                UUID.randomUUID(), List.of(new CreateOrderRequest.Item("PHO_BO", 1)))).order().orderId();

        // Advance 3h15m (45 minutes before X=4h limit)
        clock.setFixed(t0.plus(Duration.ofHours(3)).plus(Duration.ofMinutes(15)));

        var monitorResp = get("/api/monitor");
        assertThat(monitorResp.statusCode()).isEqualTo(200);
        assertThat(JsonPath.<Boolean>read(monitorResp.body(), "$.kafkaConnected")).isFalse();
        assertThat(JsonPath.<Integer>read(monitorResp.body(), "$.pendingOutboxCount")).isEqualTo(1);
        assertThat(JsonPath.<List<String>>read(monitorResp.body(), "$.expiringUnsentAlerts[*].orderId"))
                .containsExactly(orderId.toString());
        assertThat(JsonPath.<List<Number>>read(monitorResp.body(), "$.expiringUnsentAlerts[*].remainingSeconds")
                .getFirst().longValue()).isEqualTo(2700L);
    }

    @Test
    void replicationHeartbeatUpsertsSingleBranchRowAndCancelPendingRaceUpdatesCorrectly() {
        Instant t1 = Instant.parse("2026-11-22T10:00:00Z");
        clock.setFixed(t1);
        heartbeatService.beat();
        Instant t2 = t1.plusSeconds(5);
        clock.setFixed(t2);
        heartbeatService.beat();

        assertThat(jdbc.queryForObject(
                "SELECT count(*) FROM " + SCHEMA + ".replication_heartbeat WHERE branch_id = 'POS01'",
                Long.class)).isEqualTo(1L);

        UUID orderId = orderService.create(new CreateOrderRequest(
                UUID.randomUUID(), List.of(new CreateOrderRequest.Item("COM_GA", 1)))).order().orderId();
        jdbc.update("UPDATE " + SCHEMA + ".outbox_events SET attempted = TRUE WHERE order_id = ?", orderId);
        orderService.cancel(orderId, new CancelOrderRequest(ReasonCode.WRONG_ORDER_ENTERED));
        statusSyncService.processCancelPending(EventEnvelope.v1(
                UUID.randomUUID(), EventType.CancelPending, orderId, "POS01", clock.instant(),
                CancelPendingPayload.empty()));

        assertThat(jdbc.queryForObject(
                "SELECT cancel_flag FROM " + SCHEMA + ".orders WHERE order_id = ?",
                String.class, orderId)).isEqualTo("KITCHEN_WILL_NOT_COOK");
    }

    private HttpResponse<String> get(String path) throws Exception {
        var req = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                .timeout(Duration.ofSeconds(10)).GET().build();
        return http.send(req, HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> post(String path, String json) throws Exception {
        var req = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                .header("Content-Type", "application/json")
                .timeout(Duration.ofSeconds(10))
                .POST(HttpRequest.BodyPublishers.ofString(json)).build();
        return http.send(req, HttpResponse.BodyHandlers.ofString());
    }
}
