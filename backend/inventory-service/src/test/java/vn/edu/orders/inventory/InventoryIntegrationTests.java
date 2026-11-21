package vn.edu.orders.inventory;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.jayway.jsonpath.JsonPath;
import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import vn.edu.orders.contracts.events.AlertKind;
import vn.edu.orders.contracts.events.CancelledBy;
import vn.edu.orders.contracts.events.DishStatus;
import vn.edu.orders.contracts.events.DishStatusChangedPayload;
import vn.edu.orders.contracts.events.EventEnvelope;
import vn.edu.orders.contracts.events.EventTopics;
import vn.edu.orders.contracts.events.EventType;
import vn.edu.orders.contracts.events.HeartbeatPayload;
import vn.edu.orders.contracts.events.IngredientCheckChangedPayload;
import vn.edu.orders.contracts.events.InventoryStatus;
import vn.edu.orders.contracts.events.MenuVersion;
import vn.edu.orders.contracts.events.OrderCreatedPayload;
import vn.edu.orders.contracts.events.OrderFailedPayload;
import vn.edu.orders.contracts.events.OrderItemLine;
import vn.edu.orders.contracts.events.OrderReservedPayload;
import vn.edu.orders.contracts.events.ReasonCode;
import vn.edu.orders.contracts.events.ReleaseRequestedPayload;
import vn.edu.orders.contracts.events.SourceType;
import vn.edu.orders.contracts.events.StockConsumedPayload;
import vn.edu.orders.inventory.InventoryTestSupport.MutableClock;
import vn.edu.orders.inventory.application.DishStatusService;
import vn.edu.orders.inventory.application.EventProcessingContext;
import vn.edu.orders.inventory.application.InventoryOrderService;
import vn.edu.orders.inventory.application.InventoryOrderService.OrderProcessingResult;
import vn.edu.orders.inventory.application.InventoryOrderService.Outcome;
import vn.edu.orders.inventory.application.StockAdminService;
import vn.edu.orders.inventory.application.StockCommandService;
import vn.edu.orders.inventory.application.StockCommandService.CommandResult;
import vn.edu.orders.inventory.config.InventoryProperties;
import vn.edu.orders.inventory.infrastructure.InventoryStore;

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
class InventoryIntegrationTests {
    private static final String SCHEMA = "inventory_test";

    @Value("${local.server.port}")
    private int port;

    @Autowired
    private InventoryOrderService orderService;

    @Autowired
    private StockCommandService commandService;

    @Autowired
    private StockAdminService adminService;

    @Autowired
    private DishStatusService dishStatusService;

    @Autowired
    private InventoryStore inventoryStore;

    @Autowired
    private MutableClock clock;

    @Autowired
    private JdbcTemplate jdbc;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Autowired
    private ObjectMapper mapper;

    private final AtomicLong syntheticOffset = new AtomicLong(1);
    private final HttpClient http = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    @BeforeEach
    void setup() {
        clock.setInstant(Instant.parse("2026-11-22T10:00:00Z"));
        InventoryTestSupport.resetSchema(jdbc, SCHEMA);
    }

    @AfterEach
    void verifyInvariants() {
        InventoryTestSupport.assertInventoryInvariants(jdbc, SCHEMA);
    }

    @Test
    @Tag("kb-5")
    void releaseCommandThreeTimesIncreasesStockExactlyOnce() {
        UUID orderId = UUID.randomUUID();
        var created = orderCreated(UUID.randomUUID(), orderId, "POS01", SourceType.POS, clock.instant(),
                List.of(new OrderItemLine(1, "COM_GA", "Cơm gà", 2)));
        assertThat(orderService.processOrderCreated(created, orderCtx("POS01")).status())
                .isEqualTo(InventoryStatus.HELD);

        assertThat(stock("GAO").available()).isEqualByComparingTo("800");
        assertThat(stock("GAO").reserved()).isEqualByComparingTo("200");
        assertThat(stock("THIT_GA").available()).isEqualByComparingTo("1200");
        assertThat(stock("THIT_GA").reserved()).isEqualByComparingTo("300");

        UUID releaseEvent1 = UUID.randomUUID();
        var release1 = releaseRequested(releaseEvent1, orderId, CancelledBy.CUSTOMER, ReasonCode.CUSTOMER_CHANGED_MIND, List.of());

        // Same event_id delivered twice, then a new event_id for the same orderId (3 total arrivals)
        CommandResult r1 = commandService.processReleaseRequested(release1, cmdCtx());
        CommandResult r2 = commandService.processReleaseRequested(release1, cmdCtx());
        var release2 = releaseRequested(UUID.randomUUID(), orderId, CancelledBy.CUSTOMER, ReasonCode.CUSTOMER_CHANGED_MIND, List.of());
        CommandResult r3 = commandService.processReleaseRequested(release2, cmdCtx());

        assertThat(r1.outcome()).isEqualTo(StockCommandService.Outcome.APPLIED);
        assertThat(r2.outcome()).isEqualTo(StockCommandService.Outcome.DUPLICATE_EVENT);
        assertThat(r3.outcome()).isEqualTo(StockCommandService.Outcome.IGNORED_ALREADY_FINALIZED);

        assertThat(stock("GAO").available()).isEqualByComparingTo("1000");
        assertThat(stock("GAO").reserved()).isEqualByComparingTo("0");
        assertThat(stock("THIT_GA").available()).isEqualByComparingTo("1500");
        assertThat(stock("THIT_GA").reserved()).isEqualByComparingTo("0");
        assertThat(count("SELECT count(*) FROM inventory_test.alerts")).isZero();
    }

    @Test
    @Tag("kb-6")
    void spoiledIngredientWritesOffWasteFlagsCheckReleasesOtherLinesAndUpdatesDishes() throws Exception {
        UUID orderId = UUID.randomUUID();
        var created = orderCreated(UUID.randomUUID(), orderId, "POS01", SourceType.POS, clock.instant(),
                List.of(new OrderItemLine(1, "COM_GA", "Cơm gà", 1)));
        assertThat(orderService.processOrderCreated(created, orderCtx("POS01")).status())
                .isEqualTo(InventoryStatus.HELD);

        UUID releaseEventId = UUID.randomUUID();
        var spoiled = releaseRequested(
                releaseEventId, orderId, CancelledBy.KITCHEN, ReasonCode.SPOILED_INGREDIENT, List.of("THIT_GA"));
        CommandResult res = commandService.processReleaseRequested(spoiled, cmdCtx());
        assertThat(res.outcome()).isEqualTo(StockCommandService.Outcome.APPLIED);
        assertThat(res.orderStatus()).isEqualTo(InventoryStatus.RELEASED);

        // THIT_GA wasted=150, needs_check=true, available stays 1350; GAO released back to 1000
        StockRowView thitGa = stock("THIT_GA");
        assertThat(thitGa.available()).isEqualByComparingTo("1350");
        assertThat(thitGa.reserved()).isEqualByComparingTo("0");
        assertThat(thitGa.wasted()).isEqualByComparingTo("150");
        assertThat(thitGa.needsCheck()).isTrue();

        StockRowView gao = stock("GAO");
        assertThat(gao.available()).isEqualByComparingTo("1000");
        assertThat(gao.reserved()).isEqualByComparingTo("0");
        assertThat(gao.wasted()).isEqualByComparingTo("0");
        assertThat(gao.needsCheck()).isFalse();

        // COM_GA and CHAO_GA use THIT_GA -> SOLD_OUT; PHO_BO stays AVAILABLE
        assertThat(dishStatus("COM_GA")).isEqualTo(DishStatus.SOLD_OUT);
        assertThat(dishStatus("CHAO_GA")).isEqualTo(DishStatus.SOLD_OUT);
        assertThat(dishStatus("PHO_BO")).isEqualTo(DishStatus.AVAILABLE);

        // Outbox on menu-events contains DishStatusChanged for CHAO_GA & COM_GA + IngredientCheckChanged(THIT_GA, true)
        List<String> menuJsons = jdbc.queryForList("""
                SELECT payload::text FROM inventory_test.outbox_events
                WHERE topic = ? ORDER BY id ASC
                """, String.class, EventTopics.MENU_EVENTS);
        List<String> soldOutDishes = new ArrayList<>();
        boolean foundCheckChanged = false;
        for (String raw : menuJsons) {
            JsonNode node = mapper.readTree(raw);
            String type = node.get("eventType").asString();
            if ("DishStatusChanged".equals(type)) {
                var payload = mapper.treeToValue(node.get("payload"), DishStatusChangedPayload.class);
                assertThat(payload.status()).isEqualTo(DishStatus.SOLD_OUT);
                soldOutDishes.add(payload.dishId());
            } else if ("IngredientCheckChanged".equals(type)) {
                var payload = mapper.treeToValue(node.get("payload"), IngredientCheckChangedPayload.class);
                assertThat(payload.ingredientId()).isEqualTo("THIT_GA");
                assertThat(payload.needsCheck()).isTrue();
                foundCheckChanged = true;
            }
        }
        assertThat(soldOutDishes).containsExactlyInAnyOrder("CHAO_GA", "COM_GA");
        assertThat(foundCheckChanged).isTrue();
    }

    @Test
    @Tag("kb-9")
    void failedOrderResentAfterStockReceiptStillReturnsFailedAndReservesNothing() throws Exception {
        jdbc.update("UPDATE inventory_test.stock SET available = 50 WHERE ingredient_id = 'GAO'");
        UUID orderId = UUID.randomUUID();
        UUID event1 = UUID.randomUUID();
        var created1 = orderCreated(event1, orderId, "POS01", SourceType.POS, clock.instant(),
                List.of(new OrderItemLine(1, "COM_GA", "Cơm gà", 1)));

        OrderProcessingResult first = orderService.processOrderCreated(created1, orderCtx("POS01"));
        assertThat(first.status()).isEqualTo(InventoryStatus.FAILED);
        assertThat(first.reasonCode()).isEqualTo(ReasonCode.INSUFFICIENT_INGREDIENTS);

        // Receive more stock via REST endpoint
        var receipt = postJson("/api/stock/receipts", """
                { "ingredientId": "GAO", "quantity": 500 }
                """);
        assertThat(receipt.statusCode()).isEqualTo(200);
        assertThat(stock("GAO").available()).isEqualByComparingTo("550");

        // Resend same event_id -> ignored, nothing emitted
        OrderProcessingResult dupEvent = orderService.processOrderCreated(created1, orderCtx("POS01"));
        assertThat(dupEvent.outcome()).isEqualTo(Outcome.DUPLICATE_EVENT);
        assertThat(count("SELECT count(*) FROM inventory_test.outbox_events")).isEqualTo(2L);

        // Resend same orderId with NEW event_id -> returns FAILED ONLY to status-events.POS01, never kitchen-orders!
        UUID event2 = UUID.randomUUID();
        var created2 = orderCreated(event2, orderId, "POS01", SourceType.POS, clock.instant(),
                List.of(new OrderItemLine(1, "COM_GA", "Cơm gà", 1)));
        OrderProcessingResult resend = orderService.processOrderCreated(created2, orderCtx("POS01"));

        assertThat(resend.outcome()).isEqualTo(Outcome.DUPLICATE_ORDER);
        assertThat(resend.status()).isEqualTo(InventoryStatus.FAILED);
        assertThat(resend.reasonCode()).isEqualTo(ReasonCode.INSUFFICIENT_INGREDIENTS);

        // Stock untouched (reserves nothing)
        assertThat(stock("GAO").available()).isEqualByComparingTo("550");
        assertThat(stock("GAO").reserved()).isEqualByComparingTo("0");
        assertThat(stock("THIT_GA").available()).isEqualByComparingTo("1500");
        assertThat(stock("THIT_GA").reserved()).isEqualByComparingTo("0");

        // Only 1 new outbox row, to status-events.POS01, zero new rows to kitchen-orders
        assertThat(count("SELECT count(*) FROM inventory_test.outbox_events WHERE topic = 'status-events.POS01'"))
                .isEqualTo(2L);
        assertThat(count("SELECT count(*) FROM inventory_test.outbox_events WHERE topic = 'kitchen-orders'"))
                .isEqualTo(1L);
    }

    @Test
    @Tag("kb-15")
    void expiredOrderReturnsOrderExpiredAndReservesNothing() {
        Instant now = clock.instant();
        // 4h01m ago -> expired (X = 4h)
        UUID expiredOrderId = UUID.randomUUID();
        var expiredEvent = orderCreated(UUID.randomUUID(), expiredOrderId, "POS01", SourceType.POS,
                now.minus(Duration.ofHours(4).plusMinutes(1)),
                List.of(new OrderItemLine(1, "COM_GA", "Cơm gà", 1)));

        OrderProcessingResult expiredRes = orderService.processOrderCreated(expiredEvent, orderCtx("POS01"));
        assertThat(expiredRes.status()).isEqualTo(InventoryStatus.FAILED);
        assertThat(expiredRes.reasonCode()).isEqualTo(ReasonCode.ORDER_EXPIRED);
        assertThat(stock("GAO").available()).isEqualByComparingTo("1000");
        assertThat(stock("THIT_GA").available()).isEqualByComparingTo("1500");
        assertThat(count("SELECT count(*) FROM inventory_test.outbox_events WHERE aggregate_id = ? AND event_type = 'OrderFailed'",
                expiredOrderId)).isEqualTo(2L);

        // Exactly at X (4h00m00s ago) is accepted
        UUID boundaryOrderId = UUID.randomUUID();
        var boundaryEvent = orderCreated(UUID.randomUUID(), boundaryOrderId, "POS01", SourceType.POS,
                now.minus(Duration.ofHours(4)),
                List.of(new OrderItemLine(1, "COM_GA", "Cơm gà", 1)));
        assertThat(orderService.processOrderCreated(boundaryEvent, orderCtx("POS01")).status())
                .isEqualTo(InventoryStatus.HELD);
    }

    @Test
    @Tag("kb-16")
    void futureCreationTimeBeyondToleranceFailsWithoutReserving() {
        Instant now = clock.instant();
        // 1 hour in future -> FAILED(INVALID_CREATION_TIME)
        UUID futureOrderId = UUID.randomUUID();
        var futureEvent = orderCreated(UUID.randomUUID(), futureOrderId, "ONLINE", SourceType.ONLINE,
                now.plus(Duration.ofHours(1)),
                List.of(new OrderItemLine(1, "COM_GA", "Cơm gà", 1)));

        OrderProcessingResult futureRes = orderService.processOrderCreated(futureEvent, orderCtx("ONLINE"));
        assertThat(futureRes.status()).isEqualTo(InventoryStatus.FAILED);
        assertThat(futureRes.reasonCode()).isEqualTo(ReasonCode.INVALID_CREATION_TIME);
        assertThat(stock("GAO").available()).isEqualByComparingTo("1000");
        assertThat(count("SELECT count(*) FROM inventory_test.outbox_events WHERE aggregate_id = ? AND topic = 'status-events.ONLINE'",
                futureOrderId)).isEqualTo(1L);
        assertThat(count("SELECT count(*) FROM inventory_test.outbox_events WHERE aggregate_id = ? AND topic = 'kitchen-orders'",
                futureOrderId)).isEqualTo(1L);

        // 4 minutes in future (within 5m tolerance) -> HELD
        UUID toleratedId = UUID.randomUUID();
        var toleratedEvent = orderCreated(UUID.randomUUID(), toleratedId, "ONLINE", SourceType.ONLINE,
                now.plus(Duration.ofMinutes(4)),
                List.of(new OrderItemLine(1, "COM_GA", "Cơm gà", 1)));
        assertThat(orderService.processOrderCreated(toleratedEvent, orderCtx("ONLINE")).status())
                .isEqualTo(InventoryStatus.HELD);
    }

    @Test
    @Tag("kb-18")
    void versionPersistsAcrossRestartEpochBumpRewritesAllAndSnapshotIncludesHidden() throws Exception {
        // 1. Reduce GAO so COM_GA becomes SOLD_OUT (CHAO_GA still has 50 GAO, so AVAILABLE)
        jdbc.update("UPDATE inventory_test.stock SET available = 80 WHERE ingredient_id = 'GAO'");
        // Trigger recomputation via receipt of 1 unit -> GAO = 81 (< 100 for COM_GA, >= 50 for CHAO_GA)
        assertThat(postJson("/api/stock/receipts", """
                { "ingredientId": "GAO", "quantity": 1 }
                """).statusCode()).isEqualTo(200);
        assertThat(dishStatus("COM_GA")).isEqualTo(DishStatus.SOLD_OUT);
        long seqAfterSoldOut = jdbc.queryForObject(
                "SELECT seq FROM inventory_test.dish_status WHERE dish_id = 'COM_GA'", Long.class);
        assertThat(seqAfterSoldOut).isGreaterThan(1L);

        // 2. Simulate Inventory restart: instantiate fresh DishStatusService & StockAdminService (no RAM state)
        var restartedDishService = new DishStatusService(
                inventoryStore, transactionManager, mapper, clock,
                new InventoryProperties("inventory-restarted", Duration.ofHours(4), Duration.ofMinutes(5), Duration.ofMinutes(5), 5, 50));
        var restartedAdmin = new StockAdminService(
                inventoryStore, restartedDishService, transactionManager, clock,
                new InventoryProperties("inventory-restarted", Duration.ofHours(4), Duration.ofMinutes(5), Duration.ofMinutes(5), 5, 50));
        restartedAdmin.receiveStock("GAO", InventoryTestSupport.bd("50")); // GAO=131 -> COM_GA returns AVAILABLE
        long seqAfterRestart = jdbc.queryForObject(
                "SELECT seq FROM inventory_test.dish_status WHERE dish_id = 'COM_GA'", Long.class);
        assertThat(seqAfterRestart).isGreaterThan(seqAfterSoldOut);
        assertThat(dishStatus("COM_GA")).isEqualTo(DishStatus.AVAILABLE);

        // 3. Delete PHO_BO recipe -> dish moves to HIDDEN and snapshot includes HIDDEN
        adminService.deleteRecipe("PHO_BO");
        assertThat(dishStatus("PHO_BO")).isEqualTo(DishStatus.HIDDEN);

        var snapshotBeforeBump = get("/api/menu/snapshot");
        assertThat(snapshotBeforeBump.statusCode()).isEqualTo(200);
        assertThat(JsonPath.<List<String>>read(snapshotBeforeBump.body(), "$.dishes[*].dishId"))
                .containsExactly("CHAO_GA", "COM_GA", "PHO_BO");
        assertThat(JsonPath.<List<String>>read(snapshotBeforeBump.body(), "$.dishes[*].status"))
                .containsExactly("AVAILABLE", "AVAILABLE", "HIDDEN");

        // 4. Epoch bump: rewrites ALL dish_status rows with new (epoch, seq), emits IngredientCheckChanged for all 5 ingredients, NO DishStatusChanged
        jdbc.update("DELETE FROM inventory_test.outbox_events");
        var bumpResp = postJson("/api/admin/epoch/bump", "");
        assertThat(bumpResp.statusCode()).isEqualTo(200);
        int newEpoch = JsonPath.read(bumpResp.body(), "$.epoch");
        long newSeq = ((Number) JsonPath.read(bumpResp.body(), "$.seq")).longValue();
        assertThat(newEpoch).isEqualTo(2);
        assertThat(newSeq).isGreaterThan(seqAfterRestart);

        List<Integer> dishEpochs = jdbc.queryForList("SELECT epoch FROM inventory_test.dish_status", Integer.class);
        assertThat(dishEpochs).containsOnly(2);
        assertThat(count("SELECT count(*) FROM inventory_test.outbox_events WHERE event_type = 'DishStatusChanged'")).isZero();
        assertThat(count("SELECT count(*) FROM inventory_test.outbox_events WHERE event_type = 'IngredientCheckChanged'")).isEqualTo(5L);

        // 5. Heartbeat emits current (epoch, seq) with key "heartbeat"
        MenuVersion hbVersion = restartedDishService.emitHeartbeat();
        assertThat(hbVersion.epoch()).isEqualTo(2);
        String hbJson = jdbc.queryForObject("""
                SELECT payload::text FROM inventory_test.outbox_events
                WHERE event_type = 'Heartbeat' AND message_key = 'heartbeat'
                """, String.class);
        JsonNode hbNode = mapper.readTree(hbJson);
        HeartbeatPayload hbPayload = mapper.treeToValue(hbNode.get("payload"), HeartbeatPayload.class);
        assertThat(hbPayload.epoch()).isEqualTo(2);
        assertThat(hbPayload.seq()).isEqualTo(newSeq);
    }

    @Test
    @Tag("kb-22")
    void unmatchedReleaseOrConsumedWritesAlertAndLogAndLeavesStockUnchanged() throws Exception {
        Logger logger = (Logger) LoggerFactory.getLogger(StockCommandService.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);

        try {
            UUID missingOrderId = UUID.randomUUID();
            UUID releaseEventId = UUID.randomUUID();
            var unmatchedRelease = releaseRequested(
                    releaseEventId, missingOrderId, CancelledBy.CUSTOMER, ReasonCode.CUSTOMER_CHANGED_MIND, List.of());

            EventProcessingContext ctx1 = cmdCtx();
            CommandResult r1 = commandService.processReleaseRequested(unmatchedRelease, ctx1);
            assertThat(r1.outcome()).isEqualTo(StockCommandService.Outcome.UNMATCHED);

            // Duplicate delivery with new event_id on a FAILED order also logs UNMATCHED_COMMAND
            UUID failedOrderId = UUID.randomUUID();
            jdbc.update("UPDATE inventory_test.stock SET available = 10 WHERE ingredient_id = 'GAO'");
            orderService.processOrderCreated(
                    orderCreated(UUID.randomUUID(), failedOrderId, "POS01", SourceType.POS, clock.instant(),
                            List.of(new OrderItemLine(1, "COM_GA", "Cơm gà", 1))),
                    orderCtx("POS01"));
            UUID consumedEventId = UUID.randomUUID();
            CommandResult r2 = commandService.processStockConsumed(
                    stockConsumed(consumedEventId, failedOrderId), cmdCtx());
            assertThat(r2.outcome()).isEqualTo(StockCommandService.Outcome.UNMATCHED);

            assertThat(stock("GAO").available()).isEqualByComparingTo("10");
            assertThat(stock("THIT_GA").available()).isEqualByComparingTo("1500");

            var alertsResp = get("/api/alerts?kind=UNMATCHED_COMMAND");
            assertThat(alertsResp.statusCode()).isEqualTo(200);
            assertThat(JsonPath.<List<String>>read(alertsResp.body(), "$[*].kind"))
                    .containsExactly("UNMATCHED_COMMAND", "UNMATCHED_COMMAND");
            assertThat(JsonPath.<List<String>>read(alertsResp.body(), "$[*].orderId"))
                    .containsExactly(failedOrderId.toString(), missingOrderId.toString());

            assertThat(appender.list)
                    .anySatisfy(ev -> assertThat(ev.getFormattedMessage())
                            .contains(missingOrderId.toString())
                            .contains(releaseEventId.toString()))
                    .anySatisfy(ev -> assertThat(ev.getFormattedMessage())
                            .contains(failedOrderId.toString())
                            .contains(consumedEventId.toString()));
        } finally {
            logger.detachAppender(appender);
        }
    }

    @Test
    @Tag("kb-30")
    void shortOfOneIngredientRollsBackSavepointAndReturnsStoredFailedOnResend() throws Exception {
        // GAO=1000 (enough), THIT_GA=50 (short for COM_GA=150)
        jdbc.update("UPDATE inventory_test.stock SET available = 50 WHERE ingredient_id = 'THIT_GA'");
        UUID orderId = UUID.randomUUID();
        UUID event1 = UUID.randomUUID();
        var ev1 = orderCreated(event1, orderId, "POS02", SourceType.POS, clock.instant(),
                List.of(new OrderItemLine(1, "COM_GA", "Cơm gà", 1)));

        OrderProcessingResult first = orderService.processOrderCreated(ev1, orderCtx("POS02"));
        assertThat(first.outcome()).isEqualTo(Outcome.APPLIED);
        assertThat(first.status()).isEqualTo(InventoryStatus.FAILED);
        assertThat(first.reasonCode()).isEqualTo(ReasonCode.INSUFFICIENT_INGREDIENTS);

        // Savepoint rolled back GAO deduction
        assertThat(stock("GAO").available()).isEqualByComparingTo("1000");
        assertThat(stock("GAO").reserved()).isEqualByComparingTo("0");
        assertThat(stock("THIT_GA").available()).isEqualByComparingTo("50");
        assertThat(count("SELECT count(*) FROM inventory_test.inventory_reservations WHERE order_id = ?", orderId)).isZero();
        assertThat(count("SELECT count(*) FROM inventory_test.processed_events WHERE event_id = ?", event1)).isEqualTo(1L);

        // Outbox has OrderFailed to BOTH status-events.POS02 and kitchen-orders
        assertThat(count("SELECT count(*) FROM inventory_test.outbox_events WHERE aggregate_id = ? AND event_type = 'OrderFailed' AND topic = 'status-events.POS02'",
                orderId)).isEqualTo(1L);
        assertThat(count("SELECT count(*) FROM inventory_test.outbox_events WHERE aggregate_id = ? AND event_type = 'OrderFailed' AND topic = 'kitchen-orders'",
                orderId)).isEqualTo(1L);

        // Case A: Resend with SAME event_id -> processed_events blocks duplicate, publishes nothing
        OrderProcessingResult sameEventDup = orderService.processOrderCreated(ev1, orderCtx("POS02"));
        assertThat(sameEventDup.outcome()).isEqualTo(Outcome.DUPLICATE_EVENT);
        assertThat(sameEventDup.status()).isEqualTo(InventoryStatus.FAILED);
        assertThat(sameEventDup.reasonCode()).isEqualTo(ReasonCode.INSUFFICIENT_INGREDIENTS);
        assertThat(stock("GAO").available()).isEqualByComparingTo("1000");
        assertThat(stock("THIT_GA").available()).isEqualByComparingTo("50");
        assertThat(count("SELECT count(*) FROM inventory_test.outbox_events WHERE aggregate_id = ?", orderId))
                .isEqualTo(2L);

        // Case B: Resend with NEW event_id -> returns stored FAILED ONLY to status-events.POS02
        UUID event2 = UUID.randomUUID();
        var ev2 = orderCreated(event2, orderId, "POS02", SourceType.POS, clock.instant(),
                List.of(new OrderItemLine(1, "COM_GA", "Cơm gà", 1)));
        OrderProcessingResult second = orderService.processOrderCreated(ev2, orderCtx("POS02"));
        assertThat(second.outcome()).isEqualTo(Outcome.DUPLICATE_ORDER);
        assertThat(second.status()).isEqualTo(InventoryStatus.FAILED);
        assertThat(second.reasonCode()).isEqualTo(ReasonCode.INSUFFICIENT_INGREDIENTS);
        assertThat(count("SELECT count(*) FROM inventory_test.outbox_events WHERE aggregate_id = ? AND topic = 'status-events.POS02'",
                orderId)).isEqualTo(2L);
        assertThat(count("SELECT count(*) FROM inventory_test.outbox_events WHERE aggregate_id = ? AND topic = 'kitchen-orders'",
                orderId)).isEqualTo(1L);
    }

    @Test
    @Tag("kb-39")
    void readableNegativeQuantityWritesFailedInvalidOrderDataWithoutDlq() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        var negativeQtyOrder = orderCreated(eventId, orderId, "POS01", SourceType.POS, clock.instant(),
                List.of(new OrderItemLine(1, "COM_GA", "Cơm gà", -2)));

        OrderProcessingResult res = orderService.processOrderCreated(negativeQtyOrder, orderCtx("POS01"));
        assertThat(res.status()).isEqualTo(InventoryStatus.FAILED);
        assertThat(res.reasonCode()).isEqualTo(ReasonCode.INVALID_ORDER_DATA);

        assertThat(stock("GAO").available()).isEqualByComparingTo("1000");
        assertThat(stock("THIT_GA").available()).isEqualByComparingTo("1500");
        assertThat(count("SELECT count(*) FROM inventory_test.alerts")).isZero();

        String branchOutboxJson = jdbc.queryForObject("""
                SELECT payload::text FROM inventory_test.outbox_events
                WHERE aggregate_id = ? AND topic = 'status-events.POS01'
                """, String.class, orderId);
        JsonNode root = mapper.readTree(branchOutboxJson);
        OrderFailedPayload payload = mapper.treeToValue(root.get("payload"), OrderFailedPayload.class);
        assertThat(payload.reasonCode()).isEqualTo(ReasonCode.INVALID_ORDER_DATA);
        assertThat(payload.originBranch()).isEqualTo("POS01");
    }

    @Test
    @Tag("kb-42")
    void concurrentLastPortionRacePermitsExactlyOneWinnerWithoutNegativeOrPartialStock() throws Exception {
        var props1 = new InventoryProperties("inventory-1", Duration.ofHours(4), Duration.ofMinutes(5), Duration.ofMinutes(5), 5, 50);
        var props2 = new InventoryProperties("inventory-2", Duration.ofHours(4), Duration.ofMinutes(5), Duration.ofMinutes(5), 5, 50);
        var instance1 = new InventoryOrderService(inventoryStore, dishStatusService, transactionManager, mapper, clock, props1);
        var instance2 = new InventoryOrderService(inventoryStore, dishStatusService, transactionManager, mapper, clock, props2);

        // Part A: 50-iteration multi-thread race simulating two Inventory instances competing for last portion
        try (ExecutorService pool = Executors.newFixedThreadPool(2)) {
            for (int iter = 0; iter < 50; iter++) {
                InventoryTestSupport.resetSchema(jdbc, SCHEMA);
                jdbc.update("UPDATE inventory_test.stock SET available = 150 WHERE ingredient_id = 'THIT_GA'");

                var orderA = orderCreated(UUID.randomUUID(), UUID.randomUUID(), "POS01", SourceType.POS, clock.instant(),
                        List.of(new OrderItemLine(1, "COM_GA", "Cơm gà", 1)));
                var orderB = orderCreated(UUID.randomUUID(), UUID.randomUUID(), "POS02", SourceType.POS, clock.instant(),
                        List.of(new OrderItemLine(1, "COM_GA", "Cơm gà", 1)));

                CyclicBarrier barrier = new CyclicBarrier(2);
                Future<OrderProcessingResult> fA = pool.submit(() -> {
                    barrier.await(5, TimeUnit.SECONDS);
                    return instance1.processOrderCreated(orderA, orderCtx("POS01"));
                });
                Future<OrderProcessingResult> fB = pool.submit(() -> {
                    barrier.await(5, TimeUnit.SECONDS);
                    return instance2.processOrderCreated(orderB, orderCtx("POS02"));
                });

                OrderProcessingResult rA = fA.get(10, TimeUnit.SECONDS);
                OrderProcessingResult rB = fB.get(10, TimeUnit.SECONDS);

                assertThat(List.of(rA.status(), rB.status()))
                        .containsExactlyInAnyOrder(InventoryStatus.HELD, InventoryStatus.FAILED);
                OrderProcessingResult loser = rA.status() == InventoryStatus.FAILED ? rA : rB;
                assertThat(loser.reasonCode()).isEqualTo(ReasonCode.INSUFFICIENT_INGREDIENTS);

                // No partial GAO reservation on loser, no negative THIT_GA
                assertThat(stock("GAO").available()).isEqualByComparingTo("900");
                assertThat(stock("GAO").reserved()).isEqualByComparingTo("100");
                assertThat(stock("THIT_GA").available()).isEqualByComparingTo("0");
                assertThat(stock("THIT_GA").reserved()).isEqualByComparingTo("150");
                InventoryTestSupport.assertInventoryInvariants(jdbc, SCHEMA);
            }
        }

        // Part B: Verify real PostgreSQL row-lock contention via pg_stat_activity
        InventoryTestSupport.resetSchema(jdbc, SCHEMA);
        jdbc.update("UPDATE inventory_test.stock SET available = 150 WHERE ingredient_id = 'THIT_GA'");

        CountDownLatch holderLocked = new CountDownLatch(1);
        CountDownLatch releaseHolder = new CountDownLatch(1);
        TransactionTemplate tx = new TransactionTemplate(transactionManager);

        try (ExecutorService pool = Executors.newFixedThreadPool(3)) {
            Future<?> holder = pool.submit(() -> tx.executeWithoutResult(status -> {
                jdbc.queryForList("SELECT ingredient_id FROM inventory_test.stock WHERE ingredient_id = 'GAO' FOR UPDATE");
                holderLocked.countDown();
                try {
                    if (!releaseHolder.await(15, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("Timed out waiting for lock release");
                    }
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(ex);
                }
            }));
            assertThat(holderLocked.await(10, TimeUnit.SECONDS)).isTrue();

            var orderA = orderCreated(UUID.randomUUID(), UUID.randomUUID(), "POS01", SourceType.POS, clock.instant(),
                    List.of(new OrderItemLine(1, "COM_GA", "Cơm gà", 1)));
            var orderB = orderCreated(UUID.randomUUID(), UUID.randomUUID(), "POS02", SourceType.POS, clock.instant(),
                    List.of(new OrderItemLine(1, "COM_GA", "Cơm gà", 1)));

            Future<OrderProcessingResult> fA = pool.submit(() -> instance1.processOrderCreated(orderA, orderCtx("POS01")));
            Future<OrderProcessingResult> fB = pool.submit(() -> instance2.processOrderCreated(orderB, orderCtx("POS02")));

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            int waiting = 0;
            while (System.nanoTime() < deadline) {
                waiting = jdbc.queryForObject("""
                        SELECT count(*) FROM pg_stat_activity
                        WHERE datname = current_database()
                          AND state = 'active'
                          AND wait_event_type = 'Lock'
                          AND query LIKE '%inventory_test%stock%'
                        """, Integer.class);
                if (waiting >= 2) {
                    break;
                }
                Thread.sleep(25);
            }
            assertThat(waiting).isGreaterThanOrEqualTo(2);

            releaseHolder.countDown();
            holder.get(10, TimeUnit.SECONDS);
            OrderProcessingResult rA = fA.get(15, TimeUnit.SECONDS);
            OrderProcessingResult rB = fB.get(15, TimeUnit.SECONDS);
            assertThat(List.of(rA.status(), rB.status()))
                    .containsExactlyInAnyOrder(InventoryStatus.HELD, InventoryStatus.FAILED);
        } finally {
            releaseHolder.countDown();
        }
    }

    @Test
    @Tag("kb-43")
    void stockConsumedMovesReservedToConsumedWithoutChangingAvailable() {
        jdbc.update("UPDATE inventory_test.stock SET available = 10.000 WHERE ingredient_id = 'THIT_GA'");
        adminService.upsertRecipe("STEAK_GA", "Gà nướng", Map.of("THIT_GA", InventoryTestSupport.bd("2.000")));

        UUID orderId = UUID.randomUUID();
        var created = orderCreated(UUID.randomUUID(), orderId, "POS01", SourceType.POS, clock.instant(),
                List.of(new OrderItemLine(1, "STEAK_GA", "Gà nướng", 1)));
        assertThat(orderService.processOrderCreated(created, orderCtx("POS01")).status())
                .isEqualTo(InventoryStatus.HELD);

        assertThat(stock("THIT_GA").available()).isEqualByComparingTo("8.000");
        assertThat(stock("THIT_GA").reserved()).isEqualByComparingTo("2.000");
        assertThat(stock("THIT_GA").consumed()).isEqualByComparingTo("0");

        CommandResult consumed = commandService.processStockConsumed(
                stockConsumed(UUID.randomUUID(), orderId), cmdCtx());
        assertThat(consumed.outcome()).isEqualTo(StockCommandService.Outcome.APPLIED);
        assertThat(consumed.orderStatus()).isEqualTo(InventoryStatus.CONSUMED);

        StockRowView after = stock("THIT_GA");
        assertThat(after.available()).isEqualByComparingTo("8.000");
        assertThat(after.reserved()).isEqualByComparingTo("0");
        assertThat(after.consumed()).isEqualByComparingTo("2.000");
        assertThat(after.wasted()).isEqualByComparingTo("0");

        // Duplicate StockConsumed ignored cleanly without alert
        CommandResult dup = commandService.processStockConsumed(
                stockConsumed(UUID.randomUUID(), orderId), cmdCtx());
        assertThat(dup.outcome()).isEqualTo(StockCommandService.Outcome.IGNORED_ALREADY_FINALIZED);
        assertThat(stock("THIT_GA").consumed()).isEqualByComparingTo("2.000");
        assertThat(count("SELECT count(*) FROM inventory_test.alerts")).isZero();
    }

    @Test
    @Tag("kb-44")
    void flaggedIngredientBlocksNewReservationEvenWhenAvailableStockIsSufficient() {
        jdbc.update("UPDATE inventory_test.stock SET needs_check = TRUE WHERE ingredient_id = 'THIT_GA'");
        // Stock is plenty (1000 GAO, 1500 THIT_GA), yet THIT_GA is flagged!
        UUID orderId = UUID.randomUUID();
        var created = orderCreated(UUID.randomUUID(), orderId, "ONLINE", SourceType.ONLINE, clock.instant(),
                List.of(new OrderItemLine(1, "COM_GA", "Cơm gà", 1)));

        OrderProcessingResult res = orderService.processOrderCreated(created, orderCtx("ONLINE"));
        assertThat(res.status()).isEqualTo(InventoryStatus.FAILED);
        assertThat(res.reasonCode()).isEqualTo(ReasonCode.INGREDIENT_AWAITING_STOCK_CHECK);

        // Savepoint rolled back GAO -> zero ingredients reserved
        assertThat(stock("GAO").available()).isEqualByComparingTo("1000");
        assertThat(stock("GAO").reserved()).isEqualByComparingTo("0");
        assertThat(stock("THIT_GA").available()).isEqualByComparingTo("1500");
        assertThat(stock("THIT_GA").reserved()).isEqualByComparingTo("0");
    }

    @Test
    @Tag("kb-48")
    void clearCheckEnforcesCountedOnHandMinimumRestoresDishesAndAllowsNewReservations() throws Exception {
        // Order 1 holds 150 THIT_GA; Order 2 spoils and flags THIT_GA (available becomes 1500-300=1200, reserved=150, needs_check=true)
        UUID heldOrderId = UUID.randomUUID();
        assertThat(orderService.processOrderCreated(
                orderCreated(UUID.randomUUID(), heldOrderId, "POS01", SourceType.POS, clock.instant(),
                        List.of(new OrderItemLine(1, "COM_GA", "Cơm gà", 1))),
                orderCtx("POS01")).status()).isEqualTo(InventoryStatus.HELD);

        UUID spoiledOrderId = UUID.randomUUID();
        assertThat(orderService.processOrderCreated(
                orderCreated(UUID.randomUUID(), spoiledOrderId, "POS01", SourceType.POS, clock.instant(),
                        List.of(new OrderItemLine(1, "COM_GA", "Cơm gà", 1))),
                orderCtx("POS01")).status()).isEqualTo(InventoryStatus.HELD);

        commandService.processReleaseRequested(
                releaseRequested(UUID.randomUUID(), spoiledOrderId, CancelledBy.KITCHEN, ReasonCode.SPOILED_INGREDIENT, List.of("THIT_GA")),
                cmdCtx());
        assertThat(stock("THIT_GA").needsCheck()).isTrue();
        assertThat(stock("THIT_GA").reserved()).isEqualByComparingTo("150");
        assertThat(dishStatus("COM_GA")).isEqualTo(DishStatus.SOLD_OUT);
        assertThat(dishStatus("CHAO_GA")).isEqualTo(DishStatus.SOLD_OUT);

        // 409 Conflict when countedOnHand < reserved (100 < 150)
        var conflictResp = postJson("/api/ingredients/THIT_GA/clear-check", """
                { "countedOnHand": 100 }
                """);
        assertThat(conflictResp.statusCode()).isEqualTo(409);
        assertThat(stock("THIT_GA").needsCheck()).isTrue();

        // Valid countedOnHand = 1050 (on-hand) -> available = 1050 - 150 = 900, needs_check = false
        jdbc.update("DELETE FROM inventory_test.outbox_events");
        var okResp = postJson("/api/ingredients/THIT_GA/clear-check", """
                { "countedOnHand": 1050 }
                """);
        assertThat(okResp.statusCode()).isEqualTo(200);
        assertThat(JsonPath.<Boolean>read(okResp.body(), "$.needsCheck")).isFalse();
        assertThat(JsonPath.<Integer>read(okResp.body(), "$.available")).isEqualTo(900);
        assertThat(JsonPath.<Integer>read(okResp.body(), "$.reserved")).isEqualTo(150);

        // Dishes return to AVAILABLE and IngredientCheckChanged(false) is emitted
        assertThat(dishStatus("COM_GA")).isEqualTo(DishStatus.AVAILABLE);
        assertThat(dishStatus("CHAO_GA")).isEqualTo(DishStatus.AVAILABLE);
        assertThat(count("""
                SELECT count(*) FROM inventory_test.outbox_events
                WHERE topic = 'menu-events' AND event_type = 'DishStatusChanged'
                """)).isEqualTo(2L);
        String checkJson = jdbc.queryForObject("""
                SELECT payload::text FROM inventory_test.outbox_events
                WHERE topic = 'menu-events' AND event_type = 'IngredientCheckChanged'
                """, String.class);
        var checkPayload = mapper.treeToValue(mapper.readTree(checkJson).get("payload"), IngredientCheckChangedPayload.class);
        assertThat(checkPayload.ingredientId()).isEqualTo("THIT_GA");
        assertThat(checkPayload.needsCheck()).isFalse();

        // Also verify empty/null body clear-check keeps available unchanged
        jdbc.update("UPDATE inventory_test.stock SET needs_check = TRUE WHERE ingredient_id = 'THIT_GA'");
        var emptyClear = postJson("/api/ingredients/THIT_GA/clear-check", "{}");
        assertThat(emptyClear.statusCode()).isEqualTo(200);
        assertThat(stock("THIT_GA").available()).isEqualByComparingTo("900");
        assertThat(stock("THIT_GA").needsCheck()).isFalse();

        // New order using THIT_GA succeeds
        assertThat(orderService.processOrderCreated(
                orderCreated(UUID.randomUUID(), UUID.randomUUID(), "POS01", SourceType.POS, clock.instant(),
                        List.of(new OrderItemLine(1, "COM_GA", "Cơm gà", 1))),
                orderCtx("POS01")).status()).isEqualTo(InventoryStatus.HELD);
    }

    @Test
    @Tag("kb-49")
    void releaseUsesRecordedReservationQuantityEvenAfterRecipeChanges() {
        jdbc.update("UPDATE inventory_test.stock SET available = 10.000 WHERE ingredient_id = 'THIT_GA'");
        adminService.upsertRecipe("GA_CHAY", "Gà quay", Map.of("THIT_GA", InventoryTestSupport.bd("2.000")));

        UUID orderId = UUID.randomUUID();
        assertThat(orderService.processOrderCreated(
                orderCreated(UUID.randomUUID(), orderId, "POS01", SourceType.POS, clock.instant(),
                        List.of(new OrderItemLine(1, "GA_CHAY", "Gà quay", 1))),
                orderCtx("POS01")).status()).isEqualTo(InventoryStatus.HELD);
        assertThat(stock("THIT_GA").available()).isEqualByComparingTo("8.000");
        assertThat(stock("THIT_GA").reserved()).isEqualByComparingTo("2.000");

        // Change recipe from 2.000 -> 3.000 after reservation
        adminService.upsertRecipe("GA_CHAY", "Gà quay", Map.of("THIT_GA", InventoryTestSupport.bd("3.000")));

        // Release must restore exact recorded 2.000 (NOT 3.000!)
        CommandResult released = commandService.processReleaseRequested(
                releaseRequested(UUID.randomUUID(), orderId, CancelledBy.KITCHEN, ReasonCode.EQUIPMENT_FAILURE, List.of()),
                cmdCtx());
        assertThat(released.outcome()).isEqualTo(StockCommandService.Outcome.APPLIED);
        assertThat(stock("THIT_GA").available()).isEqualByComparingTo("10.000");
        assertThat(stock("THIT_GA").reserved()).isEqualByComparingTo("0");
    }

    private EventEnvelope<OrderCreatedPayload> orderCreated(
            UUID eventId,
            UUID orderId,
            String bodyBranch,
            SourceType channel,
            Instant createdAt,
            List<OrderItemLine> items
    ) {
        return EventEnvelope.v1(
                eventId,
                EventType.OrderCreated,
                orderId,
                bodyBranch,
                createdAt,
                new OrderCreatedPayload(createdAt, channel, items));
    }

    private EventEnvelope<ReleaseRequestedPayload> releaseRequested(
            UUID eventId,
            UUID orderId,
            CancelledBy cancelledBy,
            ReasonCode reasonCode,
            List<String> spoiled
    ) {
        return EventEnvelope.v1(
                eventId,
                EventType.ReleaseRequested,
                orderId,
                "KITCHEN",
                clock.instant(),
                new ReleaseRequestedPayload(cancelledBy, reasonCode, spoiled));
    }

    private EventEnvelope<StockConsumedPayload> stockConsumed(UUID eventId, UUID orderId) {
        return EventEnvelope.v1(
                eventId,
                EventType.StockConsumed,
                orderId,
                "KITCHEN",
                clock.instant(),
                StockConsumedPayload.empty());
    }

    private EventProcessingContext orderCtx(String branch) {
        return EventProcessingContext.direct(EventTopics.orderEventsTopic(branch), 0, syntheticOffset.getAndIncrement());
    }

    private EventProcessingContext cmdCtx() {
        return EventProcessingContext.direct(EventTopics.STOCK_COMMANDS, 0, syntheticOffset.getAndIncrement());
    }

    private StockRowView stock(String ingredientId) {
        return jdbc.queryForObject("""
                SELECT available, reserved, consumed, wasted, needs_check
                FROM inventory_test.stock WHERE ingredient_id = ?
                """, (rs, row) -> new StockRowView(
                rs.getBigDecimal("available"),
                rs.getBigDecimal("reserved"),
                rs.getBigDecimal("consumed"),
                rs.getBigDecimal("wasted"),
                rs.getBoolean("needs_check")), ingredientId);
    }

    private DishStatus dishStatus(String dishId) {
        return DishStatus.valueOf(jdbc.queryForObject(
                "SELECT status FROM inventory_test.dish_status WHERE dish_id = ?",
                String.class,
                dishId));
    }

    private long count(String sql, Object... args) {
        Long c = jdbc.queryForObject(sql, Long.class, args);
        return c == null ? 0L : c;
    }

    private HttpResponse<String> get(String path) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                .timeout(Duration.ofSeconds(10))
                .GET()
                .build(), HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> postJson(String path, String body) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                .header("Content-Type", "application/json")
                .timeout(Duration.ofSeconds(10))
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build(), HttpResponse.BodyHandlers.ofString());
    }

    private record StockRowView(
            BigDecimal available,
            BigDecimal reserved,
            BigDecimal consumed,
            BigDecimal wasted,
            boolean needsCheck
    ) {
    }
}
