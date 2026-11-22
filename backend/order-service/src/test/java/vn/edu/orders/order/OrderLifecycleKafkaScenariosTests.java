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
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.ObjectMapper;
import vn.edu.orders.contracts.events.DishStatus;
import vn.edu.orders.contracts.events.DishStatusChangedPayload;
import vn.edu.orders.contracts.events.EventEnvelope;
import vn.edu.orders.contracts.events.EventTopics;
import vn.edu.orders.contracts.events.EventType;
import vn.edu.orders.contracts.events.HeartbeatPayload;
import vn.edu.orders.contracts.events.ReasonCode;
import vn.edu.orders.order.config.MutableClock;
import vn.edu.orders.order.config.OrderLifecycleProperties;
import vn.edu.orders.order.menu.infrastructure.MenuEventsConsumer;
import vn.edu.orders.order.orders.CancelOrderRequest;
import vn.edu.orders.order.orders.CreateOrderRequest;
import vn.edu.orders.order.orders.OrderService;
import vn.edu.orders.order.outbox.KafkaAdminConnectivityProbe;
import vn.edu.orders.order.outbox.OutboxRelayService;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Real PostgreSQL + real EmbeddedKafka + real HTTP snapshot server scenario tests
 * covering KB-10, KB-12, KB-18, and KB-20 without mocks.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "outbox.enabled=false",
        "order.heartbeat-enabled=false",
        "spring.kafka.listener.auto-startup=true",
        "spring.jpa.properties.hibernate.default_schema=v15_order_kafka_scenarios",
        "spring.flyway.default-schema=v15_order_kafka_scenarios",
        "spring.flyway.schemas=v15_order_kafka_scenarios",
        "site.id=POS01",
        "site.source-type=POS"
})
@EmbeddedKafka(partitions = 1, topics = {
        "order-events.POS01",
        "cancel-events.POS01",
        "status-events.POS01",
        EventTopics.MENU_EVENTS,
        EventTopics.DLQ
})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class OrderLifecycleKafkaScenariosTests {

    private static final String SCHEMA = "v15_order_kafka_scenarios";
    private static volatile String snapshotJsonBody = """
            {"epoch":1,"seq":1,"dishes":[]}
            """;
    private static HttpServer snapshotServer;

    @DynamicPropertySource
    static void startSnapshotServer(DynamicPropertyRegistry registry) throws Exception {
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
    private EmbeddedKafkaBroker embeddedKafka;

    @Autowired
    private KafkaListenerEndpointRegistry listenerRegistry;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private JdbcTemplate jdbc;

    @Autowired
    private JdbcClient jdbcClient;

    @Autowired
    private TransactionTemplate transactionTemplate;

    @Autowired
    private MutableClock clock;

    @Autowired
    private OrderService orderService;

    @Autowired
    private OutboxRelayService onlineRelay;

    @Autowired
    private OrderLifecycleProperties properties;

    @Autowired
    private ObjectMapper mapper;

    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

    @BeforeEach
    void resetState() {
        clock.reset();
        jdbc.update("DELETE FROM " + SCHEMA + ".alerts");
        jdbc.update("DELETE FROM " + SCHEMA + ".processed_events");
        jdbc.update("DELETE FROM " + SCHEMA + ".outbox_events");
        jdbc.update("DELETE FROM " + SCHEMA + ".order_items");
        jdbc.update("DELETE FROM " + SCHEMA + ".orders");
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
    @Tag("kb-10")
    void offlineOrderCancelledBeforeAttemptClosesLocallyDiscardsOutboxAndSendsNothingWhenOnline() throws Exception {
        // Real KafkaAdminConnectivityProbe pointed at closed port -> offline
        var offlineProps = new OrderLifecycleProperties(
                properties.staleThreshold(), properties.pendingInventoryAlertAfter(),
                properties.expiryThreshold(), properties.expiryWarningBefore(),
                properties.dlqMaxAttempts(), 300L, 500L,
                properties.inventoryBaseUrl(), properties.heartbeatIntervalMs(), false,
                properties.cancelMessageCooking());
        var offlineProbe = new KafkaAdminConnectivityProbe("127.0.0.1:19999", offlineProps, clock);
        var offlineRelay = new OutboxRelayService(
                jdbcClient, transactionTemplate, kafkaTemplate, offlineProbe, clock, offlineProps, SCHEMA);

        UUID orderId;
        try {
            orderId = orderService.create(new CreateOrderRequest(
                    UUID.randomUUID(), List.of(new CreateOrderRequest.Item("COM_GA", 2)))).order().orderId();

            // Sender runs while offline: probe fails, so attempted MUST stay FALSE
            assertThat(offlineRelay.publishPendingOnce()).isZero();
        } finally {
            offlineProbe.shutdown();
        }
        assertThat(jdbc.queryForObject(
                "SELECT attempted FROM " + SCHEMA + ".outbox_events WHERE order_id = ?",
                Boolean.class, orderId)).isFalse();

        // Cashier cancels locally
        var cancelResp = post("/api/orders/" + orderId + "/cancel", """
                {"reasonCode":"CUSTOMER_CHANGED_MIND"}
                """);
        assertThat(cancelResp.statusCode()).isEqualTo(200);
        assertThat(JsonPath.<String>read(cancelResp.body(), "$.status")).isEqualTo("CANCELLED");
        assertThat(JsonPath.<Integer>read(cancelResp.body(), "$.statusRank")).isEqualTo(2);
        assertThat(JsonPath.<String>read(cancelResp.body(), "$.closeReason")).isEqualTo("CUSTOMER_CHANGED_MIND");
        assertThat(jdbc.queryForObject(
                "SELECT count(*) FROM " + SCHEMA + ".outbox_events WHERE order_id = ?",
                Long.class, orderId)).isZero();

        // Network returns -> online relay runs -> zero messages sent for cancelled order
        onlineRelay.publishPendingOnce();
        List<ConsumerRecord<String, String>> records = pollRecords(
                List.of("order-events.POS01", "cancel-events.POS01"), Duration.ofSeconds(2));
        assertThat(records.stream().filter(r -> orderId.toString().equals(r.key())).toList()).isEmpty();
    }

    @Test
    @Tag("kb-12")
    void attemptedEntriesSurviveRepeatedFailuresBackOffAndSendInPerOrderOrderWithoutBlockingOthers() throws Exception {
        Instant t0 = Instant.parse("2026-11-22T10:00:00Z");
        clock.setFixed(t0);

        UUID orderA = orderService.create(new CreateOrderRequest(
                UUID.randomUUID(), List.of(new CreateOrderRequest.Item("COM_GA", 1)))).order().orderId();

        // Build real Kafka client targeting an unreachable port to simulate send failure after probe succeeds
        var failingFactory = new DefaultKafkaProducerFactory<String, String>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:19999",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.MAX_BLOCK_MS_CONFIG, 300,
                ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 300,
                ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 400
        ));
        var failingProps = new OrderLifecycleProperties(
                properties.staleThreshold(), properties.pendingInventoryAlertAfter(),
                properties.expiryThreshold(), properties.expiryWarningBefore(),
                properties.dlqMaxAttempts(), 500L, 400L,
                properties.inventoryBaseUrl(), properties.heartbeatIntervalMs(), false,
                properties.cancelMessageCooking());
        // Probe points to EmbeddedKafka (probe succeeds -> commits attempted=true), then send uses failingTemplate
        var okProbe = new KafkaAdminConnectivityProbe(embeddedKafka.getBrokersAsString(), failingProps, clock);
        var failingRelay = new OutboxRelayService(
                jdbcClient, transactionTemplate, new KafkaTemplate<>(failingFactory), okProbe, clock, failingProps, SCHEMA);

        try {
            // Attempt 1..3 fail -> row NOT discarded, attempted=true, attempt_count increments
            assertThat(failingRelay.publishPendingOnce()).isZero();
            clock.advance(Duration.ofSeconds(5));
            assertThat(failingRelay.publishPendingOnce()).isZero();
            clock.advance(Duration.ofSeconds(10));
            assertThat(failingRelay.publishPendingOnce()).isZero();

            assertThat(jdbc.queryForObject(
                    "SELECT attempted FROM " + SCHEMA + ".outbox_events WHERE order_id = ? AND kind = 'CREATE_ORDER'",
                    Boolean.class, orderA)).isTrue();
            assertThat(jdbc.queryForObject(
                    "SELECT attempt_count FROM " + SCHEMA + ".outbox_events WHERE order_id = ? AND kind = 'CREATE_ORDER'",
                    Integer.class, orderA)).isEqualTo(3);

            // Cancel orderA -> CANCEL_REQUEST added after CREATE_ORDER; CREATE_ORDER is still backing off
            var cancelRes = orderService.cancel(orderA, new CancelOrderRequest(ReasonCode.CUSTOMER_WAITED_TOO_LONG));
            assertThat(cancelRes.httpStatus()).isEqualTo(202);

            // Create orderB -> orderA's backoff must NOT block orderB, and orderA's CANCEL_REQUEST must NOT overtake CREATE_ORDER
            UUID orderB = orderService.create(new CreateOrderRequest(
                    UUID.randomUUID(), List.of(new CreateOrderRequest.Item("CHAO_GA", 1)))).order().orderId();

            var dueHead = onlineRelay.findOldestDuePerOrder(10);
            assertThat(dueHead).extracting(OutboxRelayService.PendingEvent::orderId).containsExactly(orderB);

            // Online relay sends orderB immediately while orderA CREATE_ORDER is still backing off
            assertThat(onlineRelay.publishPendingOnce()).isEqualTo(1);
            assertThat(jdbc.queryForObject(
                    "SELECT sent_at IS NOT NULL FROM " + SCHEMA + ".outbox_events WHERE order_id = ?",
                    Boolean.class, orderB)).isTrue();
            assertThat(jdbc.queryForObject(
                    "SELECT sent_at IS NULL FROM " + SCHEMA + ".outbox_events WHERE order_id = ? AND kind = 'CANCEL_REQUEST'",
                    Boolean.class, orderA)).isTrue();

            // Network returns and backoff elapses -> CREATE_ORDER sends first, then CANCEL_REQUEST sends next
            clock.advance(Duration.ofMinutes(1));
            assertThat(onlineRelay.publishPendingOnce()).isEqualTo(1);
            assertThat(jdbc.queryForObject(
                    "SELECT sent_at IS NOT NULL FROM " + SCHEMA + ".outbox_events WHERE order_id = ? AND kind = 'CREATE_ORDER'",
                    Boolean.class, orderA)).isTrue();
            assertThat(jdbc.queryForObject(
                    "SELECT sent_at IS NULL FROM " + SCHEMA + ".outbox_events WHERE order_id = ? AND kind = 'CANCEL_REQUEST'",
                    Boolean.class, orderA)).isTrue();

            assertThat(onlineRelay.publishPendingOnce()).isEqualTo(1);
            assertThat(jdbc.queryForObject(
                    "SELECT count(*) FROM " + SCHEMA + ".outbox_events WHERE order_id = ? AND sent_at IS NULL",
                    Long.class, orderA)).isZero();

            List<ConsumerRecord<String, String>> sent = pollRecords(
                    List.of("order-events.POS01", "cancel-events.POS01"), Duration.ofSeconds(5));
            assertThat(sent.stream()
                    .filter(r -> orderA.toString().equals(r.key()))
                    .map(ConsumerRecord::topic))
                    .containsExactlyInAnyOrder("order-events.POS01", "cancel-events.POS01");
        } finally {
            okProbe.shutdown();
            failingFactory.destroy();
        }
    }

    @Test
    @Tag("kb-18")
    void menuConsumerReceivesIncreasingVersionDishStatusChangesImmediately() throws Exception {
        waitForMenuListener();

        var soldOut = EventEnvelope.v1Menu(
                UUID.randomUUID(), EventType.DishStatusChanged, "INVENTORY", clock.instant(), null,
                new DishStatusChangedPayload("COM_GA", DishStatus.SOLD_OUT, 1, 1L));
        kafkaTemplate.send(EventTopics.MENU_EVENTS, "COM_GA", mapper.writeValueAsString(soldOut))
                .get(10, TimeUnit.SECONDS);

        awaitCondition(Duration.ofSeconds(10), () -> {
            var menu = get("/api/menu");
            List<String> statuses = JsonPath.read(menu.body(), "$.dishes[?(@.dishId=='COM_GA')].status");
            return List.of("SOLD_OUT").equals(statuses);
        });

        var available = EventEnvelope.v1Menu(
                UUID.randomUUID(), EventType.DishStatusChanged, "INVENTORY", clock.instant(), null,
                new DishStatusChangedPayload("COM_GA", DishStatus.AVAILABLE, 1, 2L));
        kafkaTemplate.send(EventTopics.MENU_EVENTS, "COM_GA", mapper.writeValueAsString(available))
                .get(10, TimeUnit.SECONDS);

        awaitCondition(Duration.ofSeconds(10), () -> {
            var menu = get("/api/menu");
            List<String> statuses = JsonPath.read(menu.body(), "$.dishes[?(@.dishId=='COM_GA')].status");
            List<Integer> seqs = JsonPath.read(menu.body(), "$.dishes[?(@.dishId=='COM_GA')].seq");
            return List.of("AVAILABLE").equals(statuses) && List.of(2).equals(seqs);
        });
        assertThat(JsonPath.<Boolean>read(get("/api/menu").body(), "$.stale")).isFalse();
    }

    @Test
    @Tag("kb-20")
    void epochIncreaseTriggersSnapshotSyncUpdatesMatchingDishesAndHidesOldEpochDishes() throws Exception {
        waitForMenuListener();

        // In epoch 1: COM_GA is SOLD_OUT (seq 10), CHAO_GA is AVAILABLE (seq 8)
        jdbc.update("UPDATE " + SCHEMA + ".menu_cache SET status = 'SOLD_OUT', epoch = 1, seq = 10 WHERE dish_id = 'COM_GA'");
        jdbc.update("UPDATE " + SCHEMA + ".menu_cache SET status = 'AVAILABLE', epoch = 1, seq = 8 WHERE dish_id = 'CHAO_GA'");

        // Inventory restores backup at epoch 2 without CHAO_GA in snapshot
        snapshotJsonBody = """
                {
                  "epoch": 2,
                  "seq": 1,
                  "dishes": [
                    { "dishId": "COM_GA", "dishName": "Cơm gà", "status": "AVAILABLE", "epoch": 2, "seq": 1 },
                    { "dishId": "PHO_BO", "dishName": "Phở bò", "status": "AVAILABLE", "epoch": 2, "seq": 1 }
                  ]
                }
                """;

        var heartbeatEpoch2 = EventEnvelope.v1Menu(
                UUID.randomUUID(), EventType.Heartbeat, "INVENTORY", clock.instant(), null,
                new HeartbeatPayload(2, 1L));
        kafkaTemplate.send(EventTopics.MENU_EVENTS, EventTopics.HEARTBEAT_KEY, mapper.writeValueAsString(heartbeatEpoch2))
                .get(10, TimeUnit.SECONDS);

        awaitCondition(Duration.ofSeconds(10), () ->
                Integer.valueOf(2).equals(jdbc.queryForObject(
                        "SELECT epoch FROM " + SCHEMA + ".menu_cache_meta WHERE id = TRUE", Integer.class)));

        // CHAO_GA (absent from new snapshot) moved to HIDDEN in menu_cache and is excluded from GET /api/menu
        assertThat(jdbc.queryForObject(
                "SELECT status FROM " + SCHEMA + ".menu_cache WHERE dish_id = 'CHAO_GA'", String.class))
                .isEqualTo("HIDDEN");
        var menuResp = get("/api/menu");
        assertThat(menuResp.statusCode()).isEqualTo(200);
        assertThat(JsonPath.<Integer>read(menuResp.body(), "$.epoch")).isEqualTo(2);
        assertThat(JsonPath.<List<String>>read(menuResp.body(), "$.dishes[*].dishId"))
                .containsExactly("COM_GA", "PHO_BO");
        assertThat(JsonPath.<List<String>>read(menuResp.body(), "$.dishes[?(@.dishId=='COM_GA')].status"))
                .containsExactly("AVAILABLE");
    }

    private void waitForMenuListener() throws Exception {
        MessageListenerContainer container = listenerRegistry.getListenerContainer(MenuEventsConsumer.LISTENER_ID);
        assertThat(container).isNotNull();
        ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());
    }

    private List<ConsumerRecord<String, String>> pollRecords(List<String> topics, Duration duration) {
        Map<String, Object> props = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString(),
                ConsumerConfig.GROUP_ID_CONFIG, "verifier-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class
        );
        List<ConsumerRecord<String, String>> collected = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(topics);
            long deadline = System.nanoTime() + duration.toNanos();
            while (System.nanoTime() < deadline) {
                ConsumerRecords<String, String> polled = consumer.poll(Duration.ofMillis(200));
                polled.forEach(collected::add);
            }
        }
        return collected;
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

    private static void awaitCondition(Duration timeout, ThrowingCondition condition) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        Throwable lastError = null;
        while (System.nanoTime() < deadline) {
            try {
                if (condition.evaluate()) {
                    return;
                }
            } catch (Throwable e) {
                lastError = e;
            }
            Thread.sleep(100);
        }
        if (lastError != null) {
            throw new AssertionError("Condition not met within " + timeout + ": " + lastError.getMessage(), lastError);
        }
        throw new AssertionError("Condition not met within " + timeout);
    }

    @FunctionalInterface
    private interface ThrowingCondition {
        boolean evaluate() throws Exception;
    }
}
