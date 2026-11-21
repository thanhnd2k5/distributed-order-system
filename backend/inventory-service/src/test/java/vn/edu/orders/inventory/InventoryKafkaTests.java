package vn.edu.orders.inventory;

import com.jayway.jsonpath.JsonPath;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import vn.edu.orders.contracts.events.CancelledBy;
import vn.edu.orders.contracts.events.DlqHeaders;
import vn.edu.orders.contracts.events.EventEnvelope;
import vn.edu.orders.contracts.events.EventTopics;
import vn.edu.orders.contracts.events.EventType;
import vn.edu.orders.contracts.events.OrderCreatedPayload;
import vn.edu.orders.contracts.events.OrderFailedPayload;
import vn.edu.orders.contracts.events.OrderItemLine;
import vn.edu.orders.contracts.events.OrderReservedPayload;
import vn.edu.orders.contracts.events.ReasonCode;
import vn.edu.orders.contracts.events.ReleaseRequestedPayload;
import vn.edu.orders.contracts.events.SourceType;
import vn.edu.orders.inventory.InventoryTestSupport.MutableClock;
import vn.edu.orders.inventory.application.DemoFailpoint;
import vn.edu.orders.inventory.application.InventoryOrderService.OrderProcessingResult;
import vn.edu.orders.inventory.config.DemoFailpointProperties;
import vn.edu.orders.inventory.config.InventoryProperties;
import vn.edu.orders.inventory.infrastructure.OrderEventsConsumer;
import vn.edu.orders.inventory.infrastructure.StockCommandsConsumer;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "outbox.enabled=true",
        "inventory.heartbeat-enabled=false",
        "spring.kafka.listener.auto-startup=true",
        "spring.flyway.default-schema=inventory_kafka_test",
        "spring.flyway.schemas=inventory_kafka_test",
        "inventory.instance-id=inventory-kafka-test",
        "inventory.max-poison-attempts=3",
        "inventory.poison-backoff-ms=10",
        "demo.failpoint.halt-after-commit-once=false"
})
@EmbeddedKafka(partitions = 2, topics = {
        "order-events.POS01",
        "order-events.POS02",
        "order-events.ONLINE",
        "status-events.POS01",
        "status-events.POS02",
        "status-events.ONLINE",
        EventTopics.KITCHEN_ORDERS,
        EventTopics.STOCK_COMMANDS,
        EventTopics.MENU_EVENTS,
        EventTopics.DLQ
})
@Import({InventoryTestSupport.MutableClockConfig.class, InventoryKafkaTests.FailpointTestConfig.class})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class InventoryKafkaTests {
    private static final String SCHEMA = "inventory_kafka_test";

    @TestConfiguration
    static class FailpointTestConfig {
        @Bean
        @Primary
        ControllableFailpoint controllableFailpoint(
                DemoFailpointProperties failpointProperties,
                InventoryProperties inventoryProperties
        ) {
            return new ControllableFailpoint(failpointProperties, inventoryProperties);
        }
    }

    static class ControllableFailpoint extends DemoFailpoint {
        private final AtomicBoolean failOnceAfterCommit = new AtomicBoolean(false);
        private volatile CountDownLatch trippedLatch = new CountDownLatch(0);

        ControllableFailpoint(DemoFailpointProperties properties, InventoryProperties inventoryProperties) {
            super(properties, inventoryProperties);
        }

        void armOnce(CountDownLatch latch) {
            this.trippedLatch = latch;
            this.failOnceAfterCommit.set(true);
        }

        void disarm() {
            this.failOnceAfterCommit.set(false);
        }

        @Override
        public void maybeHaltAfterCommit(OrderProcessingResult result, int partition, long offset) {
            if (result.isNewlyApplied() && failOnceAfterCommit.compareAndSet(true, false)) {
                trippedLatch.countDown();
                throw new IllegalStateException("Simulated crash after DB commit and before offset ack");
            }
            super.maybeHaltAfterCommit(result, partition, offset);
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
    private ObjectMapper mapper;

    @Autowired
    private MutableClock clock;

    @Autowired
    private ControllableFailpoint failpoint;

    private final HttpClient http = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    @BeforeEach
    void setup() {
        failpoint.disarm();
        clock.setInstant(Instant.parse("2026-11-22T10:00:00Z"));
        InventoryTestSupport.resetSchema(jdbc, SCHEMA);
    }

    @AfterEach
    void verifyInvariants() {
        failpoint.disarm();
        InventoryTestSupport.assertInventoryInvariants(jdbc, SCHEMA);
    }

    @Test
    @Tag("kb-24")
    void reservesPos01OrderUsingTopicOriginBranchAndNotifiesOnlyKitchenAndPos01() throws Exception {
        MessageListenerContainer orderContainer = waitForOrderListener();
        assertThat(orderContainer.getContainerProperties().getAckMode())
                .isEqualTo(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        UUID orderId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        // Deliberately put mismatched branchId ("POS02") in body; Inventory MUST use topic origin branch ("POS01")
        var event = EventEnvelope.v1(
                eventId,
                EventType.OrderCreated,
                orderId,
                "POS02",
                clock.instant(),
                new OrderCreatedPayload(
                        clock.instant(),
                        SourceType.POS,
                        List.of(new OrderItemLine(1, "COM_GA", "Cơm gà", 1))));

        kafkaTemplate.send("order-events.POS01", 0, orderId.toString(), mapper.writeValueAsString(event))
                .get(10, TimeUnit.SECONDS);

        awaitCondition(Duration.ofSeconds(15), () ->
                count("SELECT count(*) FROM inventory_kafka_test.outbox_events WHERE aggregate_id = ? AND sent_at IS NOT NULL",
                        orderId) == 2L);

        assertThat(jdbc.queryForObject(
                "SELECT origin_branch FROM inventory_kafka_test.inventory_orders WHERE order_id = ?",
                String.class, orderId)).isEqualTo("POS01");
        assertThat(jdbc.queryForObject(
                "SELECT status FROM inventory_kafka_test.inventory_orders WHERE order_id = ?",
                String.class, orderId)).isEqualTo("HELD");
        assertThat(stockAvailable("GAO")).isEqualByComparingTo("900");
        assertThat(stockAvailable("THIT_GA")).isEqualByComparingTo("1350");

        List<ConsumerRecord<String, String>> kitchenRecords = consumeAll("kitchen-orders", Duration.ofSeconds(5));
        List<ConsumerRecord<String, String>> pos01Records = consumeAll("status-events.POS01", Duration.ofSeconds(5));
        List<ConsumerRecord<String, String>> pos02Records = consumeAll("status-events.POS02", Duration.ofSeconds(2));
        List<ConsumerRecord<String, String>> onlineRecords = consumeAll("status-events.ONLINE", Duration.ofSeconds(2));

        assertThat(kitchenRecords).anySatisfy(rec -> {
            assertThat(rec.key()).isEqualTo(orderId.toString());
            JsonNode root = mapper.readTree(rec.value());
            assertThat(root.get("eventType").asString()).isEqualTo("OrderReserved");
            OrderReservedPayload payload = mapper.treeToValue(root.get("payload"), OrderReservedPayload.class);
            assertThat(payload.originBranch()).isEqualTo("POS01");
            assertThat(payload.reserved()).hasSize(2);
        });
        assertThat(pos01Records).anySatisfy(rec -> {
            assertThat(rec.key()).isEqualTo(orderId.toString());
            JsonNode root = mapper.readTree(rec.value());
            assertThat(root.get("eventType").asString()).isEqualTo("OrderReserved");
            assertThat(root.get("payload")).isEmpty();
        });
        assertThat(pos02Records).noneMatch(rec -> orderId.toString().equals(rec.key()));
        assertThat(onlineRecords).noneMatch(rec -> orderId.toString().equals(rec.key()));
        awaitCondition(Duration.ofSeconds(10), () -> committedOffset("order-events.POS01", 0) >= 1L);
    }

    @Test
    @Tag("kb-26")
    void redeliveryAfterDbCommitAndBeforeOffsetAckIsBlockedByProcessedEvents() throws Exception {
        MessageListenerContainer orderContainer = waitForOrderListener();

        UUID orderId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        var event = EventEnvelope.v1(
                eventId,
                EventType.OrderCreated,
                orderId,
                "POS01",
                clock.instant(),
                new OrderCreatedPayload(
                        clock.instant(),
                        SourceType.POS,
                        List.of(new OrderItemLine(1, "COM_GA", "Cơm gà", 1))));

        CountDownLatch tripped = new CountDownLatch(1);
        failpoint.armOnce(tripped);

        kafkaTemplate.send("order-events.POS01", 0, orderId.toString(), mapper.writeValueAsString(event))
                .get(10, TimeUnit.SECONDS);

        // Wait until DB commit succeeded and failpoint interrupted BEFORE ack.acknowledge()
        assertThat(tripped.await(15, TimeUnit.SECONDS)).isTrue();
        awaitCondition(Duration.ofSeconds(10), () ->
                count("SELECT count(*) FROM inventory_kafka_test.processed_events WHERE event_id = ?", eventId) == 1L);

        // Stop consumer to simulate process crash before offset commit
        orderContainer.stop();
        assertThat(committedOffset("order-events.POS01", 0)).isEqualTo(-1L);
        assertThat(stockAvailable("GAO")).isEqualByComparingTo("900");
        assertThat(stockAvailable("THIT_GA")).isEqualByComparingTo("1350");

        // Restart consumer: Kafka redelivers exact same record; processed_events blocks duplicate & commits offset
        orderContainer.start();
        ContainerTestUtils.waitForAssignment(orderContainer, 6);

        awaitCondition(Duration.ofSeconds(15), () ->
                committedOffset("order-events.POS01", 0) == 1L
                        && count("SELECT count(*) FROM inventory_kafka_test.outbox_events WHERE aggregate_id = ? AND sent_at IS NOT NULL",
                        orderId) == 2L);

        // Still deducted only once, no extra outbox rows
        assertThat(stockAvailable("GAO")).isEqualByComparingTo("900");
        assertThat(stockAvailable("THIT_GA")).isEqualByComparingTo("1350");
        assertThat(count("SELECT count(*) FROM inventory_kafka_test.outbox_events WHERE aggregate_id = ?", orderId))
                .isEqualTo(2L);
    }

    @Test
    @Tag("kb-29")
    void badMessagesUnknownEventTypeAndHigherVersionGoToDlqWithoutBlockingPartition() throws Exception {
        waitForOrderListener();
        waitForStockCommandsListener();

        UUID unknownOrderId = UUID.randomUUID();
        UUID unknownEventId = UUID.randomUUID();
        String unknownTypeJson = """
                {
                  "eventId": "%s",
                  "eventType": "FutureAuditEvent",
                  "schemaVersion": 1,
                  "orderId": "%s",
                  "branchId": "POS01",
                  "occurredAt": "2026-11-22T10:00:00Z",
                  "payload": {}
                }
                """.formatted(unknownEventId, unknownOrderId);

        UUID v2OrderId = UUID.randomUUID();
        UUID v2EventId = UUID.randomUUID();
        String higherVersionJson = """
                {
                  "eventId": "%s",
                  "eventType": "OrderCreated",
                  "schemaVersion": 2,
                  "orderId": "%s",
                  "branchId": "POS01",
                  "occurredAt": "2026-11-22T10:00:00Z",
                  "payload": {}
                }
                """.formatted(v2EventId, v2OrderId);

        UUID malformedKeyOrderId = UUID.randomUUID();
        String malformedJson = "{broken-json-payload";

        kafkaTemplate.send("order-events.POS01", 0, unknownOrderId.toString(), unknownTypeJson)
                .get(10, TimeUnit.SECONDS);
        kafkaTemplate.send("order-events.POS01", 0, v2OrderId.toString(), higherVersionJson)
                .get(10, TimeUnit.SECONDS);
        kafkaTemplate.send("order-events.POS01", 0, malformedKeyOrderId.toString(), malformedJson)
                .get(10, TimeUnit.SECONDS);

        // Also send a valid order right behind them on the SAME partition 0
        UUID validOrderId = UUID.randomUUID();
        var validOrder = EventEnvelope.v1(
                UUID.randomUUID(),
                EventType.OrderCreated,
                validOrderId,
                "POS01",
                clock.instant(),
                new OrderCreatedPayload(
                        clock.instant(),
                        SourceType.POS,
                        List.of(new OrderItemLine(1, "CHAO_GA", "Cháo gà", 1))));
        kafkaTemplate.send("order-events.POS01", 0, validOrderId.toString(), mapper.writeValueAsString(validOrder))
                .get(10, TimeUnit.SECONDS);

        // Also send unknown eventType to stock-commands
        UUID badCmdOrderId = UUID.randomUUID();
        String badCmdJson = """
                {
                  "eventId": "%s",
                  "eventType": "UnknownStockOp",
                  "schemaVersion": 1,
                  "orderId": "%s",
                  "branchId": "KITCHEN",
                  "occurredAt": "2026-11-22T10:00:00Z",
                  "payload": {}
                }
                """.formatted(UUID.randomUUID(), badCmdOrderId);
        kafkaTemplate.send(EventTopics.STOCK_COMMANDS, 0, badCmdOrderId.toString(), badCmdJson)
                .get(10, TimeUnit.SECONDS);

        awaitCondition(Duration.ofSeconds(15), () ->
                committedOffset("order-events.POS01", 0) >= 4L
                        && committedOffset(EventTopics.STOCK_COMMANDS, 0) >= 1L
                        && count("SELECT count(*) FROM inventory_kafka_test.alerts WHERE kind = 'NEEDS_REVIEW'") == 4L
                        && count("SELECT count(*) FROM inventory_kafka_test.outbox_events WHERE aggregate_id = ? AND sent_at IS NOT NULL",
                        validOrderId) == 2L);

        // Valid order behind poison messages was processed cleanly
        assertThat(stockAvailable("GAO")).isEqualByComparingTo("950");
        assertThat(stockAvailable("THIT_GA")).isEqualByComparingTo("1400");

        // All 4 poison messages landed on DLQ with original payload + headers
        List<ConsumerRecord<String, String>> dlqRecords = consumeAll(EventTopics.DLQ, Duration.ofSeconds(5));
        assertThat(dlqRecords).hasSize(4);
        assertThat(dlqRecords).anySatisfy(rec -> {
            assertThat(rec.key()).isEqualTo(malformedKeyOrderId.toString());
            assertThat(rec.value()).isEqualTo(malformedJson);
            assertThat(headerValue(rec, DlqHeaders.ORIGINAL_TOPIC)).isEqualTo("order-events.POS01");
            assertThat(headerValue(rec, DlqHeaders.CONSUMER_NAME)).isEqualTo("inventory-kafka-test");
            assertThat(headerValue(rec, DlqHeaders.ERROR)).isNotBlank();
        });

        // Verify NEEDS_REVIEW alerts include Kafka key fallback for malformed JSON
        var alertsResp = get("/api/alerts?kind=NEEDS_REVIEW");
        assertThat(alertsResp.statusCode()).isEqualTo(200);
        assertThat(JsonPath.<List<String>>read(alertsResp.body(), "$[*].orderId"))
                .containsExactlyInAnyOrder(
                        unknownOrderId.toString(),
                        v2OrderId.toString(),
                        malformedKeyOrderId.toString(),
                        badCmdOrderId.toString());
    }

    @Test
    @Tag("kb-39")
    void kafkaNegativeQuantityGoesToFailedNotDlq() throws Exception {
        waitForOrderListener();

        UUID orderId = UUID.randomUUID();
        var badQtyOrder = EventEnvelope.v1(
                UUID.randomUUID(),
                EventType.OrderCreated,
                orderId,
                "POS01",
                clock.instant(),
                new OrderCreatedPayload(
                        clock.instant(),
                        SourceType.POS,
                        List.of(new OrderItemLine(1, "COM_GA", "Cơm gà", -5))));

        kafkaTemplate.send("order-events.POS01", 0, orderId.toString(), mapper.writeValueAsString(badQtyOrder))
                .get(10, TimeUnit.SECONDS);

        awaitCondition(Duration.ofSeconds(15), () ->
                committedOffset("order-events.POS01", 0) >= 1L
                        && count("SELECT count(*) FROM inventory_kafka_test.outbox_events WHERE aggregate_id = ? AND sent_at IS NOT NULL",
                        orderId) == 2L);

        assertThat(count("SELECT count(*) FROM inventory_kafka_test.alerts")).isZero();
        assertThat(consumeAll(EventTopics.DLQ, Duration.ofSeconds(1))).isEmpty();

        List<ConsumerRecord<String, String>> branchRecords = consumeAll("status-events.POS01", Duration.ofSeconds(5));
        assertThat(branchRecords).anySatisfy(rec -> {
            JsonNode root = mapper.readTree(rec.value());
            assertThat(root.get("eventType").asString()).isEqualTo("OrderFailed");
            OrderFailedPayload p = mapper.treeToValue(root.get("payload"), OrderFailedPayload.class);
            assertThat(p.reasonCode()).isEqualTo(ReasonCode.INVALID_ORDER_DATA);
        });
    }

    @Test
    @Tag("kb-6")
    @Tag("kb-48")
    void spoiledReleaseAndClearCheckPublishMenuEventsToKafka() throws Exception {
        waitForOrderListener();
        waitForStockCommandsListener();

        UUID orderId = UUID.randomUUID();
        var created = EventEnvelope.v1(
                UUID.randomUUID(),
                EventType.OrderCreated,
                orderId,
                "POS01",
                clock.instant(),
                new OrderCreatedPayload(
                        clock.instant(),
                        SourceType.POS,
                        List.of(new OrderItemLine(1, "COM_GA", "Cơm gà", 1))));
        kafkaTemplate.send("order-events.POS01", 0, orderId.toString(), mapper.writeValueAsString(created))
                .get(10, TimeUnit.SECONDS);

        awaitCondition(Duration.ofSeconds(15), () ->
                count("SELECT count(*) FROM inventory_kafka_test.inventory_orders WHERE order_id = ? AND status = 'HELD'",
                        orderId) == 1L);

        var release = EventEnvelope.v1(
                UUID.randomUUID(),
                EventType.ReleaseRequested,
                orderId,
                "KITCHEN",
                clock.instant(),
                new ReleaseRequestedPayload(CancelledBy.KITCHEN, ReasonCode.SPOILED_INGREDIENT, List.of("THIT_GA")));
        kafkaTemplate.send(EventTopics.STOCK_COMMANDS, 0, orderId.toString(), mapper.writeValueAsString(release))
                .get(10, TimeUnit.SECONDS);

        awaitCondition(Duration.ofSeconds(15), () ->
                count("SELECT count(*) FROM inventory_kafka_test.outbox_events WHERE topic = 'menu-events' AND sent_at IS NOT NULL") >= 3L);

        var clearResp = postJson("/api/ingredients/THIT_GA/clear-check", "{}");
        assertThat(clearResp.statusCode()).isEqualTo(200);

        awaitCondition(Duration.ofSeconds(15), () ->
                count("SELECT count(*) FROM inventory_kafka_test.outbox_events WHERE topic = 'menu-events' AND sent_at IS NOT NULL") >= 6L);

        List<ConsumerRecord<String, String>> menuRecords = consumeAll(EventTopics.MENU_EVENTS, Duration.ofSeconds(5));
        assertThat(menuRecords).hasSizeGreaterThanOrEqualTo(6);
    }

    @Test
    void monitorEndpointDetectsSlowConsumerLagOverThreshold() throws Exception {
        MessageListenerContainer orderContainer = waitForOrderListener();
        // Establish group offset on order-events.POS01 partition 0 first
        UUID initOrderId = UUID.randomUUID();
        var initOrder = EventEnvelope.v1(
                UUID.randomUUID(),
                EventType.OrderCreated,
                initOrderId,
                "POS01",
                clock.instant(),
                new OrderCreatedPayload(
                        clock.instant(),
                        SourceType.POS,
                        List.of(new OrderItemLine(1, "CHAO_GA", "Cháo gà", 1))));
        kafkaTemplate.send("order-events.POS01", 0, initOrderId.toString(), mapper.writeValueAsString(initOrder))
                .get(10, TimeUnit.SECONDS);
        awaitCondition(Duration.ofSeconds(15), () -> committedOffset("order-events.POS01", 0) >= 1L);

        // Pause consumption, send an unread message, advance clock 6 minutes (> 5m threshold)
        orderContainer.stop();
        UUID laggedOrderId = UUID.randomUUID();
        var laggedOrder = EventEnvelope.v1(
                UUID.randomUUID(),
                EventType.OrderCreated,
                laggedOrderId,
                "POS01",
                clock.instant(),
                new OrderCreatedPayload(
                        clock.instant(),
                        SourceType.POS,
                        List.of(new OrderItemLine(1, "CHAO_GA", "Cháo gà", 1))));
        kafkaTemplate.send("order-events.POS01", 0, laggedOrderId.toString(), mapper.writeValueAsString(laggedOrder))
                .get(10, TimeUnit.SECONDS);

        clock.advance(Duration.ofMinutes(6));

        var monitorResp = get("/api/monitor");
        assertThat(monitorResp.statusCode()).isEqualTo(200);
        assertThat(JsonPath.<Boolean>read(monitorResp.body(), "$.kafkaReachable")).isTrue();
        assertThat(JsonPath.<Boolean>read(monitorResp.body(), "$.slowConsumerWarning")).isTrue();
        assertThat(JsonPath.<List<Integer>>read(monitorResp.body(),
                "$.topics[?(@.topic=='order-events.POS01')].totalUnread")).containsExactly(1);
        assertThat(JsonPath.<List<Boolean>>read(monitorResp.body(),
                "$.topics[?(@.topic=='order-events.POS01')].slowWarning")).containsExactly(true);
    }

    private MessageListenerContainer waitForOrderListener() {
        MessageListenerContainer container = listenerRegistry.getListenerContainer(OrderEventsConsumer.LISTENER_ID);
        assertThat(container).isNotNull();
        ContainerTestUtils.waitForAssignment(container, 6);
        return container;
    }

    private MessageListenerContainer waitForStockCommandsListener() {
        MessageListenerContainer container = listenerRegistry.getListenerContainer(StockCommandsConsumer.LISTENER_ID);
        assertThat(container).isNotNull();
        ContainerTestUtils.waitForAssignment(container, 2);
        return container;
    }

    private long committedOffset(String topic, int partition) throws Exception {
        try (AdminClient admin = AdminClient.create(
                Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString()))) {
            Map<TopicPartition, OffsetAndMetadata> offsets = admin.listConsumerGroupOffsets("inventory-v1")
                    .partitionsToOffsetAndMetadata()
                    .get(5, TimeUnit.SECONDS);
            OffsetAndMetadata meta = offsets.get(new TopicPartition(topic, partition));
            return meta == null ? -1L : meta.offset();
        }
    }

    private List<ConsumerRecord<String, String>> consumeAll(String topic, Duration waitDuration) {
        Map<String, Object> props = KafkaTestUtils.consumerProps(
                "test-verifier-" + UUID.randomUUID(), "false", embeddedKafka);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        try (Consumer<String, String> consumer = new DefaultKafkaConsumerFactory<String, String>(props).createConsumer()) {
            embeddedKafka.consumeFromAnEmbeddedTopic(consumer, topic);
            var records = KafkaTestUtils.getRecords(consumer, waitDuration);
            List<ConsumerRecord<String, String>> list = new ArrayList<>();
            records.records(topic).forEach(list::add);
            return list;
        }
    }

    private static String headerValue(ConsumerRecord<String, String> record, String headerName) {
        Header h = record.headers().lastHeader(headerName);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }

    private java.math.BigDecimal stockAvailable(String ingredientId) {
        return jdbc.queryForObject(
                "SELECT available FROM inventory_kafka_test.stock WHERE ingredient_id = ?",
                java.math.BigDecimal.class,
                ingredientId);
    }

    private long count(String sql, Object... args) {
        Long c = jdbc.queryForObject(sql, Long.class, args);
        return c == null ? 0L : c;
    }

    private void awaitCondition(Duration timeout, ThrowingBooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(100);
        }
        assertThat(condition.getAsBoolean()).isTrue();
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

    @FunctionalInterface
    private interface ThrowingBooleanSupplier {
        boolean getAsBoolean() throws Exception;
    }
}
