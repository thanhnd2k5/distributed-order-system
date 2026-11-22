package vn.edu.orders.kitchen;

import com.jayway.jsonpath.JsonPath;
import java.math.BigDecimal;
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
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import tools.jackson.databind.ObjectMapper;
import vn.edu.orders.contracts.events.CancelledBy;
import vn.edu.orders.contracts.events.DishStatus;
import vn.edu.orders.contracts.events.DishStatusChangedPayload;
import vn.edu.orders.contracts.events.DlqHeaders;
import vn.edu.orders.contracts.events.EventEnvelope;
import vn.edu.orders.contracts.events.EventTopics;
import vn.edu.orders.contracts.events.EventType;
import vn.edu.orders.contracts.events.HeartbeatPayload;
import vn.edu.orders.contracts.events.IngredientCheckChangedPayload;
import vn.edu.orders.contracts.events.IngredientQuantity;
import vn.edu.orders.contracts.events.OrderCancelledPayload;
import vn.edu.orders.contracts.events.OrderItemLine;
import vn.edu.orders.contracts.events.OrderReservedPayload;
import vn.edu.orders.contracts.events.ReasonCode;
import vn.edu.orders.contracts.events.ReleaseRequestedPayload;
import vn.edu.orders.kitchen.api.RejectTicketRequest;
import vn.edu.orders.kitchen.application.KitchenOrderService;
import vn.edu.orders.kitchen.infrastructure.KitchenKafkaConsumer;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "outbox.enabled=true",
        "spring.kafka.listener.auto-startup=true",
        "spring.jpa.properties.hibernate.default_schema=v15_kitchen_kafka_test",
        "spring.flyway.default-schema=v15_kitchen_kafka_test",
        "spring.flyway.schemas=v15_kitchen_kafka_test",
        "kitchen.instance-id=kitchen-kafka-test",
        "kitchen.slow-consumer-threshold-seconds=5",
        "kitchen.monitor-admin-timeout-ms=3000"
})
@EmbeddedKafka(
        partitions = 2,
        topics = {
                EventTopics.KITCHEN_ORDERS,
                EventTopics.MENU_EVENTS,
                EventTopics.STOCK_COMMANDS,
                EventTopics.DLQ,
                "cancel-events.POS01",
                "cancel-events.POS02",
                "status-events.POS01",
                "status-events.POS02"
        }
)
@DirtiesContext
class KitchenKafkaTests {
    private static final String SCHEMA = "v15_kitchen_kafka_test";

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
    private KitchenOrderService orderService;

    private final HttpClient http = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    @BeforeEach
    void resetKafkaTestSchema() {
        jdbc.update("DELETE FROM " + SCHEMA + ".outbox_events");
        jdbc.update("DELETE FROM " + SCHEMA + ".alerts");
        jdbc.update("DELETE FROM " + SCHEMA + ".flagged_ingredients");
        jdbc.update("DELETE FROM " + SCHEMA + ".kitchen_orders");
        jdbc.update("DELETE FROM " + SCHEMA + ".processed_events");
    }

    @Test
    @Tag("kb-4")
    void rejectTicketPublishesOrderCancelledImmediatelyAndReleaseRequestedOnceToStockCommands() throws Exception {
        waitForAllListenersAssigned();

        UUID orderId = UUID.randomUUID();
        var reserved = EventEnvelope.v1(
                UUID.randomUUID(),
                EventType.OrderReserved,
                orderId,
                "POS01",
                Instant.now(),
                OrderReservedPayload.forKitchen(
                        "POS01",
                        Instant.now(),
                        List.of(new OrderItemLine(1, "COM_GA", "Cơm gà", 2)),
                        List.of(new IngredientQuantity("GAO", new BigDecimal("200.000")),
                                new IngredientQuantity("THIT_GA", new BigDecimal("300.000")))));

        kafkaTemplate.send(EventTopics.KITCHEN_ORDERS, 0, orderId.toString(), mapper.writeValueAsString(reserved))
                .get(10, TimeUnit.SECONDS);

        awaitCondition(Duration.ofSeconds(15), () ->
                count("SELECT count(*) FROM " + SCHEMA + ".kitchen_orders WHERE order_id = ? AND status = 'QUEUED'", orderId) == 1L);
        awaitCondition(Duration.ofSeconds(10), () -> committedOffset(EventTopics.KITCHEN_ORDERS, 0) >= 1L);

        orderService.rejectTicket(orderId, new RejectTicketRequest("EQUIPMENT_FAILURE", List.of()));

        awaitCondition(Duration.ofSeconds(15), () ->
                count("SELECT count(*) FROM " + SCHEMA + ".outbox_events WHERE aggregate_id = ? AND sent_at IS NOT NULL", orderId) == 2L);

        var branchCancelled = consumeMatching(
                "status-events.POS01",
                rec -> orderId.toString().equals(rec.key()) && rec.value().contains("OrderCancelled"));
        assertThat(branchCancelled).hasSize(1);
        EventEnvelope<OrderCancelledPayload> cancelEnv = mapper.readValue(branchCancelled.get(0).value(),
                mapper.getTypeFactory().constructParametricType(EventEnvelope.class, OrderCancelledPayload.class));
        assertThat(cancelEnv.payload().cancelledBy()).isEqualTo(CancelledBy.KITCHEN);
        assertThat(cancelEnv.payload().reasonCode()).isEqualTo(ReasonCode.EQUIPMENT_FAILURE);

        var stockCommands = consumeMatching(
                EventTopics.STOCK_COMMANDS,
                rec -> orderId.toString().equals(rec.key()) && rec.value().contains("ReleaseRequested"));
        assertThat(stockCommands).hasSize(1);
        EventEnvelope<ReleaseRequestedPayload> relEnv = mapper.readValue(stockCommands.get(0).value(),
                mapper.getTypeFactory().constructParametricType(EventEnvelope.class, ReleaseRequestedPayload.class));
        assertThat(relEnv.payload().cancelledBy()).isEqualTo(CancelledBy.KITCHEN);
        assertThat(relEnv.payload().reasonCode()).isEqualTo(ReasonCode.EQUIPMENT_FAILURE);
        assertThat(relEnv.payload().spoiledIngredients()).isEmpty();
    }

    @Test
    @Tag("kb-25")
    void downtimeRecoveryConsumesBacklogWithNoLossAndNoDuplicates() throws Exception {
        waitForAllListenersAssigned();

        // Simulate Kitchen down: stop all listener containers while messages accumulate
        MessageListenerContainer ordersContainer = listenerRegistry.getListenerContainer(KitchenKafkaConsumer.KITCHEN_ORDERS_LISTENER_ID);
        MessageListenerContainer cancelContainer = listenerRegistry.getListenerContainer(KitchenKafkaConsumer.CANCEL_EVENTS_LISTENER_ID);
        ordersContainer.stop();
        cancelContainer.stop();
        awaitCondition(Duration.ofSeconds(10), () -> !ordersContainer.isRunning() && !cancelContainer.isRunning());

        UUID orderQueued = UUID.randomUUID();
        UUID orderCancelled = UUID.randomUUID();

        kafkaTemplate.send(EventTopics.KITCHEN_ORDERS, 0, orderQueued.toString(), mapper.writeValueAsString(
                EventEnvelope.v1(UUID.randomUUID(), EventType.OrderReserved, orderQueued, "POS01", Instant.now(),
                        OrderReservedPayload.forKitchen("POS01", Instant.now(),
                                List.of(new OrderItemLine(1, "COM_GA", "Cơm gà", 1)),
                                List.of(new IngredientQuantity("THIT_GA", new BigDecimal("150.000")))))))
                .get(10, TimeUnit.SECONDS);

        kafkaTemplate.send(EventTopics.KITCHEN_ORDERS, 1, orderCancelled.toString(), mapper.writeValueAsString(
                EventEnvelope.v1(UUID.randomUUID(), EventType.OrderReserved, orderCancelled, "POS01", Instant.now(),
                        OrderReservedPayload.forKitchen("POS01", Instant.now(),
                                List.of(new OrderItemLine(1, "CHAO_GA", "Cháo gà", 1)),
                                List.of(new IngredientQuantity("GAO", new BigDecimal("50.000")))))))
                .get(10, TimeUnit.SECONDS);

        kafkaTemplate.send("cancel-events.POS01", 0, orderCancelled.toString(), mapper.writeValueAsString(
                EventEnvelope.v1(UUID.randomUUID(), EventType.CancelRequested, orderCancelled, "POS01", Instant.now(),
                        new vn.edu.orders.contracts.events.CancelRequestedPayload(ReasonCode.CUSTOMER_CHANGED_MIND))))
                .get(10, TimeUnit.SECONDS);

        // Kitchen restarts
        ordersContainer.start();
        cancelContainer.start();
        waitForAllListenersAssigned();

        awaitCondition(Duration.ofSeconds(15), () ->
                count("SELECT count(*) FROM " + SCHEMA + ".kitchen_orders WHERE order_id = ? AND status = 'QUEUED'", orderQueued) == 1L
                        && count("SELECT count(*) FROM " + SCHEMA + ".kitchen_orders WHERE order_id = ? AND status = 'CANCELLED'", orderCancelled) == 1L);

        assertThat(count("SELECT count(*) FROM " + SCHEMA + ".outbox_events WHERE aggregate_id = ? AND event_type = 'ReleaseRequested'", orderCancelled))
                .isEqualTo(1L);
    }

    @Test
    @Tag("kb-26")
    void redeliveryAfterDbCommitBeforeOffsetCommitIsBlockedByProcessedEvents() throws Exception {
        waitForAllListenersAssigned();

        UUID orderId = UUID.randomUUID();
        UUID cancelEventId = UUID.randomUUID();
        var cancelEnv = EventEnvelope.v1(
                cancelEventId,
                EventType.CancelRequested,
                orderId,
                "POS01",
                Instant.now(),
                new vn.edu.orders.contracts.events.CancelRequestedPayload(ReasonCode.CUSTOMER_CHANGED_MIND));

        var sendResult = kafkaTemplate.send("cancel-events.POS01", 0, orderId.toString(), mapper.writeValueAsString(cancelEnv))
                .get(10, TimeUnit.SECONDS);
        long targetOffset = sendResult.getRecordMetadata().offset();

        awaitCondition(Duration.ofSeconds(15), () ->
                count("SELECT count(*) FROM " + SCHEMA + ".kitchen_orders WHERE order_id = ? AND status = 'CANCEL_PENDING'", orderId) == 1L);
        awaitCondition(Duration.ofSeconds(10), () -> committedOffset("cancel-events.POS01", 0) > targetOffset);
        assertThat(count("SELECT count(*) FROM " + SCHEMA + ".outbox_events WHERE aggregate_id = ? AND event_type = 'CancelPending'", orderId))
                .isEqualTo(1L);

        // Simulate crash before offset commit by stopping all group containers, rewinding offset to targetOffset, and restarting
        for (MessageListenerContainer container : listenerRegistry.getListenerContainers()) {
            container.stop();
        }
        awaitCondition(Duration.ofSeconds(10), () ->
                listenerRegistry.getListenerContainers().stream().noneMatch(MessageListenerContainer::isRunning));

        try (AdminClient admin = AdminClient.create(
                Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString()))) {
            admin.alterConsumerGroupOffsets("kitchen-v1",
                    Map.of(new TopicPartition("cancel-events.POS01", 0), new OffsetAndMetadata(targetOffset)))
                    .all().get(10, TimeUnit.SECONDS);
        }

        for (MessageListenerContainer container : listenerRegistry.getListenerContainers()) {
            container.start();
        }
        waitForAllListenersAssigned();

        awaitCondition(Duration.ofSeconds(15), () -> committedOffset("cancel-events.POS01", 0) > targetOffset);
        assertThat(count("SELECT count(*) FROM " + SCHEMA + ".processed_events WHERE event_id = ?", cancelEventId))
                .isEqualTo(1L);
        assertThat(count("SELECT count(*) FROM " + SCHEMA + ".outbox_events WHERE aggregate_id = ?", orderId))
                .isEqualTo(1L);
    }

    @Test
    @Tag("kb-29")
    void badAndUnknownEventsRouteToDlqWithNeedsReviewAlertWithoutBlockingPartitionAndMenuIgnoresDishAndHeartbeat() throws Exception {
        waitForAllListenersAssigned();

        UUID badKeyOrderId = UUID.randomUUID();
        UUID unknownOrderId = UUID.randomUUID();
        UUID futureVerOrderId = UUID.randomUUID();
        UUID validOrderId = UUID.randomUUID();

        // 1. Malformed JSON on kitchen-orders partition 0 (with UUID key)
        kafkaTemplate.send(EventTopics.KITCHEN_ORDERS, 0, badKeyOrderId.toString(), "{not-valid-json")
                .get(10, TimeUnit.SECONDS);

        // 2. Unknown eventType on kitchen-orders partition 0
        String unknownJson = """
                {
                  "eventId": "%s",
                  "eventType": "FutureAuditNote",
                  "schemaVersion": 1,
                  "orderId": "%s",
                  "branchId": "POS01",
                  "occurredAt": "2026-11-22T00:00:00Z",
                  "payload": {}
                }
                """.formatted(UUID.randomUUID(), unknownOrderId);
        kafkaTemplate.send(EventTopics.KITCHEN_ORDERS, 0, unknownOrderId.toString(), unknownJson)
                .get(10, TimeUnit.SECONDS);

        // 3. Higher schemaVersion on kitchen-orders partition 0
        String futureVerJson = """
                {
                  "eventId": "%s",
                  "eventType": "OrderReserved",
                  "schemaVersion": 2,
                  "orderId": "%s",
                  "branchId": "POS01",
                  "occurredAt": "2026-11-22T00:00:00Z",
                  "payload": {}
                }
                """.formatted(UUID.randomUUID(), futureVerOrderId);
        kafkaTemplate.send(EventTopics.KITCHEN_ORDERS, 0, futureVerOrderId.toString(), futureVerJson)
                .get(10, TimeUnit.SECONDS);

        // 4. Valid OrderReserved right behind them on SAME partition 0 -> must not be blocked!
        var validEnv = EventEnvelope.v1(
                UUID.randomUUID(),
                EventType.OrderReserved,
                validOrderId,
                "POS01",
                Instant.now(),
                OrderReservedPayload.forKitchen(
                        "POS01",
                        Instant.now(),
                        List.of(new OrderItemLine(1, "COM_GA", "Cơm gà", 1)),
                        List.of(new IngredientQuantity("THIT_GA", new BigDecimal("150.000")))));
        var validSend = kafkaTemplate.send(EventTopics.KITCHEN_ORDERS, 0, validOrderId.toString(), mapper.writeValueAsString(validEnv))
                .get(10, TimeUnit.SECONDS);

        awaitCondition(Duration.ofSeconds(15), () ->
                count("SELECT count(*) FROM " + SCHEMA + ".kitchen_orders WHERE order_id = ? AND status = 'QUEUED'", validOrderId) == 1L);
        awaitCondition(Duration.ofSeconds(10), () -> committedOffset(EventTopics.KITCHEN_ORDERS, 0) > validSend.getRecordMetadata().offset());

        assertThat(count("SELECT count(*) FROM " + SCHEMA + ".alerts WHERE kind = 'NEEDS_REVIEW'")).isEqualTo(3L);
        assertThat(count("SELECT count(*) FROM " + SCHEMA + ".alerts WHERE kind = 'NEEDS_REVIEW' AND order_id = ?", badKeyOrderId)).isEqualTo(1L);
        assertThat(count("SELECT count(*) FROM " + SCHEMA + ".alerts WHERE kind = 'NEEDS_REVIEW' AND order_id = ?", unknownOrderId)).isEqualTo(1L);
        assertThat(count("SELECT count(*) FROM " + SCHEMA + ".alerts WHERE kind = 'NEEDS_REVIEW' AND order_id = ?", futureVerOrderId)).isEqualTo(1L);

        var expectedDlqKeys = java.util.Set.of(badKeyOrderId.toString(), unknownOrderId.toString(), futureVerOrderId.toString());
        var dlqRecords = consumeMatching(EventTopics.DLQ, rec -> expectedDlqKeys.contains(rec.key()));
        assertThat(dlqRecords).hasSize(3);
        for (ConsumerRecord<String, String> dlqRec : dlqRecords) {
            Header origTopic = dlqRec.headers().lastHeader(DlqHeaders.ORIGINAL_TOPIC);
            Header consumerName = dlqRec.headers().lastHeader(DlqHeaders.CONSUMER_NAME);
            Header errHeader = dlqRec.headers().lastHeader(DlqHeaders.ERROR);
            assertThat(origTopic).isNotNull();
            assertThat(new String(origTopic.value(), StandardCharsets.UTF_8)).isEqualTo(EventTopics.KITCHEN_ORDERS);
            assertThat(new String(consumerName.value(), StandardCharsets.UTF_8)).isEqualTo("kitchen-kafka-test");
            assertThat(errHeader).isNotNull();
        }

        // menu-events: DishStatusChanged & Heartbeat gracefully ignored (not in processed_events, not in dlq)
        UUID dishEventId = UUID.randomUUID();
        UUID hbEventId = UUID.randomUUID();
        kafkaTemplate.send(EventTopics.MENU_EVENTS, 0, "COM_GA", mapper.writeValueAsString(
                EventEnvelope.v1Menu(dishEventId, EventType.DishStatusChanged, "INVENTORY", Instant.now(), null,
                        new DishStatusChangedPayload("COM_GA", DishStatus.SOLD_OUT, 1, 1L))))
                .get(10, TimeUnit.SECONDS);
        kafkaTemplate.send(EventTopics.MENU_EVENTS, 0, EventTopics.HEARTBEAT_KEY, mapper.writeValueAsString(
                EventEnvelope.v1Menu(hbEventId, EventType.Heartbeat, "INVENTORY", Instant.now(), null,
                        new HeartbeatPayload(1, 1L))))
                .get(10, TimeUnit.SECONDS);

        // Followed by real IngredientCheckChanged
        UUID checkEventId = UUID.randomUUID();
        kafkaTemplate.send(EventTopics.MENU_EVENTS, 0, "THIT_GA", mapper.writeValueAsString(
                EventEnvelope.v1Menu(checkEventId, EventType.IngredientCheckChanged, "INVENTORY", Instant.now(), null,
                        new IngredientCheckChangedPayload("THIT_GA", true, 1, 2L))))
                .get(10, TimeUnit.SECONDS);

        awaitCondition(Duration.ofSeconds(15), () ->
                count("SELECT count(*) FROM " + SCHEMA + ".flagged_ingredients WHERE ingredient_id = 'THIT_GA' AND needs_check = TRUE") == 1L);
        awaitCondition(Duration.ofSeconds(10), () -> committedOffset(EventTopics.MENU_EVENTS, 0) >= 3L);

        assertThat(count("SELECT count(*) FROM " + SCHEMA + ".processed_events WHERE event_id IN (?, ?)", dishEventId, hbEventId)).isZero();
        assertThat(count("SELECT count(*) FROM " + SCHEMA + ".processed_events WHERE event_id = ?", checkEventId)).isEqualTo(1L);
        assertThat(count("SELECT count(*) FROM " + SCHEMA + ".alerts WHERE source_topic = ?", EventTopics.MENU_EVENTS)).isZero();
    }

    @Test
    void slowConsumerMonitorDetectsUnreadOldMessageAge() throws Exception {
        waitForAllListenersAssigned();

        MessageListenerContainer ordersContainer = listenerRegistry.getListenerContainer(KitchenKafkaConsumer.KITCHEN_ORDERS_LISTENER_ID);
        ordersContainer.stop();
        awaitCondition(Duration.ofSeconds(10), () -> !ordersContainer.isRunning());

        // Send a record with timestamp 30 seconds in the past (threshold configured to 5 seconds)
        long oldTimestamp = Instant.now().minusSeconds(30).toEpochMilli();
        UUID orderId = UUID.randomUUID();
        String json = mapper.writeValueAsString(EventEnvelope.v1(
                UUID.randomUUID(),
                EventType.OrderReserved,
                orderId,
                "POS01",
                Instant.now(),
                OrderReservedPayload.forKitchen(
                        "POS01",
                        Instant.now(),
                        List.of(new OrderItemLine(1, "COM_GA", "Cơm gà", 1)),
                        List.of(new IngredientQuantity("THIT_GA", new BigDecimal("150.000"))))));

        kafkaTemplate.send(new ProducerRecord<>(EventTopics.KITCHEN_ORDERS, 0, oldTimestamp, orderId.toString(), json))
                .get(10, TimeUnit.SECONDS);

        var monitorResp = http.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/api/kitchen/monitor"))
                .timeout(Duration.ofSeconds(10))
                .GET()
                .build(), HttpResponse.BodyHandlers.ofString());

        assertThat(monitorResp.statusCode()).isEqualTo(200);
        assertThat(JsonPath.<Boolean>read(monitorResp.body(), "$.kafkaConnected")).isTrue();
        assertThat(JsonPath.<Boolean>read(monitorResp.body(), "$.anySlowConsumerWarning")).isTrue();
        assertThat(JsonPath.<List<Boolean>>read(
                monitorResp.body(), "$.topics[?(@.topic=='kitchen-orders')].slowConsumerWarning").get(0))
                .isTrue();

        ordersContainer.start();
        ContainerTestUtils.waitForAssignment(ordersContainer, embeddedKafka.getPartitionsPerTopic());
        awaitCondition(Duration.ofSeconds(15), () ->
                count("SELECT count(*) FROM " + SCHEMA + ".kitchen_orders WHERE order_id = ? AND status = 'QUEUED'", orderId) == 1L);
    }

    private void waitForAllListenersAssigned() throws Exception {
        MessageListenerContainer orders = listenerRegistry.getListenerContainer(KitchenKafkaConsumer.KITCHEN_ORDERS_LISTENER_ID);
        MessageListenerContainer cancel = listenerRegistry.getListenerContainer(KitchenKafkaConsumer.CANCEL_EVENTS_LISTENER_ID);
        MessageListenerContainer menu = listenerRegistry.getListenerContainer(KitchenKafkaConsumer.MENU_EVENTS_LISTENER_ID);
        assertThat(orders).isNotNull();
        assertThat(cancel).isNotNull();
        assertThat(menu).isNotNull();
        assertThat(orders.getContainerProperties().getAckMode()).isEqualTo(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        ContainerTestUtils.waitForAssignment(orders, embeddedKafka.getPartitionsPerTopic());
        ContainerTestUtils.waitForAssignment(cancel, embeddedKafka.getPartitionsPerTopic() * 2);
        ContainerTestUtils.waitForAssignment(menu, embeddedKafka.getPartitionsPerTopic());
    }

    private long committedOffset(String topic, int partition) throws Exception {
        try (AdminClient admin = AdminClient.create(
                Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString()))) {
            Map<TopicPartition, OffsetAndMetadata> offsets = admin.listConsumerGroupOffsets("kitchen-v1")
                    .partitionsToOffsetAndMetadata()
                    .get(5, TimeUnit.SECONDS);
            OffsetAndMetadata meta = offsets.get(new TopicPartition(topic, partition));
            return meta == null ? -1L : meta.offset();
        }
    }

    private List<ConsumerRecord<String, String>> consumeMatching(
            String topic,
            java.util.function.Predicate<ConsumerRecord<String, String>> filter
    ) {
        Map<String, Object> consumerProps = new java.util.HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-kitchen-consumer-" + UUID.randomUUID());
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        List<ConsumerRecord<String, String>> found = new ArrayList<>();
        try (Consumer<String, String> consumer = new DefaultKafkaConsumerFactory<String, String>(consumerProps)
                .createConsumer()) {
            embeddedKafka.consumeFromAnEmbeddedTopic(consumer, topic);
            consumer.seekToBeginning(consumer.assignment());
            long deadline = System.currentTimeMillis() + 5_000L;
            while (System.currentTimeMillis() < deadline) {
                var records = consumer.poll(Duration.ofMillis(300));
                for (ConsumerRecord<String, String> rec : records.records(topic)) {
                    if (filter.test(rec)) {
                        found.add(rec);
                    }
                }
                if (!found.isEmpty() && records.isEmpty()) {
                    break;
                }
            }
        }
        return found;
    }

    private long count(String sql, Object... args) {
        Long value = jdbc.queryForObject(sql, Long.class, args);
        return value == null ? 0L : value;
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

    @FunctionalInterface
    private interface ThrowingBooleanSupplier {
        boolean getAsBoolean() throws Exception;
    }
}
