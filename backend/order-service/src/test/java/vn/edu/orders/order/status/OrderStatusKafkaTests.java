package vn.edu.orders.order.status;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import tools.jackson.databind.ObjectMapper;
import vn.edu.orders.contracts.events.DlqHeaders;
import vn.edu.orders.contracts.events.EventEnvelope;
import vn.edu.orders.contracts.events.EventTopics;
import vn.edu.orders.contracts.events.EventType;
import vn.edu.orders.contracts.events.IngredientCheckChangedPayload;
import vn.edu.orders.contracts.events.OrderCompletedPayload;
import vn.edu.orders.contracts.events.OrderReservedPayload;
import vn.edu.orders.order.menu.infrastructure.MenuEventsConsumer;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "outbox.enabled=false",
        "order.heartbeat-enabled=false",
        "spring.kafka.listener.auto-startup=true",
        "spring.jpa.properties.hibernate.default_schema=b4_order_kafka_test",
        "spring.flyway.default-schema=b4_order_kafka_test",
        "spring.flyway.schemas=b4_order_kafka_test",
        "site.id=POS01",
        "site.source-type=POS",
        "order.dlq-max-attempts=3"
})
@EmbeddedKafka(partitions = 1, topics = {"status-events.POS01", EventTopics.MENU_EVENTS, EventTopics.DLQ})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class OrderStatusKafkaTests {

    private static final String POS01_STATUS_TOPIC = "status-events.POS01";

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

    @BeforeEach
    void resetKafkaTestSchema() {
        jdbc.update("DELETE FROM b4_order_kafka_test.alerts");
        jdbc.update("DELETE FROM b4_order_kafka_test.processed_events");
        jdbc.update("DELETE FROM b4_order_kafka_test.outbox_events");
        jdbc.update("DELETE FROM b4_order_kafka_test.order_items");
        jdbc.update("DELETE FROM b4_order_kafka_test.orders");
    }

    private void createOrder(UUID orderId, String status, int rank) {
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        jdbc.update("""
                INSERT INTO b4_order_kafka_test.orders (
                    order_id, client_request_id, site_id, source_type, status, status_rank,
                    cancel_flag, created_at, updated_at
                ) VALUES (?, ?, 'POS01', 'POS', ?, ?, 'NONE', ?, ?)
                """, orderId, UUID.randomUUID(), status, rank, now, now);
    }

    @Test
    void validReservedAndCompletedEventsAreConsumedWithManualAck() throws Exception {
        MessageListenerContainer container = waitForListener(OrderStatusConsumer.LISTENER_ID);
        assertThat(container.getContainerProperties().getAckMode()).isEqualTo(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        UUID orderId = UUID.randomUUID();
        createOrder(orderId, "PENDING_INVENTORY", 0);

        var reserved = EventEnvelope.v1(
                UUID.randomUUID(), EventType.OrderReserved, orderId, "POS01", Instant.now(),
                OrderReservedPayload.forBranch());
        kafkaTemplate.send(POS01_STATUS_TOPIC, 0, orderId.toString(), mapper.writeValueAsString(reserved))
                .get(10, TimeUnit.SECONDS);

        awaitCondition(Duration.ofSeconds(15), () ->
                "RESERVED".equals(jdbc.queryForObject(
                        "SELECT status FROM b4_order_kafka_test.orders WHERE order_id = ?",
                        String.class, orderId)));
        awaitCondition(Duration.ofSeconds(10), () -> committedOffset("order-status-POS01-v1", POS01_STATUS_TOPIC, 0) >= 1L);

        var completed = EventEnvelope.v1(
                UUID.randomUUID(), EventType.OrderCompleted, orderId, "POS01", Instant.now(),
                OrderCompletedPayload.empty());
        kafkaTemplate.send(POS01_STATUS_TOPIC, 0, orderId.toString(), mapper.writeValueAsString(completed))
                .get(10, TimeUnit.SECONDS);

        awaitCondition(Duration.ofSeconds(15), () ->
                "DONE".equals(jdbc.queryForObject(
                        "SELECT status FROM b4_order_kafka_test.orders WHERE order_id = ?",
                        String.class, orderId)));
        awaitCondition(Duration.ofSeconds(10), () -> committedOffset("order-status-POS01-v1", POS01_STATUS_TOPIC, 0) >= 2L);
    }

    @Test
    void unknownEventTypeAndHigherSchemaVersionOnDisplaySideAreAlertedIgnoredAndOffsetCommitted() throws Exception {
        MessageListenerContainer container = waitForListener(OrderStatusConsumer.LISTENER_ID);

        UUID unknownOrderId = UUID.randomUUID();
        String unknownTypeJson = """
                {
                  "eventId": "%s",
                  "schemaVersion": 1,
                  "eventType": "FutureBranchHint",
                  "orderId": "%s",
                  "branchId": "POS01",
                  "occurredAt": "2026-11-21T10:00:00Z",
                  "payload": {}
                }
                """.formatted(UUID.randomUUID(), unknownOrderId);
        kafkaTemplate.send(POS01_STATUS_TOPIC, 0, unknownOrderId.toString(), unknownTypeJson)
                .get(10, TimeUnit.SECONDS);

        UUID futureVersionOrderId = UUID.randomUUID();
        String futureVersionJson = """
                {
                  "eventId": "%s",
                  "schemaVersion": 2,
                  "eventType": "OrderReserved",
                  "orderId": "%s",
                  "branchId": "POS01",
                  "occurredAt": "2026-11-21T10:00:00Z",
                  "payload": {}
                }
                """.formatted(UUID.randomUUID(), futureVersionOrderId);
        kafkaTemplate.send(POS01_STATUS_TOPIC, 0, futureVersionOrderId.toString(), futureVersionJson)
                .get(10, TimeUnit.SECONDS);

        awaitCondition(Duration.ofSeconds(15), () ->
                committedOffset("order-status-POS01-v1", POS01_STATUS_TOPIC, 0) >= 2L);
        assertThat(container.isRunning()).isTrue();
        assertThat(jdbc.queryForObject(
                "SELECT count(*) FROM b4_order_kafka_test.alerts WHERE kind = 'UNKNOWN_EVENT_TYPE'",
                Long.class)).isEqualTo(1L);
        assertThat(jdbc.queryForObject(
                "SELECT count(*) FROM b4_order_kafka_test.alerts WHERE kind = 'UNSUPPORTED_SCHEMA_VERSION'",
                Long.class)).isEqualTo(1L);
    }

    @Test
    void unreadableMessageMovesToDlqWithAlertWithoutBlockingSubsequentOrders() throws Exception {
        MessageListenerContainer container = waitForListener(OrderStatusConsumer.LISTENER_ID);

        UUID badKeyOrderId = UUID.randomUUID();
        kafkaTemplate.send(POS01_STATUS_TOPIC, 0, badKeyOrderId.toString(), "{not-valid-json")
                .get(10, TimeUnit.SECONDS);

        UUID nextOrderId = UUID.randomUUID();
        createOrder(nextOrderId, "PENDING_INVENTORY", 0);
        var reserved = EventEnvelope.v1(
                UUID.randomUUID(), EventType.OrderReserved, nextOrderId, "POS01", Instant.now(),
                OrderReservedPayload.forBranch());
        kafkaTemplate.send(POS01_STATUS_TOPIC, 0, nextOrderId.toString(), mapper.writeValueAsString(reserved))
                .get(10, TimeUnit.SECONDS);

        awaitCondition(Duration.ofSeconds(15), () ->
                "RESERVED".equals(jdbc.queryForObject(
                        "SELECT status FROM b4_order_kafka_test.orders WHERE order_id = ?",
                        String.class, nextOrderId)));
        awaitCondition(Duration.ofSeconds(10), () ->
                committedOffset("order-status-POS01-v1", POS01_STATUS_TOPIC, 0) >= 2L);
        assertThat(container.isRunning()).isTrue();

        assertThat(jdbc.queryForObject(
                "SELECT count(*) FROM b4_order_kafka_test.alerts WHERE kind = 'NEEDS_REVIEW' AND order_id = ?",
                Long.class, badKeyOrderId)).isEqualTo(1L);

        ConsumerRecord<String, String> dlqRecord = pollSingleDlqRecord();
        assertThat(dlqRecord).isNotNull();
        assertThat(dlqRecord.value()).isEqualTo("{not-valid-json");
        assertThat(new String(dlqRecord.headers().lastHeader(DlqHeaders.ORIGINAL_TOPIC).value(), StandardCharsets.UTF_8))
                .isEqualTo(POS01_STATUS_TOPIC);
        assertThat(new String(dlqRecord.headers().lastHeader(DlqHeaders.CONSUMER_NAME).value(), StandardCharsets.UTF_8))
                .isEqualTo("POS01-1");
    }

    @Test
    void menuConsumerIgnoresIngredientCheckChangedWithoutProcessedEventsOrDlq() throws Exception {
        waitForListener(MenuEventsConsumer.LISTENER_ID);

        UUID eventId = UUID.randomUUID();
        var ingredientChanged = EventEnvelope.v1Menu(
                eventId, EventType.IngredientCheckChanged, "INVENTORY", Instant.now(), null,
                new IngredientCheckChangedPayload("THIT_GA", true, 1, 1L));
        kafkaTemplate.send(EventTopics.MENU_EVENTS, 0, "THIT_GA", mapper.writeValueAsString(ingredientChanged))
                .get(10, TimeUnit.SECONDS);

        awaitCondition(Duration.ofSeconds(15), () ->
                committedOffset("order-menu-POS01-v1", EventTopics.MENU_EVENTS, 0) >= 1L);
        assertThat(jdbc.queryForObject(
                "SELECT count(*) FROM b4_order_kafka_test.processed_events WHERE event_id = ?",
                Long.class, eventId)).isZero();
        assertThat(jdbc.queryForObject(
                "SELECT count(*) FROM b4_order_kafka_test.alerts",
                Long.class)).isZero();
    }

    private ConsumerRecord<String, String> pollSingleDlqRecord() {
        Map<String, Object> props = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString(),
                ConsumerConfig.GROUP_ID_CONFIG, "dlq-verifier-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class
        );
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(EventTopics.DLQ));
            long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while (System.nanoTime() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
                if (!records.isEmpty()) {
                    return records.iterator().next();
                }
            }
            return null;
        }
    }

    private MessageListenerContainer waitForListener(String listenerId) throws Exception {
        MessageListenerContainer container = listenerRegistry.getListenerContainer(listenerId);
        assertThat(container).isNotNull();
        ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());
        return container;
    }

    private long committedOffset(String groupId, String topic, int partition) throws Exception {
        try (AdminClient admin = AdminClient.create(
                Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString()))) {
            Map<TopicPartition, OffsetAndMetadata> offsets = admin.listConsumerGroupOffsets(groupId)
                    .partitionsToOffsetAndMetadata()
                    .get(5, TimeUnit.SECONDS);
            OffsetAndMetadata meta = offsets.get(new TopicPartition(topic, partition));
            return meta == null ? -1L : meta.offset();
        }
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
            throw new AssertionError("Condition not met within " + timeout + ", last error: " + lastError.getMessage(), lastError);
        }
        throw new AssertionError("Condition not met within " + timeout);
    }

    @FunctionalInterface
    private interface ThrowingCondition {
        boolean evaluate() throws Exception;
    }
}
