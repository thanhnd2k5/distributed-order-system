package vn.edu.orders.order.status;

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
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
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
import vn.edu.orders.contracts.events.DoneEvent;
import vn.edu.orders.contracts.events.EventTopics;
import vn.edu.orders.contracts.events.OrderItemLine;
import vn.edu.orders.contracts.events.ReservedEvent;
import vn.edu.orders.contracts.events.SourceType;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "outbox.enabled=false",
        "spring.kafka.listener.auto-startup=true",
        "spring.jpa.properties.hibernate.default_schema=b4_order_kafka_test",
        "spring.flyway.default-schema=b4_order_kafka_test",
        "spring.flyway.schemas=b4_order_kafka_test",
        "site.id=POS01",
        "site.source-type=POS"
})
@EmbeddedKafka(partitions = 3, topics = {EventTopics.STATUS_EVENTS})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class OrderStatusKafkaTests {

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
        jdbc.update("DELETE FROM b4_order_kafka_test.processed_events");
        jdbc.update("DELETE FROM b4_order_kafka_test.outbox_events");
        jdbc.update("DELETE FROM b4_order_kafka_test.order_items");
        jdbc.update("DELETE FROM b4_order_kafka_test.orders");
    }

    private void createOrder(UUID orderId, UUID clientRequestId, String status) {
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        jdbc.update("""
                INSERT INTO b4_order_kafka_test.orders (
                    order_id, client_request_id, site_id, source_type, status, created_at, updated_at
                ) VALUES (?, ?, 'POS01', 'POS', ?, ?, ?)
                """, orderId, clientRequestId, status, now, now);
    }

    @Test
    void validReservedAndDoneEventsAreConsumedAndCommitOffset() throws Exception {
        MessageListenerContainer container = waitForListenerAssignment();
        assertThat(container.getContainerProperties().getAckMode()).isEqualTo(ContainerProperties.AckMode.RECORD);

        UUID orderId = UUID.randomUUID();
        createOrder(orderId, UUID.randomUUID(), "PENDING_INVENTORY");

        ReservedEvent reserved = ReservedEvent.v1(
                UUID.randomUUID(),
                orderId,
                SourceType.POS,
                "POS01",
                Instant.now(),
                orderId,
                List.of(new OrderItemLine(1, "COM_GA", "Cơm gà", 2))
        );

        kafkaTemplate.send(EventTopics.STATUS_EVENTS, 0, orderId.toString(), mapper.writeValueAsString(reserved))
                .get(10, TimeUnit.SECONDS);

        awaitCondition(Duration.ofSeconds(15), () ->
                "RESERVED".equals(jdbc.queryForObject(
                        "SELECT status FROM b4_order_kafka_test.orders WHERE order_id = ?",
                        String.class, orderId)));
        awaitCondition(Duration.ofSeconds(10), () -> committedOffset(0) >= 1L);

        // Send Done event
        DoneEvent done = DoneEvent.v1(
                UUID.randomUUID(),
                orderId,
                SourceType.POS,
                "POS01",
                Instant.now(),
                UUID.randomUUID(),
                Instant.now()
        );

        kafkaTemplate.send(EventTopics.STATUS_EVENTS, 0, orderId.toString(), mapper.writeValueAsString(done))
                .get(10, TimeUnit.SECONDS);

        awaitCondition(Duration.ofSeconds(15), () ->
                "DONE".equals(jdbc.queryForObject(
                        "SELECT status FROM b4_order_kafka_test.orders WHERE order_id = ?",
                        String.class, orderId)));
        awaitCondition(Duration.ofSeconds(10), () -> committedOffset(0) >= 2L);
    }

    @Test
    void foreignSiteEventIsSkippedAndOffsetAdvancesWithoutModifyingOrders() throws Exception {
        MessageListenerContainer container = waitForListenerAssignment();

        UUID foreignOrderId = UUID.randomUUID();
        ReservedEvent foreignEvent = ReservedEvent.v1(
                UUID.randomUUID(),
                foreignOrderId,
                SourceType.POS,
                "POS02",
                Instant.now(),
                foreignOrderId,
                List.of(new OrderItemLine(1, "COM_GA", "Cơm gà", 1))
        );

        kafkaTemplate.send(EventTopics.STATUS_EVENTS, 1, foreignOrderId.toString(), mapper.writeValueAsString(foreignEvent))
                .get(10, TimeUnit.SECONDS);

        awaitCondition(Duration.ofSeconds(10), () -> committedOffset(1) >= 1L);
        assertThat(container.isRunning()).isTrue();

        Long count = jdbc.queryForObject(
                "SELECT count(*) FROM b4_order_kafka_test.processed_events WHERE order_id = ?",
                Long.class, foreignOrderId);
        assertThat(count).isEqualTo(0L);
    }

    @Test
    void malformedJsonStopsContainerWithoutCommittingOffset() throws Exception {
        MessageListenerContainer container = waitForListenerAssignment();

        UUID firstOrderId = UUID.randomUUID();
        createOrder(firstOrderId, UUID.randomUUID(), "PENDING_INVENTORY");
        ReservedEvent validFirst = ReservedEvent.v1(
                UUID.randomUUID(),
                firstOrderId,
                SourceType.POS,
                "POS01",
                Instant.now(),
                firstOrderId,
                List.of(new OrderItemLine(1, "COM_GA", "Cơm gà", 1))
        );
        kafkaTemplate.send(EventTopics.STATUS_EVENTS, 1, firstOrderId.toString(), mapper.writeValueAsString(validFirst))
                .get(10, TimeUnit.SECONDS);
        awaitCondition(Duration.ofSeconds(15), () -> committedOffset(1) == 1L);

        kafkaTemplate.send(EventTopics.STATUS_EVENTS, 1, "bad-key", "{not-valid-json")
                .get(10, TimeUnit.SECONDS);

        awaitCondition(Duration.ofSeconds(15), () -> !container.isRunning());
        assertThat(committedOffset(1)).isEqualTo(1L);
    }

    @Test
    void unsupportedSchemaVersionForMatchingSiteStopsContainerWithoutCommittingOffset() throws Exception {
        MessageListenerContainer container = waitForListenerAssignment();

        UUID firstOrderId = UUID.randomUUID();
        createOrder(firstOrderId, UUID.randomUUID(), "PENDING_INVENTORY");
        ReservedEvent validFirst = ReservedEvent.v1(
                UUID.randomUUID(),
                firstOrderId,
                SourceType.POS,
                "POS01",
                Instant.now(),
                firstOrderId,
                List.of(new OrderItemLine(1, "COM_GA", "Cơm gà", 1))
        );
        kafkaTemplate.send(EventTopics.STATUS_EVENTS, 2, firstOrderId.toString(), mapper.writeValueAsString(validFirst))
                .get(10, TimeUnit.SECONDS);
        awaitCondition(Duration.ofSeconds(15), () -> committedOffset(2) == 1L);

        UUID badOrderId = UUID.randomUUID();
        String futureVersionJson = """
                {
                  "eventId": "%s",
                  "schemaVersion": 2,
                  "eventType": "Reserved",
                  "orderId": "%s",
                  "sourceType": "POS",
                  "sourceSiteId": "POS01",
                  "occurredAt": "2026-11-11T00:00:00Z",
                  "payload": {
                    "reservationId": "%s",
                    "items": [ { "dishId": "COM_GA", "quantity": 1, "unitPrice": 40000 } ]
                  }
                }
                """.formatted(UUID.randomUUID(), badOrderId, badOrderId);

        kafkaTemplate.send(EventTopics.STATUS_EVENTS, 2, badOrderId.toString(), futureVersionJson)
                .get(10, TimeUnit.SECONDS);

        awaitCondition(Duration.ofSeconds(15), () -> !container.isRunning());
        assertThat(committedOffset(2)).isEqualTo(1L);
    }

    @Test
    void missingEventTypeForMatchingSiteStopsContainerWithoutCommittingOffset() throws Exception {
        MessageListenerContainer container = waitForListenerAssignment();

        UUID orderId = UUID.randomUUID();
        createOrder(orderId, UUID.randomUUID(), "PENDING_INVENTORY");
        ReservedEvent reserved = ReservedEvent.v1(
                UUID.randomUUID(),
                orderId,
                SourceType.POS,
                "POS01",
                Instant.now(),
                orderId,
                List.of(new OrderItemLine(1, "COM_GA", "Cơm gà", 1))
        );
        kafkaTemplate.send(EventTopics.STATUS_EVENTS, 0, orderId.toString(), mapper.writeValueAsString(reserved))
                .get(10, TimeUnit.SECONDS);
        awaitCondition(Duration.ofSeconds(15), () -> committedOffset(0) == 1L);

        UUID nextOrderId = UUID.randomUUID();
        createOrder(nextOrderId, UUID.randomUUID(), "PENDING_INVENTORY");
        String missingType = """
                {
                  "eventId": "%s",
                  "schemaVersion": 1,
                  "orderId": "%s",
                  "sourceType": "POS",
                  "sourceSiteId": "POS01",
                  "occurredAt": "2026-11-11T00:00:00Z"
                }
                """.formatted(UUID.randomUUID(), nextOrderId);

        kafkaTemplate.send(EventTopics.STATUS_EVENTS, 0, nextOrderId.toString(), missingType)
                .get(10, TimeUnit.SECONDS);

        awaitCondition(Duration.ofSeconds(15), () -> !container.isRunning());
        assertThat(committedOffset(0)).isEqualTo(1L);
        assertThat(jdbc.queryForObject(
                "SELECT status FROM b4_order_kafka_test.orders WHERE order_id = ?",
                String.class, nextOrderId)).isEqualTo("PENDING_INVENTORY");
    }

    @Test
    void doneEventWithoutPayloadStopsContainerWithoutChangingOrder() throws Exception {
        MessageListenerContainer container = waitForListenerAssignment();

        UUID orderId = UUID.randomUUID();
        createOrder(orderId, UUID.randomUUID(), "PENDING_INVENTORY");
        ReservedEvent reserved = ReservedEvent.v1(
                UUID.randomUUID(),
                orderId,
                SourceType.POS,
                "POS01",
                Instant.now(),
                orderId,
                List.of(new OrderItemLine(1, "COM_GA", "Cơm gà", 1))
        );
        kafkaTemplate.send(EventTopics.STATUS_EVENTS, 1, orderId.toString(), mapper.writeValueAsString(reserved))
                .get(10, TimeUnit.SECONDS);
        awaitCondition(Duration.ofSeconds(15), () -> committedOffset(1) == 1L);

        String doneWithoutPayload = """
                {
                  "eventId": "%s",
                  "schemaVersion": 1,
                  "eventType": "Done",
                  "orderId": "%s",
                  "sourceType": "POS",
                  "sourceSiteId": "POS01",
                  "occurredAt": "2026-11-11T00:00:00Z",
                  "payload": null
                }
                """.formatted(UUID.randomUUID(), orderId);
        kafkaTemplate.send(EventTopics.STATUS_EVENTS, 1, orderId.toString(), doneWithoutPayload)
                .get(10, TimeUnit.SECONDS);

        awaitCondition(Duration.ofSeconds(15), () -> !container.isRunning());
        assertThat(committedOffset(1)).isEqualTo(1L);
        assertThat(jdbc.queryForObject(
                "SELECT status FROM b4_order_kafka_test.orders WHERE order_id = ?",
                String.class, orderId)).isEqualTo("RESERVED");
    }

    private MessageListenerContainer waitForListenerAssignment() throws Exception {
        MessageListenerContainer container = listenerRegistry.getListenerContainer(OrderStatusConsumer.LISTENER_ID);
        assertThat(container).isNotNull();
        ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());
        return container;
    }

    private long committedOffset(int partition) throws Exception {
        try (AdminClient admin = AdminClient.create(
                Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString()))) {
            Map<TopicPartition, OffsetAndMetadata> offsets = admin.listConsumerGroupOffsets("order-status-POS01-v1")
                    .partitionsToOffsetAndMetadata()
                    .get(5, TimeUnit.SECONDS);
            OffsetAndMetadata meta = offsets.get(new TopicPartition(EventTopics.STATUS_EVENTS, partition));
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
