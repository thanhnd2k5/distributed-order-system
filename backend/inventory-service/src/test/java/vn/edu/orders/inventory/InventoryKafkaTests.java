package vn.edu.orders.inventory;

import java.time.Duration;
import java.time.Instant;
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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
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
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import tools.jackson.databind.ObjectMapper;
import vn.edu.orders.contracts.events.EventTopics;
import vn.edu.orders.contracts.events.OrderCreatedEvent;
import vn.edu.orders.contracts.events.OrderItemLine;
import vn.edu.orders.contracts.events.ReservedEvent;
import vn.edu.orders.contracts.events.SourceType;
import vn.edu.orders.inventory.infrastructure.OrderEventsConsumer;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "outbox.enabled=true",
        "spring.kafka.listener.auto-startup=true",
        "spring.jpa.properties.hibernate.default_schema=b3_kafka_test",
        "spring.flyway.default-schema=b3_kafka_test",
        "spring.flyway.schemas=b3_kafka_test",
        "inventory.instance-id=inventory-kafka-test",
        "demo.failpoint.halt-after-commit-once=false"
})
@EmbeddedKafka(partitions = 3, topics = {EventTopics.ORDER_EVENTS, EventTopics.STATUS_EVENTS})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class InventoryKafkaTests {
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
        jdbc.update("DELETE FROM b3_kafka_test.outbox_events");
        jdbc.update("DELETE FROM b3_kafka_test.reservation_items");
        jdbc.update("DELETE FROM b3_kafka_test.inventory_order_decisions");
        jdbc.update("DELETE FROM b3_kafka_test.processed_events");
        jdbc.update("UPDATE b3_kafka_test.ingredients SET available_quantity = 1000, updated_at = now() WHERE ingredient_id = 'GAO'");
        jdbc.update("UPDATE b3_kafka_test.ingredients SET available_quantity = 1500, updated_at = now() WHERE ingredient_id = 'THIT_GA'");
    }

    @Test
    void validOrderCreatedIsConsumedCommittedAndPublishedToStatusEvents() throws Exception {
        MessageListenerContainer container = waitForListenerAssignment();
        assertThat(container.getContainerProperties().getAckMode()).isEqualTo(ContainerProperties.AckMode.RECORD);

        UUID eventId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();
        OrderCreatedEvent event = OrderCreatedEvent.v1(
                eventId,
                orderId,
                SourceType.POS,
                "POS01",
                Instant.now(),
                List.of(new OrderItemLine(1, "COM_GA", "Cơm gà", 1)));

        kafkaTemplate.send(EventTopics.ORDER_EVENTS, 0, orderId.toString(), mapper.writeValueAsString(event))
                .get(10, TimeUnit.SECONDS);

        awaitCondition(Duration.ofSeconds(15), () ->
                count("SELECT count(*) FROM b3_kafka_test.outbox_events WHERE aggregate_id = ? AND sent_at IS NOT NULL",
                        orderId) == 1L);

        assertThat(stock("GAO")).isEqualTo(900L);
        assertThat(stock("THIT_GA")).isEqualTo(1350L);
        assertThat(jdbc.queryForObject(
                "SELECT decision FROM b3_kafka_test.inventory_order_decisions WHERE order_id = ?",
                String.class, orderId)).isEqualTo("RESERVED");

        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(
                "test-status-consumer-" + UUID.randomUUID(), "false", embeddedKafka);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        try (Consumer<String, String> consumer = new DefaultKafkaConsumerFactory<String, String>(consumerProps)
                .createConsumer()) {
            embeddedKafka.consumeFromAnEmbeddedTopic(consumer, EventTopics.STATUS_EVENTS);
            var records = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(10));
            ConsumerRecord<String, String> match = null;
            for (ConsumerRecord<String, String> record : records.records(EventTopics.STATUS_EVENTS)) {
                if (orderId.toString().equals(record.key())) {
                    match = record;
                    break;
                }
            }
            assertThat(match).isNotNull();
            ReservedEvent reserved = mapper.readValue(match.value(), ReservedEvent.class);
            assertThat(reserved.orderId()).isEqualTo(orderId);
            assertThat(reserved.payload().reservationId()).isEqualTo(orderId);
            assertThat(reserved.payload().items()).hasSize(1);
        }

        awaitCondition(Duration.ofSeconds(10), () -> committedOffset(0) >= 1L);
    }

    @Test
    void malformedJsonStopsContainerWithoutCommittingOffset() throws Exception {
        MessageListenerContainer container = waitForListenerAssignment();

        UUID firstOrderId = UUID.randomUUID();
        OrderCreatedEvent validFirst = OrderCreatedEvent.v1(
                UUID.randomUUID(),
                firstOrderId,
                SourceType.POS,
                "POS01",
                Instant.now(),
                List.of(new OrderItemLine(1, "CHAO_GA", "Cháo gà", 1)));
        kafkaTemplate.send(EventTopics.ORDER_EVENTS, 0, firstOrderId.toString(), mapper.writeValueAsString(validFirst))
                .get(10, TimeUnit.SECONDS);
        awaitCondition(Duration.ofSeconds(15), () -> committedOffset(0) == 1L);

        kafkaTemplate.send(EventTopics.ORDER_EVENTS, 0, "poison-json", "{malformed-json")
                .get(10, TimeUnit.SECONDS);

        awaitCondition(Duration.ofSeconds(15), () -> !container.isRunning());
        assertThat(count("SELECT count(*) FROM b3_kafka_test.processed_events")).isEqualTo(1L);
        assertThat(committedOffset(0)).isEqualTo(1L);
    }

    @Test
    void unsupportedSchemaVersionStopsContainerWithoutCommittingOffset() throws Exception {
        MessageListenerContainer container = waitForListenerAssignment();

        UUID firstOrderId = UUID.randomUUID();
        OrderCreatedEvent validFirst = OrderCreatedEvent.v1(
                UUID.randomUUID(),
                firstOrderId,
                SourceType.POS,
                "POS01",
                Instant.now(),
                List.of(new OrderItemLine(1, "CHAO_GA", "Cháo gà", 1)));
        kafkaTemplate.send(EventTopics.ORDER_EVENTS, 1, firstOrderId.toString(), mapper.writeValueAsString(validFirst))
                .get(10, TimeUnit.SECONDS);
        awaitCondition(Duration.ofSeconds(15), () -> committedOffset(1) == 1L);

        UUID badOrderId = UUID.randomUUID();
        String futureVersionJson = """
                {
                  "eventId": "%s",
                  "schemaVersion": 2,
                  "eventType": "OrderCreated",
                  "orderId": "%s",
                  "sourceType": "POS",
                  "sourceSiteId": "POS01",
                  "occurredAt": "2026-11-11T00:00:00Z",
                  "payload": {
                    "items": [ { "lineNo": 1, "dishId": "COM_GA", "dishName": "Cơm gà", "quantity": 1 } ]
                  }
                }
                """.formatted(UUID.randomUUID(), badOrderId);

        kafkaTemplate.send(EventTopics.ORDER_EVENTS, 1, badOrderId.toString(), futureVersionJson)
                .get(10, TimeUnit.SECONDS);

        awaitCondition(Duration.ofSeconds(15), () -> !container.isRunning());
        assertThat(count("SELECT count(*) FROM b3_kafka_test.processed_events")).isEqualTo(1L);
        assertThat(committedOffset(1)).isEqualTo(1L);
    }

    @Test
    void unknownEventTypeIsSkippedAdvancesOffsetAndNextOrderSucceeds() throws Exception {
        MessageListenerContainer container = waitForListenerAssignment();
        String unknownEventJson = """
                {
                  "eventId": "%s",
                  "schemaVersion": 1,
                  "eventType": "FutureAuditNote",
                  "orderId": "%s",
                  "sourceType": "POS",
                  "sourceSiteId": "POS01",
                  "occurredAt": "2026-11-11T00:00:00Z"
                }
                """.formatted(UUID.randomUUID(), UUID.randomUUID());

        kafkaTemplate.send(EventTopics.ORDER_EVENTS, 0, "future-event", unknownEventJson)
                .get(10, TimeUnit.SECONDS);

        awaitCondition(Duration.ofSeconds(15), () -> committedOffset(0) == 1L);
        assertThat(container.isRunning()).isTrue();
        assertThat(count("SELECT count(*) FROM b3_kafka_test.processed_events")).isZero();

        UUID orderId = UUID.randomUUID();
        OrderCreatedEvent validOrder = OrderCreatedEvent.v1(
                UUID.randomUUID(),
                orderId,
                SourceType.POS,
                "POS01",
                Instant.now(),
                List.of(new OrderItemLine(1, "COM_GA", "Cơm gà", 1)));
        kafkaTemplate.send(EventTopics.ORDER_EVENTS, 0, orderId.toString(), mapper.writeValueAsString(validOrder))
                .get(10, TimeUnit.SECONDS);

        awaitCondition(Duration.ofSeconds(15), () -> committedOffset(0) == 2L
                && count("SELECT count(*) FROM b3_kafka_test.outbox_events WHERE aggregate_id = ? AND sent_at IS NOT NULL",
                orderId) == 1L);
        assertThat(stock("GAO")).isEqualTo(900L);
        assertThat(stock("THIT_GA")).isEqualTo(1350L);
    }

    private MessageListenerContainer waitForListenerAssignment() throws Exception {
        MessageListenerContainer container = listenerRegistry.getListenerContainer(OrderEventsConsumer.LISTENER_ID);
        assertThat(container).isNotNull();
        ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());
        return container;
    }

    private long committedOffset(int partition) throws Exception {
        try (AdminClient admin = AdminClient.create(
                Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString()))) {
            Map<TopicPartition, OffsetAndMetadata> offsets = admin.listConsumerGroupOffsets("inventory-v1")
                    .partitionsToOffsetAndMetadata()
                    .get(5, TimeUnit.SECONDS);
            OffsetAndMetadata meta = offsets.get(new TopicPartition(EventTopics.ORDER_EVENTS, partition));
            return meta == null ? -1L : meta.offset();
        }
    }

    private long stock(String ingredientId) {
        return jdbc.queryForObject(
                "SELECT available_quantity FROM b3_kafka_test.ingredients WHERE ingredient_id = ?",
                Long.class,
                ingredientId);
    }

    private long count(String sql, Object... args) {
        return jdbc.queryForObject(sql, Long.class, args);
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
