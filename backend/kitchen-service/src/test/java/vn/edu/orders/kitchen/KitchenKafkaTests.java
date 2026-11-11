package vn.edu.orders.kitchen;

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
import vn.edu.orders.contracts.events.DoneEvent;
import vn.edu.orders.contracts.events.EventTopics;
import vn.edu.orders.contracts.events.FailedEvent;
import vn.edu.orders.contracts.events.FailureReasonCode;
import vn.edu.orders.contracts.events.OrderItemLine;
import vn.edu.orders.contracts.events.ReservedEvent;
import vn.edu.orders.contracts.events.SourceType;
import vn.edu.orders.kitchen.application.KitchenTicketService;
import vn.edu.orders.kitchen.infrastructure.StatusEventsConsumer;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "outbox.enabled=true",
        "spring.kafka.listener.auto-startup=true",
        "spring.jpa.properties.hibernate.default_schema=b4_kitchen_kafka_test",
        "spring.flyway.default-schema=b4_kitchen_kafka_test",
        "spring.flyway.schemas=b4_kitchen_kafka_test",
        "kitchen.instance-id=kitchen-kafka-test"
})
@EmbeddedKafka(partitions = 3, topics = {EventTopics.STATUS_EVENTS})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class KitchenKafkaTests {
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
    private KitchenTicketService ticketService;

    @BeforeEach
    void resetKafkaTestSchema() {
        jdbc.update("DELETE FROM b4_kitchen_kafka_test.outbox_events");
        jdbc.update("DELETE FROM b4_kitchen_kafka_test.kitchen_ticket_items");
        jdbc.update("DELETE FROM b4_kitchen_kafka_test.kitchen_tickets");
        jdbc.update("DELETE FROM b4_kitchen_kafka_test.processed_events");
    }

    @Test
    void validReservedEventIsConsumedCommittedAndCreatesWaitingTicketAndCompletePublishesDone() throws Exception {
        MessageListenerContainer container = waitForListenerAssignment();
        assertThat(container.getContainerProperties().getAckMode()).isEqualTo(ContainerProperties.AckMode.RECORD);

        UUID orderId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        ReservedEvent event = ReservedEvent.v1(
                eventId,
                orderId,
                SourceType.POS,
                "POS01",
                Instant.now(),
                orderId,
                List.of(new OrderItemLine(1, "COM_GA", "Cơm gà", 2))
        );

        kafkaTemplate.send(EventTopics.STATUS_EVENTS, 0, orderId.toString(), mapper.writeValueAsString(event))
                .get(10, TimeUnit.SECONDS);

        awaitCondition(Duration.ofSeconds(15), () ->
                count("SELECT count(*) FROM b4_kitchen_kafka_test.kitchen_tickets WHERE order_id = ? AND status = 'WAITING'",
                        orderId) == 1L);

        awaitCondition(Duration.ofSeconds(10), () -> committedOffset(0) >= 1L);

        UUID ticketId = jdbc.queryForObject(
                "SELECT ticket_id FROM b4_kitchen_kafka_test.kitchen_tickets WHERE order_id = ?",
                UUID.class, orderId);
        assertThat(ticketId).isNotNull();

        // Complete the ticket: OutboxPublisher should send DoneEvent to status-events
        ticketService.completeTicket(ticketId);

        awaitCondition(Duration.ofSeconds(15), () ->
                count("SELECT count(*) FROM b4_kitchen_kafka_test.outbox_events WHERE aggregate_id = ? AND sent_at IS NOT NULL",
                        ticketId) == 1L);

        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(
                "test-kitchen-done-consumer-" + UUID.randomUUID(), "false", embeddedKafka);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        try (Consumer<String, String> consumer = new DefaultKafkaConsumerFactory<String, String>(consumerProps)
                .createConsumer()) {
            embeddedKafka.consumeFromAnEmbeddedTopic(consumer, EventTopics.STATUS_EVENTS);
            consumer.seekToBeginning(consumer.assignment());
            ConsumerRecord<String, String> doneRecord = null;
            long deadline = System.currentTimeMillis() + 15_000L;
            while (System.currentTimeMillis() < deadline && doneRecord == null) {
                var records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records.records(EventTopics.STATUS_EVENTS)) {
                    if (orderId.toString().equals(record.key()) && record.value().contains("Done")) {
                        doneRecord = record;
                        break;
                    }
                }
            }
            assertThat(doneRecord).isNotNull();
            DoneEvent doneEvent = mapper.readValue(doneRecord.value(), DoneEvent.class);
            assertThat(doneEvent.orderId()).isEqualTo(orderId);
            assertThat(doneEvent.payload().ticketId()).isEqualTo(ticketId);
            assertThat(doneEvent.payload().completedAt()).isNotNull();
        }
    }

    @Test
    void failedAndDoneEventsOnStatusEventsAreSkippedAndAdvanceOffset() throws Exception {
        MessageListenerContainer container = waitForListenerAssignment();

        UUID failedOrderId = UUID.randomUUID();
        FailedEvent failedEvent = FailedEvent.v1(
                UUID.randomUUID(),
                failedOrderId,
                SourceType.POS,
                "POS01",
                Instant.now(),
                FailureReasonCode.INSUFFICIENT_INGREDIENT,
                "Không đủ nguyên liệu"
        );

        kafkaTemplate.send(EventTopics.STATUS_EVENTS, 1, failedOrderId.toString(), mapper.writeValueAsString(failedEvent))
                .get(10, TimeUnit.SECONDS);

        awaitCondition(Duration.ofSeconds(15), () -> committedOffset(1) == 1L);
        assertThat(count("SELECT count(*) FROM b4_kitchen_kafka_test.kitchen_tickets")).isZero();
        assertThat(count("SELECT count(*) FROM b4_kitchen_kafka_test.processed_events")).isZero();

        UUID doneOrderId = UUID.randomUUID();
        DoneEvent doneEvent = DoneEvent.v1(
                UUID.randomUUID(),
                doneOrderId,
                SourceType.POS,
                "POS01",
                Instant.now(),
                UUID.randomUUID(),
                Instant.now()
        );

        kafkaTemplate.send(EventTopics.STATUS_EVENTS, 1, doneOrderId.toString(), mapper.writeValueAsString(doneEvent))
                .get(10, TimeUnit.SECONDS);

        awaitCondition(Duration.ofSeconds(15), () -> committedOffset(1) == 2L);
        assertThat(count("SELECT count(*) FROM b4_kitchen_kafka_test.kitchen_tickets")).isZero();
        assertThat(container.isRunning()).isTrue();
    }

    @Test
    void unknownEventTypeIsSkippedAndAdvancesOffset() throws Exception {
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

        kafkaTemplate.send(EventTopics.STATUS_EVENTS, 0, "future-event", unknownEventJson)
                .get(10, TimeUnit.SECONDS);

        awaitCondition(Duration.ofSeconds(15), () -> committedOffset(0) == 1L);
        assertThat(container.isRunning()).isTrue();
        assertThat(count("SELECT count(*) FROM b4_kitchen_kafka_test.kitchen_tickets")).isZero();
    }

    @Test
    void malformedJsonStopsContainerWithoutCommittingOffset() throws Exception {
        MessageListenerContainer container = waitForListenerAssignment();

        UUID firstOrderId = UUID.randomUUID();
        ReservedEvent validFirst = ReservedEvent.v1(
                UUID.randomUUID(),
                firstOrderId,
                SourceType.POS,
                "POS01",
                Instant.now(),
                firstOrderId,
                List.of(new OrderItemLine(1, "COM_GA", "Cơm gà", 1))
        );
        kafkaTemplate.send(EventTopics.STATUS_EVENTS, 0, firstOrderId.toString(), mapper.writeValueAsString(validFirst))
                .get(10, TimeUnit.SECONDS);
        awaitCondition(Duration.ofSeconds(15), () -> committedOffset(0) == 1L);

        kafkaTemplate.send(EventTopics.STATUS_EVENTS, 0, "poison-key", "{malformed-json")
                .get(10, TimeUnit.SECONDS);

        awaitCondition(Duration.ofSeconds(15), () -> !container.isRunning());
        assertThat(committedOffset(0)).isEqualTo(1L);
    }

    @Test
    void unsupportedSchemaVersionStopsContainerWithoutCommittingOffset() throws Exception {
        MessageListenerContainer container = waitForListenerAssignment();

        UUID firstOrderId = UUID.randomUUID();
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
                    "items": [ { "lineNo": 1, "dishId": "COM_GA", "dishName": "Cơm gà", "quantity": 1 } ]
                  }
                }
                """.formatted(UUID.randomUUID(), badOrderId, badOrderId);

        kafkaTemplate.send(EventTopics.STATUS_EVENTS, 2, badOrderId.toString(), futureVersionJson)
                .get(10, TimeUnit.SECONDS);

        awaitCondition(Duration.ofSeconds(15), () -> !container.isRunning());
        assertThat(committedOffset(2)).isEqualTo(1L);
    }

    private MessageListenerContainer waitForListenerAssignment() throws Exception {
        MessageListenerContainer container = listenerRegistry.getListenerContainer(StatusEventsConsumer.LISTENER_ID);
        assertThat(container).isNotNull();
        ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());
        return container;
    }

    private long committedOffset(int partition) throws Exception {
        try (AdminClient admin = AdminClient.create(
                Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString()))) {
            Map<TopicPartition, OffsetAndMetadata> offsets = admin.listConsumerGroupOffsets("kitchen-v1")
                    .partitionsToOffsetAndMetadata()
                    .get(5, TimeUnit.SECONDS);
            OffsetAndMetadata meta = offsets.get(new TopicPartition(EventTopics.STATUS_EVENTS, partition));
            return meta == null ? -1L : meta.offset();
        }
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
