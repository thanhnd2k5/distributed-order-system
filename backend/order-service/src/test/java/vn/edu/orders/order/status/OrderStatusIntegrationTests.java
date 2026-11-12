package vn.edu.orders.order.status;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import vn.edu.orders.contracts.events.DoneEvent;
import vn.edu.orders.contracts.events.EventType;
import vn.edu.orders.contracts.events.FailedEvent;
import vn.edu.orders.contracts.events.FailureReasonCode;
import vn.edu.orders.contracts.events.OrderItemLine;
import vn.edu.orders.contracts.events.ReservedEvent;
import vn.edu.orders.contracts.events.SourceType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "outbox.enabled=false",
        "spring.kafka.listener.auto-startup=false",
        "spring.jpa.properties.hibernate.default_schema=b4_order_test",
        "spring.flyway.default-schema=b4_order_test",
        "spring.flyway.schemas=b4_order_test",
        "site.id=POS01",
        "site.source-type=POS"
})
class OrderStatusIntegrationTests {

    @Autowired
    private OrderStatusSyncService syncService;

    @Autowired
    private JdbcTemplate jdbc;

    @BeforeEach
    void cleanTables() {
        jdbc.update("DELETE FROM b4_order_test.processed_events");
        jdbc.update("DELETE FROM b4_order_test.outbox_events");
        jdbc.update("DELETE FROM b4_order_test.order_items");
        jdbc.update("DELETE FROM b4_order_test.orders");
    }

    private void createOrder(UUID orderId, UUID clientRequestId, String status) {
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        jdbc.update("""
                INSERT INTO b4_order_test.orders (
                    order_id, client_request_id, site_id, source_type, status, created_at, updated_at
                ) VALUES (?, ?, 'POS01', 'POS', ?, ?, ?)
                """, orderId, clientRequestId, status, now, now);
    }

    private String getOrderStatus(UUID orderId) {
        return jdbc.queryForObject("SELECT status FROM b4_order_test.orders WHERE order_id = ?",
                String.class, orderId);
    }

    private String getFailureReason(UUID orderId) {
        return jdbc.queryForObject("SELECT failure_reason FROM b4_order_test.orders WHERE order_id = ?",
                String.class, orderId);
    }

    private long version(UUID orderId) {
        return jdbc.queryForObject("SELECT version FROM b4_order_test.orders WHERE order_id = ?",
                Long.class, orderId);
    }

    private String outcome(UUID eventId) {
        return jdbc.queryForObject(
                "SELECT outcome FROM b4_order_test.processed_events WHERE event_id = ?",
                String.class, eventId);
    }

    private long processedCount(UUID orderId) {
        return jdbc.queryForObject(
                "SELECT count(*) FROM b4_order_test.processed_events WHERE order_id = ?",
                Long.class, orderId);
    }

    @Test
    void processReservedTransitionsPendingInventoryToReserved() {
        UUID orderId = UUID.randomUUID();
        createOrder(orderId, UUID.randomUUID(), "PENDING_INVENTORY");

        ReservedEvent event = ReservedEvent.v1(
                UUID.randomUUID(),
                orderId,
                SourceType.POS,
                "POS01",
                Instant.now(),
                orderId,
                List.of(new OrderItemLine(1, "COM_GA", "Cơm gà", 2))
        );

        var result = syncService.processReserved(event);
        assertThat(result.outcome()).isEqualTo("SUCCESS");
        assertThat(getOrderStatus(orderId)).isEqualTo("RESERVED");

        Long count = jdbc.queryForObject(
                "SELECT count(*) FROM b4_order_test.processed_events WHERE event_id = ? AND outcome = 'SUCCESS'",
                Long.class, event.eventId());
        assertThat(count).isEqualTo(1L);
    }

    @Test
    void processReservedIsTechnicallyDeduplicatedOnEventId() {
        UUID orderId = UUID.randomUUID();
        createOrder(orderId, UUID.randomUUID(), "PENDING_INVENTORY");

        ReservedEvent event = ReservedEvent.v1(
                UUID.randomUUID(),
                orderId,
                SourceType.POS,
                "POS01",
                Instant.now(),
                orderId,
                List.of(new OrderItemLine(1, "COM_GA", "Cơm gà", 2))
        );

        var first = syncService.processReserved(event);
        assertThat(first.outcome()).isEqualTo("SUCCESS");

        var second = syncService.processReserved(event);
        assertThat(second.outcome()).isEqualTo("SKIPPED_DUPLICATE");
        assertThat(getOrderStatus(orderId)).isEqualTo("RESERVED");
    }

    @Test
    void processReservedIsBusinessIdempotent() {
        UUID orderId = UUID.randomUUID();
        createOrder(orderId, UUID.randomUUID(), "PENDING_INVENTORY");

        ReservedEvent event1 = ReservedEvent.v1(
                UUID.randomUUID(),
                orderId,
                SourceType.POS,
                "POS01",
                Instant.now(),
                orderId,
                List.of(new OrderItemLine(1, "COM_GA", "Cơm gà", 2))
        );
        ReservedEvent event2 = ReservedEvent.v1(
                UUID.randomUUID(),
                orderId,
                SourceType.POS,
                "POS01",
                Instant.now(),
                orderId,
                List.of(new OrderItemLine(1, "COM_GA", "Cơm gà", 2))
        );

        var first = syncService.processReserved(event1);
        assertThat(first.outcome()).isEqualTo("SUCCESS");

        var second = syncService.processReserved(event2);
        assertThat(second.outcome()).isEqualTo("ALREADY_APPLIED");
        assertThat(getOrderStatus(orderId)).isEqualTo("RESERVED");
        assertThat(outcome(event2.eventId())).isEqualTo("ALREADY_APPLIED");
        assertThat(version(orderId)).isEqualTo(1L);
    }

    @Test
    void lateReservedDoesNotRevertDoneOrder() {
        UUID orderId = UUID.randomUUID();
        createOrder(orderId, UUID.randomUUID(), "DONE");

        ReservedEvent lateReserved = ReservedEvent.v1(
                UUID.randomUUID(),
                orderId,
                SourceType.POS,
                "POS01",
                Instant.now(),
                orderId,
                List.of(new OrderItemLine(1, "COM_GA", "Cơm gà", 2))
        );

        var result = syncService.processReserved(lateReserved);
        assertThat(result.outcome()).isEqualTo("TERMINAL_STATE_IGNORED");
        assertThat(getOrderStatus(orderId)).isEqualTo("DONE");
    }

    @Test
    void processFailedTransitionsPendingInventoryToFailedWithReason() {
        UUID orderId = UUID.randomUUID();
        createOrder(orderId, UUID.randomUUID(), "PENDING_INVENTORY");

        FailedEvent event = FailedEvent.v1(
                UUID.randomUUID(),
                orderId,
                SourceType.POS,
                "POS01",
                Instant.now(),
                FailureReasonCode.INSUFFICIENT_INGREDIENT,
                "Hết gà xé phay"
        );

        var result = syncService.processFailed(event);
        assertThat(result.outcome()).isEqualTo("SUCCESS");
        assertThat(getOrderStatus(orderId)).isEqualTo("FAILED");
        assertThat(getFailureReason(orderId)).isEqualTo("INSUFFICIENT_INGREDIENT: Hết gà xé phay");
    }

    @Test
    void processFailedIsTechnicallyDeduplicated() {
        UUID orderId = UUID.randomUUID();
        createOrder(orderId, UUID.randomUUID(), "PENDING_INVENTORY");

        FailedEvent event = FailedEvent.v1(
                UUID.randomUUID(),
                orderId,
                SourceType.POS,
                "POS01",
                Instant.now(),
                FailureReasonCode.INSUFFICIENT_INGREDIENT,
                "Hết gà xé phay"
        );

        var first = syncService.processFailed(event);
        assertThat(first.outcome()).isEqualTo("SUCCESS");

        var second = syncService.processFailed(event);
        assertThat(second.outcome()).isEqualTo("SKIPPED_DUPLICATE");
        assertThat(getOrderStatus(orderId)).isEqualTo("FAILED");
    }

    @Test
    void processDoneTransitionsReservedToDone() {
        UUID orderId = UUID.randomUUID();
        createOrder(orderId, UUID.randomUUID(), "RESERVED");

        DoneEvent event = DoneEvent.v1(
                UUID.randomUUID(),
                orderId,
                SourceType.POS,
                "POS01",
                Instant.now(),
                UUID.randomUUID(),
                Instant.now()
        );

        var result = syncService.processDone(event);
        assertThat(result.outcome()).isEqualTo("SUCCESS");
        assertThat(getOrderStatus(orderId)).isEqualTo("DONE");
    }

    @Test
    void outOfOrderDoneArrivingBeforeReservedConvergesToDone() {
        UUID orderId = UUID.randomUUID();
        createOrder(orderId, UUID.randomUUID(), "PENDING_INVENTORY");

        DoneEvent doneEvent = DoneEvent.v1(
                UUID.randomUUID(),
                orderId,
                SourceType.POS,
                "POS01",
                Instant.now(),
                UUID.randomUUID(),
                Instant.now()
        );

        var resultDone = syncService.processDone(doneEvent);
        assertThat(resultDone.outcome()).isEqualTo("SUCCESS");
        assertThat(getOrderStatus(orderId)).isEqualTo("DONE");

        // Later arrival of ReservedEvent must not revert DONE
        ReservedEvent lateReserved = ReservedEvent.v1(
                UUID.randomUUID(),
                orderId,
                SourceType.POS,
                "POS01",
                Instant.now(),
                orderId,
                List.of(new OrderItemLine(1, "COM_GA", "Cơm gà", 2))
        );
        var resultLateReserved = syncService.processReserved(lateReserved);
        assertThat(resultLateReserved.outcome()).isEqualTo("TERMINAL_STATE_IGNORED");
        assertThat(getOrderStatus(orderId)).isEqualTo("DONE");
    }

    @Test
    void orderNotFoundRecordsOutcomeAndDoesNotThrow() {
        UUID missingOrderId = UUID.randomUUID();
        ReservedEvent event = ReservedEvent.v1(
                UUID.randomUUID(),
                missingOrderId,
                SourceType.POS,
                "POS01",
                Instant.now(),
                missingOrderId,
                List.of(new OrderItemLine(1, "COM_GA", "Cơm gà", 2))
        );

        var result = syncService.processReserved(event);
        assertThat(result.outcome()).isEqualTo("ORDER_NOT_FOUND");

        Long count = jdbc.queryForObject(
                "SELECT count(*) FROM b4_order_test.processed_events WHERE event_id = ? AND outcome = 'ORDER_NOT_FOUND'",
                Long.class, event.eventId());
        assertThat(count).isEqualTo(1L);
    }

    @Test
    void concurrentUpdatesAreSafelySerialized() throws Exception {
        UUID orderId = UUID.randomUUID();
        createOrder(orderId, UUID.randomUUID(), "PENDING_INVENTORY");

        ReservedEvent event = ReservedEvent.v1(
                UUID.randomUUID(),
                orderId,
                SourceType.POS,
                "POS01",
                Instant.now(),
                orderId,
                List.of(new OrderItemLine(1, "COM_GA", "Cơm gà", 2))
        );

        int threads = 4;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);

        AtomicInteger successCount = new AtomicInteger();
        AtomicInteger duplicateCount = new AtomicInteger();

        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                ready.countDown();
                try {
                    start.await();
                    var res = syncService.processReserved(event);
                    if ("SUCCESS".equals(res.outcome())) {
                        successCount.incrementAndGet();
                    } else if ("SKIPPED_DUPLICATE".equals(res.outcome())) {
                        duplicateCount.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }

        ready.await(5, TimeUnit.SECONDS);
        start.countDown();
        done.await(10, TimeUnit.SECONDS);
        pool.shutdown();

        assertThat(successCount.get()).isEqualTo(1);
        assertThat(duplicateCount.get()).isEqualTo(3);
        assertThat(getOrderStatus(orderId)).isEqualTo("RESERVED");
    }

    @Test
    void failedEventDoesNotRevertReservedOrder() {
        UUID orderId = UUID.randomUUID();
        createOrder(orderId, UUID.randomUUID(), "RESERVED");

        FailedEvent event = FailedEvent.v1(
                UUID.randomUUID(),
                orderId,
                SourceType.POS,
                "POS01",
                Instant.now(),
                FailureReasonCode.INSUFFICIENT_INGREDIENT,
                "Hết gà xé phay"
        );

        var result = syncService.processFailed(event);
        assertThat(result.outcome()).isEqualTo("ILLEGAL_TRANSITION_IGNORED");
        assertThat(getOrderStatus(orderId)).isEqualTo("RESERVED");
        assertThat(getFailureReason(orderId)).isNull();
        assertThat(version(orderId)).isZero();
    }

    @Test
    void doneDoesNotOverrideFailedAndFailedDoesNotOverrideDone() {
        UUID failedOrderId = UUID.randomUUID();
        createOrder(failedOrderId, UUID.randomUUID(), "FAILED");
        DoneEvent lateDone = DoneEvent.v1(
                UUID.randomUUID(),
                failedOrderId,
                SourceType.POS,
                "POS01",
                Instant.now(),
                UUID.randomUUID(),
                Instant.now()
        );
        assertThat(syncService.processDone(lateDone).outcome()).isEqualTo("TERMINAL_STATE_IGNORED");
        assertThat(getOrderStatus(failedOrderId)).isEqualTo("FAILED");

        FailedEvent secondFailed = FailedEvent.v1(
                UUID.randomUUID(),
                failedOrderId,
                SourceType.POS,
                "POS01",
                Instant.now(),
                FailureReasonCode.INSUFFICIENT_INGREDIENT,
                "Vẫn hết nguyên liệu"
        );
        assertThat(syncService.processFailed(secondFailed).outcome()).isEqualTo("ALREADY_APPLIED");
        assertThat(getOrderStatus(failedOrderId)).isEqualTo("FAILED");

        UUID doneOrderId = UUID.randomUUID();
        createOrder(doneOrderId, UUID.randomUUID(), "DONE");
        FailedEvent lateFailed = FailedEvent.v1(
                UUID.randomUUID(),
                doneOrderId,
                SourceType.POS,
                "POS01",
                Instant.now(),
                FailureReasonCode.RECIPE_NOT_FOUND,
                "Không có công thức"
        );
        assertThat(syncService.processFailed(lateFailed).outcome()).isEqualTo("TERMINAL_STATE_IGNORED");
        assertThat(getOrderStatus(doneOrderId)).isEqualTo("DONE");
        assertThat(getFailureReason(doneOrderId)).isNull();
    }

    @Test
    void secondDoneEventIsAlreadyAppliedAndDoesNotChangeVersion() {
        UUID orderId = UUID.randomUUID();
        createOrder(orderId, UUID.randomUUID(), "RESERVED");
        DoneEvent first = DoneEvent.v1(
                UUID.randomUUID(), orderId, SourceType.POS, "POS01", Instant.now(), UUID.randomUUID(), Instant.now());
        DoneEvent second = DoneEvent.v1(
                UUID.randomUUID(), orderId, SourceType.POS, "POS01", Instant.now(), UUID.randomUUID(), Instant.now());

        assertThat(syncService.processDone(first).outcome()).isEqualTo("SUCCESS");
        assertThat(syncService.processDone(second).outcome()).isEqualTo("ALREADY_APPLIED");
        assertThat(getOrderStatus(orderId)).isEqualTo("DONE");
        assertThat(version(orderId)).isEqualTo(1L);
        assertThat(outcome(second.eventId())).isEqualTo("ALREADY_APPLIED");
    }

    @Test
    void invalidStatusEventsAreRejectedBeforeAnyWrite() {
        UUID orderId = UUID.randomUUID();
        createOrder(orderId, UUID.randomUUID(), "PENDING_INVENTORY");

        DoneEvent doneWithoutPayload = new DoneEvent(
                UUID.randomUUID(), 1, EventType.Done, orderId, SourceType.POS, "POS01", Instant.now(), null);
        FailedEvent failedWithoutPayload = new FailedEvent(
                UUID.randomUUID(), 1, EventType.Failed, orderId, SourceType.POS, "POS01", Instant.now(), null);
        ReservedEvent reservedWithoutItems = ReservedEvent.v1(
                UUID.randomUUID(), orderId, SourceType.POS, "POS01", Instant.now(), orderId, List.of());

        assertThatThrownBy(() -> syncService.processDone(doneWithoutPayload))
                .isInstanceOf(InvalidEventException.class);
        assertThatThrownBy(() -> syncService.processFailed(failedWithoutPayload))
                .isInstanceOf(InvalidEventException.class);
        assertThatThrownBy(() -> syncService.processReserved(reservedWithoutItems))
                .isInstanceOf(InvalidEventException.class);

        assertThat(getOrderStatus(orderId)).isEqualTo("PENDING_INVENTORY");
        assertThat(version(orderId)).isZero();
        assertThat(processedCount(orderId)).isZero();
    }
}
