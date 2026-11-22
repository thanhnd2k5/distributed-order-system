package vn.edu.orders.order.status;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
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
import vn.edu.orders.contracts.events.CancelPendingPayload;
import vn.edu.orders.contracts.events.CancelRejectedPayload;
import vn.edu.orders.contracts.events.CancelledBy;
import vn.edu.orders.contracts.events.EventEnvelope;
import vn.edu.orders.contracts.events.EventType;
import vn.edu.orders.contracts.events.InvalidEventException;
import vn.edu.orders.contracts.events.KitchenStatus;
import vn.edu.orders.contracts.events.OrderCancelledPayload;
import vn.edu.orders.contracts.events.OrderCompletedPayload;
import vn.edu.orders.contracts.events.OrderFailedPayload;
import vn.edu.orders.contracts.events.OrderReservedPayload;
import vn.edu.orders.contracts.events.ReasonCode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "outbox.enabled=false",
        "order.heartbeat-enabled=false",
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
        jdbc.update("DELETE FROM b4_order_test.alerts");
        jdbc.update("DELETE FROM b4_order_test.processed_events");
        jdbc.update("DELETE FROM b4_order_test.outbox_events");
        jdbc.update("DELETE FROM b4_order_test.order_items");
        jdbc.update("DELETE FROM b4_order_test.orders");
    }

    private void createOrder(UUID orderId, String status, int rank, String cancelFlag) {
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        jdbc.update("""
                INSERT INTO b4_order_test.orders (
                    order_id, client_request_id, site_id, source_type, status, status_rank,
                    cancel_flag, created_at, updated_at
                ) VALUES (?, ?, 'POS01', 'POS', ?, ?, ?, ?, ?)
                """, orderId, UUID.randomUUID(), status, rank, cancelFlag, now, now);
    }

    private String getOrderStatus(UUID orderId) {
        return jdbc.queryForObject("SELECT status FROM b4_order_test.orders WHERE order_id = ?",
                String.class, orderId);
    }

    private int getStatusRank(UUID orderId) {
        return jdbc.queryForObject("SELECT status_rank FROM b4_order_test.orders WHERE order_id = ?",
                Integer.class, orderId);
    }

    private String getCancelFlag(UUID orderId) {
        return jdbc.queryForObject("SELECT cancel_flag FROM b4_order_test.orders WHERE order_id = ?",
                String.class, orderId);
    }

    private String getCancelMessage(UUID orderId) {
        return jdbc.queryForObject("SELECT cancel_message FROM b4_order_test.orders WHERE order_id = ?",
                String.class, orderId);
    }

    private String getCloseReason(UUID orderId) {
        return jdbc.queryForObject("SELECT close_reason FROM b4_order_test.orders WHERE order_id = ?",
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
    void orderReservedTransitionsPendingInventoryToReservedAndPreservesCancelFlag() {
        UUID orderId = UUID.randomUUID();
        createOrder(orderId, "PENDING_INVENTORY", 0, "REQUESTED");

        var event = EventEnvelope.v1(
                UUID.randomUUID(),
                EventType.OrderReserved,
                orderId,
                "POS01",
                Instant.now(),
                OrderReservedPayload.forBranch()
        );

        var result = syncService.processOrderReserved(event);
        assertThat(result.outcome()).isEqualTo("SUCCESS");
        assertThat(getOrderStatus(orderId)).isEqualTo("RESERVED");
        assertThat(getStatusRank(orderId)).isEqualTo(1);
        assertThat(getCancelFlag(orderId)).isEqualTo("REQUESTED");
        assertThat(outcome(event.eventId())).isEqualTo("SUCCESS");
    }

    @Test
    void orderReservedIsDeduplicatedByEventIdAndRankCheckedAcrossNewEventIds() {
        UUID orderId = UUID.randomUUID();
        createOrder(orderId, "PENDING_INVENTORY", 0, "NONE");

        var event1 = EventEnvelope.v1(
                UUID.randomUUID(), EventType.OrderReserved, orderId, "POS01", Instant.now(),
                OrderReservedPayload.forBranch());
        var event2 = EventEnvelope.v1(
                UUID.randomUUID(), EventType.OrderReserved, orderId, "POS01", Instant.now(),
                OrderReservedPayload.forBranch());

        assertThat(syncService.processOrderReserved(event1).outcome()).isEqualTo("SUCCESS");
        assertThat(syncService.processOrderReserved(event1).outcome()).isEqualTo("SKIPPED_DUPLICATE");
        assertThat(syncService.processOrderReserved(event2).outcome()).isEqualTo("ALREADY_APPLIED");
        assertThat(getOrderStatus(orderId)).isEqualTo("RESERVED");
        assertThat(version(orderId)).isEqualTo(1L);
    }

    @Test
    void lateReservedDoesNotRevertTerminalDoneOrCancelled() {
        UUID doneId = UUID.randomUUID();
        createOrder(doneId, "DONE", 2, "NONE");

        var lateReserved = EventEnvelope.v1(
                UUID.randomUUID(), EventType.OrderReserved, doneId, "POS01", Instant.now(),
                OrderReservedPayload.forBranch());

        assertThat(syncService.processOrderReserved(lateReserved).outcome()).isEqualTo("TERMINAL_STATE_IGNORED");
        assertThat(getOrderStatus(doneId)).isEqualTo("DONE");
    }

    @Test
    void orderFailedClearsCancelFlagAndRecordsCloseReason() {
        UUID orderId = UUID.randomUUID();
        createOrder(orderId, "PENDING_INVENTORY", 0, "KITCHEN_WILL_NOT_COOK");

        var failed = EventEnvelope.v1(
                UUID.randomUUID(),
                EventType.OrderFailed,
                orderId,
                "POS01",
                Instant.now(),
                new OrderFailedPayload("POS01", ReasonCode.INSUFFICIENT_INGREDIENTS, "Hết gà")
        );

        assertThat(syncService.processOrderFailed(failed).outcome()).isEqualTo("SUCCESS");
        assertThat(getOrderStatus(orderId)).isEqualTo("FAILED");
        assertThat(getStatusRank(orderId)).isEqualTo(2);
        assertThat(getCloseReason(orderId)).isEqualTo("INSUFFICIENT_INGREDIENTS");
        assertThat(getCancelFlag(orderId)).isEqualTo("NONE");
    }

    @Test
    void cancelPendingAppliesOnlyWhenNonTerminalAndCancelFlagRequested() {
        UUID activeId = UUID.randomUUID();
        createOrder(activeId, "PENDING_INVENTORY", 0, "REQUESTED");
        var pending1 = EventEnvelope.v1(
                UUID.randomUUID(), EventType.CancelPending, activeId, "POS01", Instant.now(),
                CancelPendingPayload.empty());
        assertThat(syncService.processCancelPending(pending1).outcome()).isEqualTo("SUCCESS");
        assertThat(getCancelFlag(activeId)).isEqualTo("KITCHEN_WILL_NOT_COOK");

        // Arriving after OrderFailed (rank 2) must NOT turn cancel_flag back on (KB-8 race)
        UUID failedId = UUID.randomUUID();
        createOrder(failedId, "FAILED", 2, "NONE");
        var pendingLate = EventEnvelope.v1(
                UUID.randomUUID(), EventType.CancelPending, failedId, "POS01", Instant.now(),
                CancelPendingPayload.empty());
        assertThat(syncService.processCancelPending(pendingLate).outcome()).isEqualTo("CONDITION_NOT_MET");
        assertThat(getCancelFlag(failedId)).isEqualTo("NONE");
    }

    @Test
    void cancelRejectedAppliesAtAnyRankIncludingDone() {
        UUID doneId = UUID.randomUUID();
        createOrder(doneId, "DONE", 2, "NONE");

        var rejected = EventEnvelope.v1(
                UUID.randomUUID(),
                EventType.CancelRejected,
                doneId,
                "POS01",
                Instant.now(),
                new CancelRejectedPayload(KitchenStatus.DONE, "Cannot cancel: the kitchen has started cooking")
        );

        assertThat(syncService.processCancelRejected(rejected).outcome()).isEqualTo("SUCCESS");
        assertThat(getOrderStatus(doneId)).isEqualTo("DONE");
        assertThat(getCancelFlag(doneId)).isEqualTo("NONE");
        assertThat(getCancelMessage(doneId)).isEqualTo("Cannot cancel: the kitchen has started cooking");
    }

    @Test
    void outOfOrderCompletedBeforeReservedConvergesToDone() {
        UUID orderId = UUID.randomUUID();
        createOrder(orderId, "PENDING_INVENTORY", 0, "NONE");

        var completed = EventEnvelope.v1(
                UUID.randomUUID(), EventType.OrderCompleted, orderId, "POS01", Instant.now(),
                OrderCompletedPayload.empty());
        assertThat(syncService.processOrderCompleted(completed).outcome()).isEqualTo("SUCCESS");
        assertThat(getOrderStatus(orderId)).isEqualTo("DONE");
        assertThat(getStatusRank(orderId)).isEqualTo(2);

        var lateReserved = EventEnvelope.v1(
                UUID.randomUUID(), EventType.OrderReserved, orderId, "POS01", Instant.now(),
                OrderReservedPayload.forBranch());
        assertThat(syncService.processOrderReserved(lateReserved).outcome()).isEqualTo("TERMINAL_STATE_IGNORED");
        assertThat(getOrderStatus(orderId)).isEqualTo("DONE");
    }

    @Test
    void orderCancelledFromKitchenClearsCancelFlagAndRecordsReason() {
        UUID orderId = UUID.randomUUID();
        createOrder(orderId, "RESERVED", 1, "REQUESTED");

        var cancelled = EventEnvelope.v1(
                UUID.randomUUID(),
                EventType.OrderCancelled,
                orderId,
                "POS01",
                Instant.now(),
                new OrderCancelledPayload(CancelledBy.KITCHEN, ReasonCode.SPOILED_INGREDIENT)
        );

        assertThat(syncService.processOrderCancelled(cancelled).outcome()).isEqualTo("SUCCESS");
        assertThat(getOrderStatus(orderId)).isEqualTo("CANCELLED");
        assertThat(getStatusRank(orderId)).isEqualTo(2);
        assertThat(getCloseReason(orderId)).isEqualTo("SPOILED_INGREDIENT");
        assertThat(getCancelFlag(orderId)).isEqualTo("NONE");
    }

    @Test
    void orderNotFoundRecordsOutcomeWithoutThrowing() {
        UUID missingOrderId = UUID.randomUUID();
        var event = EventEnvelope.v1(
                UUID.randomUUID(), EventType.OrderReserved, missingOrderId, "POS01", Instant.now(),
                OrderReservedPayload.forBranch());

        assertThat(syncService.processOrderReserved(event).outcome()).isEqualTo("ORDER_NOT_FOUND");
        assertThat(outcome(event.eventId())).isEqualTo("ORDER_NOT_FOUND");
    }

    @Test
    void concurrentDuplicateStatusEventsAreSafelySerialized() throws Exception {
        UUID orderId = UUID.randomUUID();
        createOrder(orderId, "PENDING_INVENTORY", 0, "NONE");

        var event = EventEnvelope.v1(
                UUID.randomUUID(), EventType.OrderReserved, orderId, "POS01", Instant.now(),
                OrderReservedPayload.forBranch());

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
                    var res = syncService.processOrderReserved(event);
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
    void invalidStatusEventsAreRejectedBeforeAnyWrite() {
        UUID orderId = UUID.randomUUID();
        createOrder(orderId, "PENDING_INVENTORY", 0, "NONE");

        EventEnvelope<OrderCompletedPayload> completedNullPayload = EventEnvelope.v1(
                UUID.randomUUID(), EventType.OrderCompleted, orderId, "POS01", Instant.now(), null);
        EventEnvelope<OrderFailedPayload> failedNullReason = EventEnvelope.v1(
                UUID.randomUUID(), EventType.OrderFailed, orderId, "POS01", Instant.now(),
                new OrderFailedPayload("POS01", null, "msg"));

        assertThatThrownBy(() -> syncService.processOrderCompleted(completedNullPayload))
                .isInstanceOf(InvalidEventException.class);
        assertThatThrownBy(() -> syncService.processOrderFailed(failedNullReason))
                .isInstanceOf(InvalidEventException.class);

        assertThat(getOrderStatus(orderId)).isEqualTo("PENDING_INVENTORY");
        assertThat(version(orderId)).isZero();
        assertThat(processedCount(orderId)).isZero();
    }
}
