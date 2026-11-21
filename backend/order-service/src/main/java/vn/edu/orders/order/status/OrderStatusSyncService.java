package vn.edu.orders.order.status;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import vn.edu.orders.contracts.events.CancelPendingPayload;
import vn.edu.orders.contracts.events.CancelRejectedPayload;
import vn.edu.orders.contracts.events.EventEnvelope;
import vn.edu.orders.contracts.events.EventType;
import vn.edu.orders.contracts.events.InvalidEventException;
import vn.edu.orders.contracts.events.OrderCancelledPayload;
import vn.edu.orders.contracts.events.OrderCompletedPayload;
import vn.edu.orders.contracts.events.OrderFailedPayload;
import vn.edu.orders.contracts.events.OrderReservedPayload;
import vn.edu.orders.contracts.events.OrderStatus;
import vn.edu.orders.order.config.MutableClock;
import vn.edu.orders.order.config.OrderLifecycleProperties;
import vn.edu.orders.order.config.SiteProperties;
import vn.edu.orders.order.status.OrderStatusStore.OrderSnapshot;

@Service
public class OrderStatusSyncService {
    private static final Logger log = LoggerFactory.getLogger(OrderStatusSyncService.class);

    public static final String SUCCESS = "SUCCESS";
    public static final String ALREADY_APPLIED = "ALREADY_APPLIED";
    public static final String SKIPPED_DUPLICATE = "SKIPPED_DUPLICATE";
    public static final String ORDER_NOT_FOUND = "ORDER_NOT_FOUND";
    public static final String TERMINAL_STATE_IGNORED = "TERMINAL_STATE_IGNORED";
    public static final String ILLEGAL_TRANSITION_IGNORED = "ILLEGAL_TRANSITION_IGNORED";
    public static final String CONDITION_NOT_MET = "CONDITION_NOT_MET";

    private final OrderStatusStore store;
    private final TransactionTemplate readCommittedTx;
    private final MutableClock clock;
    private final OrderLifecycleProperties properties;
    private final String siteId;
    private final String instanceId;

    public OrderStatusSyncService(
            OrderStatusStore store,
            PlatformTransactionManager transactionManager,
            MutableClock clock,
            OrderLifecycleProperties properties,
            SiteProperties siteProperties
    ) {
        this.store = store;
        this.readCommittedTx = new TransactionTemplate(transactionManager);
        this.readCommittedTx.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        this.clock = clock;
        this.properties = properties;
        this.siteId = siteProperties.id();
        this.instanceId = siteProperties.instanceId();
    }

    public record StatusSyncResult(UUID eventId, UUID orderId, String outcome) {
    }

    public StatusSyncResult processOrderReserved(EventEnvelope<OrderReservedPayload> event) {
        return processOrderReserved(event, StatusEventContext.direct());
    }

    public StatusSyncResult processOrderReserved(
            EventEnvelope<OrderReservedPayload> event,
            StatusEventContext context
    ) {
        validateEnvelope(event, EventType.OrderReserved);
        return applyRankStatus(event, OrderStatus.RESERVED, null, null, context);
    }

    public StatusSyncResult processOrderFailed(EventEnvelope<OrderFailedPayload> event) {
        return processOrderFailed(event, StatusEventContext.direct());
    }

    public StatusSyncResult processOrderFailed(
            EventEnvelope<OrderFailedPayload> event,
            StatusEventContext context
    ) {
        validateEnvelope(event, EventType.OrderFailed);
        OrderFailedPayload payload = event.payload();
        if (payload.reasonCode() == null) {
            throw new InvalidEventException("Missing reasonCode on OrderFailed", event.eventId(), event.orderId());
        }
        String closeReason = payload.reasonCode().name();
        String failureReason = payload.reasonMessage() != null && !payload.reasonMessage().isBlank()
                ? closeReason + ": " + payload.reasonMessage()
                : closeReason + ": " + payload.reasonCode().defaultMessage();
        return applyRankStatus(event, OrderStatus.FAILED, closeReason, failureReason, context);
    }

    public StatusSyncResult processOrderCancelled(EventEnvelope<OrderCancelledPayload> event) {
        return processOrderCancelled(event, StatusEventContext.direct());
    }

    public StatusSyncResult processOrderCancelled(
            EventEnvelope<OrderCancelledPayload> event,
            StatusEventContext context
    ) {
        validateEnvelope(event, EventType.OrderCancelled);
        OrderCancelledPayload payload = event.payload();
        if (payload.reasonCode() == null) {
            throw new InvalidEventException("Missing reasonCode on OrderCancelled", event.eventId(), event.orderId());
        }
        return applyRankStatus(event, OrderStatus.CANCELLED, payload.reasonCode().name(), null, context);
    }

    public StatusSyncResult processOrderCompleted(EventEnvelope<OrderCompletedPayload> event) {
        return processOrderCompleted(event, StatusEventContext.direct());
    }

    public StatusSyncResult processOrderCompleted(
            EventEnvelope<OrderCompletedPayload> event,
            StatusEventContext context
    ) {
        validateEnvelope(event, EventType.OrderCompleted);
        return applyRankStatus(event, OrderStatus.DONE, null, null, context);
    }

    public StatusSyncResult processCancelPending(EventEnvelope<CancelPendingPayload> event) {
        return processCancelPending(event, StatusEventContext.direct());
    }

    public StatusSyncResult processCancelPending(
            EventEnvelope<CancelPendingPayload> event,
            StatusEventContext context
    ) {
        validateEnvelope(event, EventType.CancelPending);
        return inTransaction(event.eventId(), event.orderId(), EventType.CancelPending.name(), context, now -> {
            int updated = store.applyCancelPending(event.orderId(), now);
            if (updated == 1) {
                store.updateProcessedOutcome(event.eventId(), SUCCESS);
                log.info("Applied CancelPending service=order-service siteId={} instanceId={} orderId={} eventId={} partition={} offset={}",
                        siteId, instanceId, event.orderId(), event.eventId(), context.partition(), context.offset());
                return new StatusSyncResult(event.eventId(), event.orderId(), SUCCESS);
            }
            Optional<OrderSnapshot> snapshot = store.findOrderSnapshot(event.orderId());
            if (snapshot.isEmpty()) {
                return notFound(event, context);
            }
            store.updateProcessedOutcome(event.eventId(), CONDITION_NOT_MET);
            log.info("Ignored CancelPending due to order state service=order-service siteId={} orderId={} eventId={} status={} cancelFlag={}",
                    siteId, event.orderId(), event.eventId(), snapshot.get().status(), snapshot.get().cancelFlag());
            return new StatusSyncResult(event.eventId(), event.orderId(), CONDITION_NOT_MET);
        });
    }

    public StatusSyncResult processCancelRejected(EventEnvelope<CancelRejectedPayload> event) {
        return processCancelRejected(event, StatusEventContext.direct());
    }

    public StatusSyncResult processCancelRejected(
            EventEnvelope<CancelRejectedPayload> event,
            StatusEventContext context
    ) {
        validateEnvelope(event, EventType.CancelRejected);
        CancelRejectedPayload payload = event.payload();
        String msg = (payload.message() != null && !payload.message().isBlank())
                ? payload.message()
                : properties.cancelMessageCooking();
        return inTransaction(event.eventId(), event.orderId(), EventType.CancelRejected.name(), context, now -> {
            int updated = store.applyCancelRejected(event.orderId(), msg, now);
            if (updated == 1) {
                store.updateProcessedOutcome(event.eventId(), SUCCESS);
                log.info("Applied CancelRejected service=order-service siteId={} instanceId={} orderId={} eventId={} partition={} offset={}",
                        siteId, instanceId, event.orderId(), event.eventId(), context.partition(), context.offset());
                return new StatusSyncResult(event.eventId(), event.orderId(), SUCCESS);
            }
            return notFound(event, context);
        });
    }

    private StatusSyncResult applyRankStatus(
            EventEnvelope<?> event,
            OrderStatus targetStatus,
            String closeReason,
            String failureReason,
            StatusEventContext context
    ) {
        String eventType = event.eventType().name();
        return inTransaction(event.eventId(), event.orderId(), eventType, context, now -> {
            int updated = store.applyRankBasedStatus(event.orderId(), targetStatus, closeReason, failureReason, now);
            if (updated == 1) {
                store.updateProcessedOutcome(event.eventId(), SUCCESS);
                log.info("Order status transitioned service=order-service siteId={} instanceId={} orderId={} eventId={} eventType={} to={} partition={} offset={}",
                        siteId, instanceId, event.orderId(), event.eventId(), eventType, targetStatus,
                        context.partition(), context.offset());
                return new StatusSyncResult(event.eventId(), event.orderId(), SUCCESS);
            }

            Optional<OrderSnapshot> snapshot = store.findOrderSnapshot(event.orderId());
            if (snapshot.isEmpty()) {
                return notFound(event, context);
            }
            OrderSnapshot current = snapshot.get();
            String outcome;
            if (targetStatus.name().equals(current.status())) {
                outcome = ALREADY_APPLIED;
            } else if (current.statusRank() >= OrderStatus.DONE.statusRank()) {
                outcome = TERMINAL_STATE_IGNORED;
            } else {
                outcome = ILLEGAL_TRANSITION_IGNORED;
            }
            store.updateProcessedOutcome(event.eventId(), outcome);
            log.info("Status event ignored by rank service=order-service siteId={} orderId={} eventId={} eventType={} currentStatus={} currentRank={} outcome={}",
                    siteId, event.orderId(), event.eventId(), eventType, current.status(), current.statusRank(), outcome);
            return new StatusSyncResult(event.eventId(), event.orderId(), outcome);
        });
    }

    private StatusSyncResult inTransaction(
            UUID eventId,
            UUID orderId,
            String eventType,
            StatusEventContext context,
            TransactionalAction action
    ) {
        StatusSyncResult result = readCommittedTx.execute(status -> {
            OffsetDateTime now = OffsetDateTime.ofInstant(clock.instant(), ZoneOffset.UTC);
            if (!store.tryInsertProcessedEvent(eventId, orderId, eventType, now)) {
                log.info("Duplicate status event ignored service=order-service siteId={} instanceId={} orderId={} eventId={} eventType={} partition={} offset={} outcome={}",
                        siteId, instanceId, orderId, eventId, eventType, context.partition(), context.offset(), SKIPPED_DUPLICATE);
                return new StatusSyncResult(eventId, orderId, SKIPPED_DUPLICATE);
            }
            return action.run(now);
        });
        if (result == null) {
            throw new IllegalStateException("Status sync transaction returned null for eventId=" + eventId);
        }
        return result;
    }

    private StatusSyncResult notFound(EventEnvelope<?> event, StatusEventContext context) {
        log.warn("Order not found for status event service=order-service siteId={} instanceId={} orderId={} eventId={} eventType={} partition={} offset={}",
                siteId, instanceId, event.orderId(), event.eventId(), event.eventType(),
                context.partition(), context.offset());
        store.updateProcessedOutcome(event.eventId(), ORDER_NOT_FOUND);
        return new StatusSyncResult(event.eventId(), event.orderId(), ORDER_NOT_FOUND);
    }

    private static void validateEnvelope(EventEnvelope<?> event, EventType expectedType) {
        if (event == null) {
            throw new InvalidEventException("Event envelope must not be null");
        }
        event.validateEnvelope();
        if (event.eventType() != expectedType) {
            throw new InvalidEventException(
                    "Expected eventType " + expectedType + " but got " + event.eventType(),
                    event.eventId(),
                    event.orderId()
            );
        }
    }

    @FunctionalInterface
    private interface TransactionalAction {
        StatusSyncResult run(OffsetDateTime now);
    }
}
