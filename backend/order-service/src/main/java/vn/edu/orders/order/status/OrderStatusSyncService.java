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
import vn.edu.orders.contracts.events.DoneEvent;
import vn.edu.orders.contracts.events.FailedEvent;
import vn.edu.orders.contracts.events.ReservedEvent;
import vn.edu.orders.order.config.SiteProperties;
import vn.edu.orders.order.status.OrderStatusStore.OrderRecord;

@Service
public class OrderStatusSyncService {
    private static final Logger log = LoggerFactory.getLogger(OrderStatusSyncService.class);

    static final String SUCCESS = "SUCCESS";
    static final String ALREADY_APPLIED = "ALREADY_APPLIED";
    static final String SKIPPED_DUPLICATE = "SKIPPED_DUPLICATE";
    static final String ORDER_NOT_FOUND = "ORDER_NOT_FOUND";
    static final String TERMINAL_STATE_IGNORED = "TERMINAL_STATE_IGNORED";
    static final String ILLEGAL_TRANSITION_IGNORED = "ILLEGAL_TRANSITION_IGNORED";

    private final OrderStatusStore store;
    private final TransactionTemplate readCommittedTx;
    private final String siteId;
    private final String instanceId;

    public OrderStatusSyncService(
            OrderStatusStore store,
            PlatformTransactionManager transactionManager,
            SiteProperties siteProperties
    ) {
        this.store = store;
        this.readCommittedTx = new TransactionTemplate(transactionManager);
        this.readCommittedTx.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        this.siteId = siteProperties.id();
        this.instanceId = siteProperties.instanceId();
    }

    public record StatusSyncResult(UUID eventId, UUID orderId, String outcome) {}

    public StatusSyncResult processReserved(ReservedEvent event) {
        return processReserved(event, StatusEventContext.direct());
    }

    public StatusSyncResult processReserved(ReservedEvent event, StatusEventContext context) {
        StatusEventValidator.validateReserved(event);
        return inTransaction(event.eventId(), event.orderId(), event.eventType().name(), context, (order, now) -> {
            String current = order.status();
            if ("PENDING_INVENTORY".equals(current)) {
                return transitioned(event.eventId(), order.orderId(), event.eventType().name(),
                        current, "RESERVED", null, now, context);
            }
            if ("RESERVED".equals(current)) {
                return alreadyApplied(event.eventId(), order.orderId(), event.eventType().name(), current, context);
            }
            return ignored(event.eventId(), order.orderId(), event.eventType().name(),
                    current, TERMINAL_STATE_IGNORED, context);
        });
    }

    public StatusSyncResult processFailed(FailedEvent event) {
        return processFailed(event, StatusEventContext.direct());
    }

    public StatusSyncResult processFailed(FailedEvent event, StatusEventContext context) {
        StatusEventValidator.validateFailed(event);
        String failureReason = event.payload().reasonCode().name() + ": " + event.payload().reasonMessage();
        return inTransaction(event.eventId(), event.orderId(), event.eventType().name(), context, (order, now) -> {
            String current = order.status();
            if ("PENDING_INVENTORY".equals(current)) {
                return transitioned(event.eventId(), order.orderId(), event.eventType().name(),
                        current, "FAILED", failureReason, now, context);
            }
            if ("FAILED".equals(current)) {
                return alreadyApplied(event.eventId(), order.orderId(), event.eventType().name(), current, context);
            }
            String outcome = "RESERVED".equals(current) ? ILLEGAL_TRANSITION_IGNORED : TERMINAL_STATE_IGNORED;
            return ignored(event.eventId(), order.orderId(), event.eventType().name(), current, outcome, context);
        });
    }

    public StatusSyncResult processDone(DoneEvent event) {
        return processDone(event, StatusEventContext.direct());
    }

    public StatusSyncResult processDone(DoneEvent event, StatusEventContext context) {
        StatusEventValidator.validateDone(event);
        return inTransaction(event.eventId(), event.orderId(), event.eventType().name(), context, (order, now) -> {
            String current = order.status();
            if ("RESERVED".equals(current) || "PENDING_INVENTORY".equals(current)) {
                return transitioned(event.eventId(), order.orderId(), event.eventType().name(),
                        current, "DONE", null, now, context);
            }
            if ("DONE".equals(current)) {
                return alreadyApplied(event.eventId(), order.orderId(), event.eventType().name(), current, context);
            }
            return ignored(event.eventId(), order.orderId(), event.eventType().name(),
                    current, TERMINAL_STATE_IGNORED, context);
        });
    }

    private StatusSyncResult inTransaction(
            UUID eventId,
            UUID orderId,
            String eventType,
            StatusEventContext context,
            Transition transition
    ) {
        StatusSyncResult result = readCommittedTx.execute(status -> {
            OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
            if (!store.tryInsertProcessedEvent(eventId, orderId, eventType, now)) {
                log.info("Duplicate status event ignored service=order-service siteId={} instanceId={} orderId={} eventId={} eventType={} partition={} offset={} outcome={}",
                        siteId, instanceId, orderId, eventId, eventType, context.partition(), context.offset(), SKIPPED_DUPLICATE);
                return new StatusSyncResult(eventId, orderId, SKIPPED_DUPLICATE);
            }

            Optional<OrderRecord> orderOpt = store.lockOrderForUpdate(orderId);
            if (orderOpt.isEmpty()) {
                log.error("Order not found for status event service=order-service siteId={} instanceId={} orderId={} eventId={} eventType={} partition={} offset={} outcome={}",
                        siteId, instanceId, orderId, eventId, eventType, context.partition(), context.offset(), ORDER_NOT_FOUND);
                store.updateProcessedOutcome(eventId, ORDER_NOT_FOUND);
                return new StatusSyncResult(eventId, orderId, ORDER_NOT_FOUND);
            }
            return transition.apply(orderOpt.get(), now);
        });
        if (result == null) {
            throw new IllegalStateException("Status sync transaction returned null for eventId=" + eventId);
        }
        return result;
    }

    private StatusSyncResult transitioned(
            UUID eventId,
            UUID orderId,
            String eventType,
            String from,
            String to,
            String failureReason,
            OffsetDateTime now,
            StatusEventContext context
    ) {
        store.updateOrderStatus(orderId, to, failureReason, now);
        store.updateProcessedOutcome(eventId, SUCCESS);
        log.info("Order status transitioned service=order-service siteId={} instanceId={} orderId={} eventId={} eventType={} from={} to={} partition={} offset={} outcome={}",
                siteId, instanceId, orderId, eventId, eventType, from, to, context.partition(), context.offset(), SUCCESS);
        return new StatusSyncResult(eventId, orderId, SUCCESS);
    }

    private StatusSyncResult alreadyApplied(
            UUID eventId,
            UUID orderId,
            String eventType,
            String currentStatus,
            StatusEventContext context
    ) {
        store.updateProcessedOutcome(eventId, ALREADY_APPLIED);
        log.info("Status event already applied service=order-service siteId={} instanceId={} orderId={} eventId={} eventType={} currentStatus={} partition={} offset={} outcome={}",
                siteId, instanceId, orderId, eventId, eventType, currentStatus, context.partition(), context.offset(), ALREADY_APPLIED);
        return new StatusSyncResult(eventId, orderId, ALREADY_APPLIED);
    }

    private StatusSyncResult ignored(
            UUID eventId,
            UUID orderId,
            String eventType,
            String currentStatus,
            String outcome,
            StatusEventContext context
    ) {
        store.updateProcessedOutcome(eventId, outcome);
        log.warn("Status event ignored service=order-service siteId={} instanceId={} orderId={} eventId={} eventType={} currentStatus={} partition={} offset={} outcome={}",
                siteId, instanceId, orderId, eventId, eventType, currentStatus, context.partition(), context.offset(), outcome);
        return new StatusSyncResult(eventId, orderId, outcome);
    }

    @FunctionalInterface
    private interface Transition {
        StatusSyncResult apply(OrderRecord order, OffsetDateTime now);
    }
}
