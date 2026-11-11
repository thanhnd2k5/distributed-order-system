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

@Service
public class OrderStatusSyncService {
    private static final Logger log = LoggerFactory.getLogger(OrderStatusSyncService.class);

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
        return readCommittedTx.execute(status -> {
            OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
            boolean inserted = store.tryInsertProcessedEvent(event.eventId(), event.orderId(), event.eventType().name(), now);
            if (!inserted) {
                log.info("Duplicate event ignored siteId={} instanceId={} eventId={} orderId={} eventType={}",
                        siteId, instanceId, event.eventId(), event.orderId(), event.eventType());
                return new StatusSyncResult(event.eventId(), event.orderId(), "SKIPPED_DUPLICATE");
            }

            Optional<OrderStatusStore.OrderRecord> orderOpt = store.lockOrderForUpdate(event.orderId());
            if (orderOpt.isEmpty()) {
                log.error("Order not found for status event siteId={} instanceId={} orderId={} eventId={} eventType={}",
                        siteId, instanceId, event.orderId(), event.eventId(), event.eventType());
                store.updateProcessedOutcome(event.eventId(), "ORDER_NOT_FOUND");
                return new StatusSyncResult(event.eventId(), event.orderId(), "ORDER_NOT_FOUND");
            }

            OrderStatusStore.OrderRecord order = orderOpt.get();
            String currentStatus = order.status();

            if ("PENDING_INVENTORY".equals(currentStatus)) {
                store.updateOrderStatus(order.orderId(), "RESERVED", null, now);
                store.updateProcessedOutcome(event.eventId(), "SUCCESS");
                log.info("Order status transitioned to RESERVED siteId={} instanceId={} orderId={} eventId={}",
                        siteId, instanceId, order.orderId(), event.eventId());
                return new StatusSyncResult(event.eventId(), event.orderId(), "SUCCESS");
            } else if ("RESERVED".equals(currentStatus)) {
                store.updateProcessedOutcome(event.eventId(), "SUCCESS");
                log.info("Order status already RESERVED siteId={} instanceId={} orderId={} eventId={}",
                        siteId, instanceId, order.orderId(), event.eventId());
                return new StatusSyncResult(event.eventId(), event.orderId(), "SUCCESS");
            } else if ("DONE".equals(currentStatus)) {
                log.info("Order already DONE, ignoring late ReservedEvent siteId={} instanceId={} orderId={} eventId={}",
                        siteId, instanceId, order.orderId(), event.eventId());
                store.updateProcessedOutcome(event.eventId(), "TERMINAL_STATE_IGNORED");
                return new StatusSyncResult(event.eventId(), event.orderId(), "TERMINAL_STATE_IGNORED");
            } else {
                log.warn("Order is in state {}, ignoring ReservedEvent siteId={} instanceId={} orderId={} eventId={}",
                        currentStatus, siteId, instanceId, order.orderId(), event.eventId());
                store.updateProcessedOutcome(event.eventId(), "TERMINAL_STATE_IGNORED");
                return new StatusSyncResult(event.eventId(), event.orderId(), "TERMINAL_STATE_IGNORED");
            }
        });
    }

    public StatusSyncResult processFailed(FailedEvent event) {
        return readCommittedTx.execute(status -> {
            OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
            boolean inserted = store.tryInsertProcessedEvent(event.eventId(), event.orderId(), event.eventType().name(), now);
            if (!inserted) {
                log.info("Duplicate event ignored siteId={} instanceId={} eventId={} orderId={} eventType={}",
                        siteId, instanceId, event.eventId(), event.orderId(), event.eventType());
                return new StatusSyncResult(event.eventId(), event.orderId(), "SKIPPED_DUPLICATE");
            }

            Optional<OrderStatusStore.OrderRecord> orderOpt = store.lockOrderForUpdate(event.orderId());
            if (orderOpt.isEmpty()) {
                log.error("Order not found for status event siteId={} instanceId={} orderId={} eventId={} eventType={}",
                        siteId, instanceId, event.orderId(), event.eventId(), event.eventType());
                store.updateProcessedOutcome(event.eventId(), "ORDER_NOT_FOUND");
                return new StatusSyncResult(event.eventId(), event.orderId(), "ORDER_NOT_FOUND");
            }

            OrderStatusStore.OrderRecord order = orderOpt.get();
            String currentStatus = order.status();
            String failureReason = event.payload().reasonCode().name() + ": " + event.payload().reasonMessage();

            if ("PENDING_INVENTORY".equals(currentStatus)) {
                store.updateOrderStatus(order.orderId(), "FAILED", failureReason, now);
                store.updateProcessedOutcome(event.eventId(), "SUCCESS");
                log.info("Order status transitioned to FAILED siteId={} instanceId={} orderId={} eventId={} reason={}",
                        siteId, instanceId, order.orderId(), event.eventId(), failureReason);
                return new StatusSyncResult(event.eventId(), event.orderId(), "SUCCESS");
            } else if ("FAILED".equals(currentStatus)) {
                store.updateProcessedOutcome(event.eventId(), "SUCCESS");
                log.info("Order status already FAILED siteId={} instanceId={} orderId={} eventId={}",
                        siteId, instanceId, order.orderId(), event.eventId());
                return new StatusSyncResult(event.eventId(), event.orderId(), "SUCCESS");
            } else {
                log.warn("Order is in state {}, ignoring FailedEvent siteId={} instanceId={} orderId={} eventId={}",
                        currentStatus, siteId, instanceId, order.orderId(), event.eventId());
                store.updateProcessedOutcome(event.eventId(), "TERMINAL_STATE_IGNORED");
                return new StatusSyncResult(event.eventId(), event.orderId(), "TERMINAL_STATE_IGNORED");
            }
        });
    }

    public StatusSyncResult processDone(DoneEvent event) {
        return readCommittedTx.execute(status -> {
            OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
            boolean inserted = store.tryInsertProcessedEvent(event.eventId(), event.orderId(), event.eventType().name(), now);
            if (!inserted) {
                log.info("Duplicate event ignored siteId={} instanceId={} eventId={} orderId={} eventType={}",
                        siteId, instanceId, event.eventId(), event.orderId(), event.eventType());
                return new StatusSyncResult(event.eventId(), event.orderId(), "SKIPPED_DUPLICATE");
            }

            Optional<OrderStatusStore.OrderRecord> orderOpt = store.lockOrderForUpdate(event.orderId());
            if (orderOpt.isEmpty()) {
                log.error("Order not found for status event siteId={} instanceId={} orderId={} eventId={} eventType={}",
                        siteId, instanceId, event.orderId(), event.eventId(), event.eventType());
                store.updateProcessedOutcome(event.eventId(), "ORDER_NOT_FOUND");
                return new StatusSyncResult(event.eventId(), event.orderId(), "ORDER_NOT_FOUND");
            }

            OrderStatusStore.OrderRecord order = orderOpt.get();
            String currentStatus = order.status();

            if ("RESERVED".equals(currentStatus)) {
                store.updateOrderStatus(order.orderId(), "DONE", null, now);
                store.updateProcessedOutcome(event.eventId(), "SUCCESS");
                log.info("Order status transitioned to DONE siteId={} instanceId={} orderId={} eventId={}",
                        siteId, instanceId, order.orderId(), event.eventId());
                return new StatusSyncResult(event.eventId(), event.orderId(), "SUCCESS");
            } else if ("PENDING_INVENTORY".equals(currentStatus)) {
                store.updateOrderStatus(order.orderId(), "DONE", null, now);
                store.updateProcessedOutcome(event.eventId(), "SUCCESS");
                log.info("Out-of-order Done arrived before Reserved for orderId={}, converged to DONE siteId={} instanceId={} eventId={}",
                        order.orderId(), siteId, instanceId, event.eventId());
                return new StatusSyncResult(event.eventId(), event.orderId(), "SUCCESS");
            } else if ("DONE".equals(currentStatus)) {
                store.updateProcessedOutcome(event.eventId(), "SUCCESS");
                log.info("Order status already DONE siteId={} instanceId={} orderId={} eventId={}",
                        siteId, instanceId, order.orderId(), event.eventId());
                return new StatusSyncResult(event.eventId(), event.orderId(), "SUCCESS");
            } else {
                log.warn("Order is in state {}, ignoring DoneEvent siteId={} instanceId={} orderId={} eventId={}",
                        currentStatus, siteId, instanceId, order.orderId(), event.eventId());
                store.updateProcessedOutcome(event.eventId(), "TERMINAL_STATE_IGNORED");
                return new StatusSyncResult(event.eventId(), event.orderId(), "TERMINAL_STATE_IGNORED");
            }
        });
    }
}
