package vn.edu.orders.kitchen.application;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.server.ResponseStatusException;
import tools.jackson.databind.ObjectMapper;
import vn.edu.orders.contracts.events.DoneEvent;
import vn.edu.orders.contracts.events.OrderItemLine;
import vn.edu.orders.contracts.events.ReservedEvent;
import vn.edu.orders.contracts.events.SourceType;
import vn.edu.orders.kitchen.api.KitchenTicketResponse;
import vn.edu.orders.kitchen.application.TicketProcessingResult.Outcome;
import vn.edu.orders.kitchen.config.KitchenProperties;
import vn.edu.orders.kitchen.infrastructure.KitchenTicketStore;
import vn.edu.orders.kitchen.infrastructure.KitchenTicketStore.TicketRecord;

@Service
public class KitchenTicketService {
    private static final Logger log = LoggerFactory.getLogger(KitchenTicketService.class);

    private final KitchenTicketStore store;
    private final TransactionTemplate readCommittedTx;
    private final ObjectMapper json;
    private final String instanceId;

    public KitchenTicketService(
            KitchenTicketStore store,
            PlatformTransactionManager transactionManager,
            ObjectMapper json,
            KitchenProperties properties
    ) {
        this.store = store;
        this.readCommittedTx = new TransactionTemplate(transactionManager);
        this.readCommittedTx.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        this.json = json;
        this.instanceId = properties.instanceId();
    }

    public TicketProcessingResult processReserved(ReservedEvent event, EventProcessingContext context) {
        ReservedEventValidator.validate(event);
        String fingerprint = ReservedEventValidator.fingerprint(event);

        TicketProcessingResult result = readCommittedTx.execute(status -> applyInTransaction(event, fingerprint, context));
        if (result == null) {
            throw new IllegalStateException("Ticket processing transaction returned null for orderId=" + event.orderId());
        }
        return result;
    }

    private TicketProcessingResult applyInTransaction(
            ReservedEvent event,
            String fingerprint,
            EventProcessingContext context
    ) {
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.MICROS);
        UUID eventId = event.eventId();
        UUID orderId = event.orderId();

        // 1. Technical deduplication by event_id
        if (!store.tryInsertProcessedEvent(eventId, orderId, event.eventType().name(), fingerprint, now)) {
            var existing = store.findTicketRecordByOrderId(orderId);
            String status = existing.map(TicketRecord::status).orElse(null);
            UUID ticketId = existing.map(TicketRecord::ticketId).orElse(null);
            log.info("Kitchen duplicate event skipped instanceId={} orderId={} eventId={} partition={} offset={} outcome=DUPLICATE_EVENT existingStatus={}",
                    instanceId, orderId, eventId, context.partition(), context.offset(), status);
            return new TicketProcessingResult(eventId, orderId, ticketId, Outcome.DUPLICATE_EVENT, status);
        }

        // 2. Business idempotency by order_id
        UUID newTicketId = UUID.randomUUID();
        if (!store.tryInsertWaitingTicket(newTicketId, orderId, event.sourceType(), event.sourceSiteId(), fingerprint, now)) {
            TicketRecord existing = store.findTicketRecordByOrderId(orderId)
                    .orElseThrow(() -> new IllegalStateException("Ticket disappeared after conflict for orderId=" + orderId));
            if (existing.requestFingerprint().equals(fingerprint)) {
                store.updateProcessedOutcome(eventId, Outcome.DUPLICATE_ORDER.name());
                log.info("Kitchen duplicate orderId skipped instanceId={} orderId={} eventId={} ticketId={} partition={} offset={} outcome=DUPLICATE_ORDER existingStatus={}",
                        instanceId, orderId, eventId, existing.ticketId(), context.partition(), context.offset(), existing.status());
                return new TicketProcessingResult(eventId, orderId, existing.ticketId(), Outcome.DUPLICATE_ORDER, existing.status());
            }
            store.updateProcessedOutcome(eventId, Outcome.PAYLOAD_CONFLICT.name());
            log.error("Kitchen payload conflict detected instanceId={} orderId={} eventId={} ticketId={} partition={} offset={} outcome=PAYLOAD_CONFLICT existingStatus={} existingFingerprint={} incomingFingerprint={}",
                    instanceId, orderId, eventId, existing.ticketId(), context.partition(), context.offset(),
                    existing.status(), existing.requestFingerprint(), fingerprint);
            return new TicketProcessingResult(eventId, orderId, existing.ticketId(), Outcome.PAYLOAD_CONFLICT, existing.status());
        }

        for (OrderItemLine item : event.payload().items()) {
            store.insertTicketItem(newTicketId, item.lineNo(), item.dishId(), item.dishName(), item.quantity());
        }

        log.info("Kitchen created ticket instanceId={} orderId={} eventId={} ticketId={} partition={} offset={} outcome=APPLIED status=WAITING",
                instanceId, orderId, eventId, newTicketId, context.partition(), context.offset());
        return new TicketProcessingResult(eventId, orderId, newTicketId, Outcome.APPLIED, "WAITING");
    }

    public KitchenTicketResponse completeTicket(UUID ticketId) {
        KitchenTicketResponse response = readCommittedTx.execute(status -> {
            OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.MICROS);
            TicketRecord record = store.lockTicketForUpdate(ticketId)
                    .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Ticket not found: " + ticketId));

            if ("DONE".equals(record.status())) {
                log.info("Kitchen completeTicket already DONE (idempotent return) instanceId={} ticketId={} orderId={}",
                        instanceId, ticketId, record.orderId());
                return store.findTicketById(ticketId)
                        .orElseThrow(() -> new IllegalStateException("Ticket disappeared: " + ticketId));
            }

            store.markTicketDone(ticketId, now);

            UUID outboxEventId = UUID.randomUUID();
            DoneEvent doneEvent = DoneEvent.v1(
                    outboxEventId,
                    record.orderId(),
                    SourceType.valueOf(record.sourceType()),
                    record.sourceSiteId(),
                    now.toInstant(),
                    ticketId,
                    now.toInstant()
            );

            String payloadJson = json.writeValueAsString(doneEvent);
            store.insertOutboxDoneEvent(outboxEventId, ticketId, record.orderId(), payloadJson, now);

            log.info("Kitchen ticket marked DONE instanceId={} ticketId={} orderId={} outboxEventId={}",
                    instanceId, ticketId, record.orderId(), outboxEventId);

            return store.findTicketById(ticketId)
                    .orElseThrow(() -> new IllegalStateException("Ticket disappeared: " + ticketId));
        });

        if (response == null) {
            throw new IllegalStateException("completeTicket transaction returned null for ticketId=" + ticketId);
        }
        return response;
    }
}
