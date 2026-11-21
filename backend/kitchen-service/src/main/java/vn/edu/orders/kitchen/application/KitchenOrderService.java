package vn.edu.orders.kitchen.application;

import java.time.Clock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import vn.edu.orders.contracts.events.AlertKind;
import vn.edu.orders.contracts.events.CancelPendingPayload;
import vn.edu.orders.contracts.events.CancelRejectedPayload;
import vn.edu.orders.contracts.events.CancelRequestedPayload;
import vn.edu.orders.contracts.events.CancelledBy;
import vn.edu.orders.contracts.events.EventEnvelope;
import vn.edu.orders.contracts.events.EventTopics;
import vn.edu.orders.contracts.events.EventType;
import vn.edu.orders.contracts.events.IngredientCheckChangedPayload;
import vn.edu.orders.contracts.events.IngredientQuantity;
import vn.edu.orders.contracts.events.KitchenStatus;
import vn.edu.orders.contracts.events.OrderCancelledPayload;
import vn.edu.orders.contracts.events.OrderCompletedPayload;
import vn.edu.orders.contracts.events.OrderFailedPayload;
import vn.edu.orders.contracts.events.OrderItemLine;
import vn.edu.orders.contracts.events.OrderReservedPayload;
import vn.edu.orders.contracts.events.ReasonCode;
import vn.edu.orders.contracts.events.ReleaseRequestedPayload;
import vn.edu.orders.contracts.events.StockConsumedPayload;
import vn.edu.orders.kitchen.api.KitchenAlertResponse;
import vn.edu.orders.kitchen.api.KitchenTicketResponse;
import vn.edu.orders.kitchen.api.RejectTicketRequest;
import vn.edu.orders.kitchen.application.KitchenProcessingResult.Outcome;
import vn.edu.orders.kitchen.config.KitchenProperties;
import vn.edu.orders.kitchen.infrastructure.KitchenOrderStore;
import vn.edu.orders.kitchen.infrastructure.KitchenOrderStore.AlertRow;
import vn.edu.orders.kitchen.infrastructure.KitchenOrderStore.OrderRow;
import vn.edu.orders.kitchen.infrastructure.KitchenOrderStore.TicketViewRow;

@Service
public class KitchenOrderService {
    private static final Logger log = LoggerFactory.getLogger(KitchenOrderService.class);
    private static final String CANCEL_REJECTED_MESSAGE = "Cannot cancel: the kitchen has started cooking";

    private final KitchenOrderStore store;
    private final TransactionTemplate readCommittedTx;
    private final ObjectMapper json;
    private final Clock clock;
    private final String instanceId;

    public KitchenOrderService(
            KitchenOrderStore store,
            PlatformTransactionManager transactionManager,
            ObjectMapper json,
            Clock clock,
            KitchenProperties properties
    ) {
        this.store = store;
        this.readCommittedTx = new TransactionTemplate(transactionManager);
        this.readCommittedTx.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        this.json = json;
        this.clock = clock;
        this.instanceId = properties.instanceId();
    }

    public KitchenProcessingResult processOrderReserved(
            EventEnvelope<OrderReservedPayload> envelope,
            EventProcessingContext context
    ) {
        KitchenEventValidator.validateOrderReserved(envelope);
        KitchenProcessingResult result = readCommittedTx.execute(txStatus -> {
            OffsetDateTime now = nowUtc();
            UUID eventId = envelope.eventId();
            UUID orderId = envelope.orderId();

            if (!store.tryInsertProcessedEvent(eventId, orderId, EventType.OrderReserved.name(), "APPLIED", now)) {
                txStatus.setRollbackOnly();
                log.info("Kitchen duplicate event skipped instanceId={} orderId={} eventId={} topic={} partition={} offset={} outcome=DUPLICATE_EVENT",
                        instanceId, orderId, eventId, context.sourceTopic(), context.partition(), context.offset());
                return new KitchenProcessingResult(eventId, orderId, Outcome.DUPLICATE_EVENT, null);
            }

            OrderRow current = store.ensureAndLockOrder(orderId);
            KitchenStatus status = KitchenStatus.valueOf(current.status());
            OrderReservedPayload payload = envelope.payload();
            String originBranch = payload.originBranch().trim();
            String itemsJson = writeJson(payload.items());
            String reservedJson = writeJson(payload.reserved());
            OffsetDateTime orderCreatedAt = OffsetDateTime.ofInstant(payload.createdAt(), ZoneOffset.UTC);

            switch (status) {
                case NOT_EXISTS -> {
                    store.transitionNotExistsToQueued(orderId, originBranch, itemsJson, reservedJson, orderCreatedAt, now);
                    log.info("Kitchen queued order instanceId={} orderId={} eventId={} originBranch={} outcome=APPLIED status=QUEUED",
                            instanceId, orderId, eventId, originBranch);
                    return new KitchenProcessingResult(eventId, orderId, Outcome.APPLIED, KitchenStatus.QUEUED);
                }
                case CANCEL_PENDING -> {
                    Map<String, String> cancelReasons = parseStringMap(current.cancelReasonsJson());
                    if (current.cancelBranches().contains(originBranch)) {
                        String reasonStr = cancelReasons.get(originBranch);
                        if (reasonStr == null) {
                            throw new IllegalStateException(
                                    "Missing cancel reason for originBranch=" + originBranch + " orderId=" + orderId);
                        }
                        ReasonCode reasonCode = ReasonCode.valueOf(reasonStr);
                        store.transitionCancelPendingToCancelled(
                                orderId, originBranch, reasonCode.name(), itemsJson, reservedJson, orderCreatedAt, now);
                        emitOrderCancelled(orderId, originBranch, CancelledBy.CUSTOMER, reasonCode, eventId, now);
                        emitReleaseRequested(orderId, originBranch, CancelledBy.CUSTOMER, reasonCode, List.of(), eventId, now);
                        log.info("Kitchen cancelled pending order on arrival instanceId={} orderId={} eventId={} originBranch={} reason={} outcome=APPLIED status=CANCELLED",
                                instanceId, orderId, eventId, originBranch, reasonCode);
                        return new KitchenProcessingResult(eventId, orderId, Outcome.APPLIED, KitchenStatus.CANCELLED);
                    } else {
                        store.transitionCancelPendingToQueuedDismissingWrongBranches(
                                orderId, originBranch, itemsJson, reservedJson, orderCreatedAt, now);
                        Map<String, Object> detail = new LinkedHashMap<>();
                        detail.put("originBranch", originBranch);
                        detail.put("dismissedBranches", current.cancelBranches());
                        detail.put("dismissedReasons", cancelReasons);
                        store.insertAlert(
                                AlertKind.WRONG_BRANCH_CANCEL.name(),
                                orderId,
                                eventId,
                                context.sourceTopic(),
                                context.partition(),
                                context.offset(),
                                writeJson(detail),
                                now);
                        log.warn("Kitchen dismissed wrong-branch cancel requests on OrderReserved instanceId={} orderId={} eventId={} originBranch={} dismissedBranches={}",
                                instanceId, orderId, eventId, originBranch, current.cancelBranches());
                        return new KitchenProcessingResult(eventId, orderId, Outcome.APPLIED, KitchenStatus.QUEUED);
                    }
                }
                case QUEUED, COOKING -> {
                    store.updateProcessedOutcome(eventId, Outcome.DUPLICATE_ORDER.name());
                    log.info("Kitchen ignored duplicate OrderReserved instanceId={} orderId={} eventId={} status={}",
                            instanceId, orderId, eventId, status);
                    return new KitchenProcessingResult(eventId, orderId, Outcome.DUPLICATE_ORDER, status);
                }
                case DONE, CANCELLED, FAILED -> {
                    reemitStoredTerminalResult(current, eventId, now);
                    store.updateProcessedOutcome(eventId, Outcome.STORED_RESULT.name());
                    log.info("Kitchen re-emitted stored result for duplicate OrderReserved instanceId={} orderId={} eventId={} status={}",
                            instanceId, orderId, eventId, status);
                    return new KitchenProcessingResult(eventId, orderId, Outcome.STORED_RESULT, status);
                }
            }
            throw new IllegalStateException("Unhandled KitchenStatus: " + status);
        });
        return requireNonNullResult(result, envelope.orderId());
    }

    public KitchenProcessingResult processOrderFailed(
            EventEnvelope<OrderFailedPayload> envelope,
            EventProcessingContext context
    ) {
        KitchenEventValidator.validateOrderFailed(envelope);
        KitchenProcessingResult result = readCommittedTx.execute(txStatus -> {
            OffsetDateTime now = nowUtc();
            UUID eventId = envelope.eventId();
            UUID orderId = envelope.orderId();

            if (!store.tryInsertProcessedEvent(eventId, orderId, EventType.OrderFailed.name(), "APPLIED", now)) {
                txStatus.setRollbackOnly();
                log.info("Kitchen duplicate OrderFailed skipped instanceId={} orderId={} eventId={} outcome=DUPLICATE_EVENT",
                        instanceId, orderId, eventId);
                return new KitchenProcessingResult(eventId, orderId, Outcome.DUPLICATE_EVENT, null);
            }

            OrderRow current = store.ensureAndLockOrder(orderId);
            KitchenStatus status = KitchenStatus.valueOf(current.status());
            OrderFailedPayload payload = envelope.payload();
            String originBranch = payload.originBranch().trim();
            ReasonCode reasonCode = payload.reasonCode();

            switch (status) {
                case NOT_EXISTS, CANCEL_PENDING -> {
                    store.transitionToFailed(orderId, status.name(), originBranch, reasonCode.name(), now);
                    log.info("Kitchen recorded OrderFailed instanceId={} orderId={} eventId={} fromStatus={} originBranch={} reason={} status=FAILED",
                            instanceId, orderId, eventId, status, originBranch, reasonCode);
                    return new KitchenProcessingResult(eventId, orderId, Outcome.APPLIED, KitchenStatus.FAILED);
                }
                case FAILED, CANCELLED, DONE -> {
                    reemitStoredTerminalResult(current, eventId, now);
                    store.updateProcessedOutcome(eventId, Outcome.STORED_RESULT.name());
                    log.info("Kitchen re-emitted stored result for duplicate OrderFailed instanceId={} orderId={} eventId={} status={}",
                            instanceId, orderId, eventId, status);
                    return new KitchenProcessingResult(eventId, orderId, Outcome.STORED_RESULT, status);
                }
                case QUEUED, COOKING -> {
                    store.updateProcessedOutcome(eventId, Outcome.IGNORED_INVALID_TRANSITION.name());
                    log.error("Kitchen ignored unexpected OrderFailed on active ticket instanceId={} orderId={} eventId={} status={}",
                            instanceId, orderId, eventId, status);
                    return new KitchenProcessingResult(eventId, orderId, Outcome.IGNORED_INVALID_TRANSITION, status);
                }
            }
            throw new IllegalStateException("Unhandled KitchenStatus: " + status);
        });
        return requireNonNullResult(result, envelope.orderId());
    }

    public KitchenProcessingResult processCancelRequested(
            String sourceTopic,
            EventEnvelope<CancelRequestedPayload> envelope,
            EventProcessingContext context
    ) {
        String requestingBranch = KitchenEventValidator.validateCancelRequested(sourceTopic, envelope);
        KitchenProcessingResult result = readCommittedTx.execute(txStatus -> {
            OffsetDateTime now = nowUtc();
            UUID eventId = envelope.eventId();
            UUID orderId = envelope.orderId();

            if (!store.tryInsertProcessedEvent(eventId, orderId, EventType.CancelRequested.name(), "APPLIED", now)) {
                txStatus.setRollbackOnly();
                log.info("Kitchen duplicate CancelRequested skipped instanceId={} orderId={} eventId={} outcome=DUPLICATE_EVENT",
                        instanceId, orderId, eventId);
                return new KitchenProcessingResult(eventId, orderId, Outcome.DUPLICATE_EVENT, null);
            }

            OrderRow current = store.ensureAndLockOrder(orderId);
            KitchenStatus status = KitchenStatus.valueOf(current.status());
            ReasonCode reasonCode = envelope.payload().reasonCode();

            // Precedence rule 1: if originBranch is known and differs from requestingBranch, ignore + alert.
            if (current.originBranch() != null && !current.originBranch().equals(requestingBranch)) {
                Map<String, Object> detail = new LinkedHashMap<>();
                detail.put("requestingBranch", requestingBranch);
                detail.put("originBranch", current.originBranch());
                detail.put("status", status.name());
                detail.put("reasonCode", reasonCode.name());
                store.insertAlert(
                        AlertKind.WRONG_BRANCH_CANCEL.name(),
                        orderId,
                        eventId,
                        sourceTopic,
                        context.partition(),
                        context.offset(),
                        writeJson(detail),
                        now);
                store.updateProcessedOutcome(eventId, Outcome.IGNORED_WRONG_BRANCH.name());
                log.warn("Kitchen ignored wrong-branch CancelRequested instanceId={} orderId={} eventId={} requestingBranch={} originBranch={} status={}",
                        instanceId, orderId, eventId, requestingBranch, current.originBranch(), status);
                return new KitchenProcessingResult(eventId, orderId, Outcome.IGNORED_WRONG_BRANCH, status);
            }

            switch (status) {
                case NOT_EXISTS -> {
                    store.transitionNotExistsToCancelPending(orderId, requestingBranch, reasonCode.name(), now);
                    emitCancelPending(orderId, requestingBranch, eventId, now);
                    log.info("Kitchen created CANCEL_PENDING instanceId={} orderId={} eventId={} requestingBranch={} reason={}",
                            instanceId, orderId, eventId, requestingBranch, reasonCode);
                    return new KitchenProcessingResult(eventId, orderId, Outcome.APPLIED, KitchenStatus.CANCEL_PENDING);
                }
                case CANCEL_PENDING -> {
                    boolean alreadyRequested = current.cancelBranches().contains(requestingBranch);
                    if (!alreadyRequested) {
                        store.addCancelBranchToPending(orderId, requestingBranch, reasonCode.name(), now);
                        emitCancelPending(orderId, requestingBranch, eventId, now);
                        log.info("Kitchen added cancel branch to CANCEL_PENDING instanceId={} orderId={} eventId={} requestingBranch={}",
                                instanceId, orderId, eventId, requestingBranch);
                        return new KitchenProcessingResult(eventId, orderId, Outcome.APPLIED, KitchenStatus.CANCEL_PENDING);
                    } else {
                        // Precedence rule 3: repeat cancel from same branch keeps earlier reason & re-emits CancelPending
                        emitCancelPending(orderId, requestingBranch, eventId, now);
                        store.updateProcessedOutcome(eventId, Outcome.STORED_RESULT.name());
                        log.info("Kitchen re-emitted CancelPending for duplicate branch cancel instanceId={} orderId={} eventId={} requestingBranch={}",
                                instanceId, orderId, eventId, requestingBranch);
                        return new KitchenProcessingResult(eventId, orderId, Outcome.STORED_RESULT, KitchenStatus.CANCEL_PENDING);
                    }
                }
                case QUEUED -> {
                    String originBranch = current.originBranch();
                    store.transitionQueuedToCustomerCancelled(orderId, reasonCode.name(), now);
                    emitOrderCancelled(orderId, originBranch, CancelledBy.CUSTOMER, reasonCode, eventId, now);
                    emitReleaseRequested(orderId, originBranch, CancelledBy.CUSTOMER, reasonCode, List.of(), eventId, now);
                    log.info("Kitchen cancelled QUEUED ticket on customer request instanceId={} orderId={} eventId={} originBranch={} reason={}",
                            instanceId, orderId, eventId, originBranch, reasonCode);
                    return new KitchenProcessingResult(eventId, orderId, Outcome.APPLIED, KitchenStatus.CANCELLED);
                }
                case COOKING, DONE -> {
                    // Precedence rule 2: explicit row emits CancelRejected even on DONE!
                    emitCancelRejected(orderId, current.originBranch(), status, eventId, now);
                    store.updateProcessedOutcome(eventId, Outcome.CANCEL_REJECTED.name());
                    log.info("Kitchen rejected CancelRequested instanceId={} orderId={} eventId={} originBranch={} status={}",
                            instanceId, orderId, eventId, current.originBranch(), status);
                    return new KitchenProcessingResult(eventId, orderId, Outcome.CANCEL_REJECTED, status);
                }
                case CANCELLED, FAILED -> {
                    // Precedence rule 3: first or duplicate origin-branch cancel on CANCELLED/FAILED re-emits stored result
                    reemitStoredTerminalResult(current, eventId, now);
                    store.updateProcessedOutcome(eventId, Outcome.STORED_RESULT.name());
                    log.info("Kitchen re-emitted stored terminal result on CancelRequested instanceId={} orderId={} eventId={} status={}",
                            instanceId, orderId, eventId, status);
                    return new KitchenProcessingResult(eventId, orderId, Outcome.STORED_RESULT, status);
                }
            }
            throw new IllegalStateException("Unhandled KitchenStatus: " + status);
        });
        return requireNonNullResult(result, envelope.orderId());
    }

    public KitchenProcessingResult processIngredientCheckChanged(
            EventEnvelope<IngredientCheckChangedPayload> envelope,
            JsonNode payloadNode,
            EventProcessingContext context
    ) {
        KitchenEventValidator.validateIngredientCheckChanged(envelope, payloadNode);
        KitchenProcessingResult result = readCommittedTx.execute(txStatus -> {
            OffsetDateTime now = nowUtc();
            UUID eventId = envelope.eventId();
            if (!store.tryInsertProcessedEvent(eventId, null, EventType.IngredientCheckChanged.name(), "APPLIED", now)) {
                txStatus.setRollbackOnly();
                log.info("Kitchen duplicate IngredientCheckChanged skipped instanceId={} eventId={} topic={} partition={} offset={}",
                        instanceId, eventId, context.sourceTopic(), context.partition(), context.offset());
                return new KitchenProcessingResult(eventId, null, Outcome.DUPLICATE_EVENT, null);
            }

            IngredientCheckChangedPayload payload = envelope.payload();
            boolean updated = store.upsertFlaggedIngredientIfNewer(
                    payload.ingredientId().trim(),
                    payload.needsCheck(),
                    payload.epoch(),
                    payload.seq(),
                    now);
            if (!updated) {
                store.updateProcessedOutcome(eventId, Outcome.STALE_VERSION.name());
                log.info("Kitchen ignored stale IngredientCheckChanged instanceId={} eventId={} ingredientId={} epoch={} seq={}",
                        instanceId, eventId, payload.ingredientId(), payload.epoch(), payload.seq());
                return new KitchenProcessingResult(eventId, null, Outcome.STALE_VERSION, null);
            }
            log.info("Kitchen updated flagged_ingredients instanceId={} eventId={} ingredientId={} needsCheck={} epoch={} seq={}",
                    instanceId, eventId, payload.ingredientId(), payload.needsCheck(), payload.epoch(), payload.seq());
            return new KitchenProcessingResult(eventId, null, Outcome.APPLIED, null);
        });
        return result != null ? result : new KitchenProcessingResult(envelope.eventId(), null, Outcome.APPLIED, null);
    }

    public KitchenTicketResponse startTicket(UUID orderId) {
        KitchenTicketResponse response = readCommittedTx.execute(txStatus -> {
            OffsetDateTime now = nowUtc();
            OrderRow current = store.ensureAndLockOrder(orderId);
            KitchenStatus status = KitchenStatus.valueOf(current.status());

            if (status == KitchenStatus.NOT_EXISTS) {
                txStatus.setRollbackOnly();
                throw new TicketNotFoundException(orderId);
            }
            if (status != KitchenStatus.QUEUED) {
                txStatus.setRollbackOnly();
                throw new TicketConflictException(orderId, status.name(), KitchenStatus.QUEUED.name());
            }

            store.transitionQueuedToCooking(orderId, now);
            emitStockConsumed(orderId, current.originBranch(), now);

            log.info("Kitchen started cooking ticket instanceId={} orderId={} originBranch={}",
                    instanceId, orderId, current.originBranch());
            return store.findTicket(orderId)
                    .map(this::toTicketResponse)
                    .orElseThrow(() -> new IllegalStateException("Ticket disappeared after start: " + orderId));
        });
        if (response == null) {
            throw new IllegalStateException("startTicket transaction returned null for orderId=" + orderId);
        }
        return response;
    }

    public KitchenTicketResponse rejectTicket(UUID orderId, RejectTicketRequest request) {
        if (request == null || request.reasonCode() == null || request.reasonCode().isBlank()) {
            throw new BadKitchenRequestException("reasonCode is required");
        }
        ReasonCode reasonCode;
        try {
            reasonCode = ReasonCode.valueOf(request.reasonCode().trim());
        } catch (IllegalArgumentException ex) {
            throw new BadKitchenRequestException("Invalid reasonCode: " + request.reasonCode());
        }
        if (!reasonCode.isKitchenRejection()) {
            throw new BadKitchenRequestException("reasonCode is not a Kitchen rejection reason: " + reasonCode);
        }

        Set<String> normalizedSpoiled = new LinkedHashSet<>();
        for (String ingredient : request.spoiledIngredients()) {
            if (ingredient == null || ingredient.isBlank()) {
                throw new BadKitchenRequestException("spoiledIngredients must not contain blank entries");
            }
            normalizedSpoiled.add(ingredient.trim());
        }
        List<String> spoiledList = List.copyOf(normalizedSpoiled);

        if (reasonCode.requiresSpoiledIngredients() && spoiledList.isEmpty()) {
            throw new BadKitchenRequestException("spoiledIngredients is required when reasonCode is SPOILED_INGREDIENT");
        }
        if (!reasonCode.requiresSpoiledIngredients() && !spoiledList.isEmpty()) {
            throw new BadKitchenRequestException("spoiledIngredients must be empty for reasonCode " + reasonCode);
        }

        KitchenTicketResponse response = readCommittedTx.execute(txStatus -> {
            OffsetDateTime now = nowUtc();
            OrderRow current = store.ensureAndLockOrder(orderId);
            KitchenStatus status = KitchenStatus.valueOf(current.status());

            if (status == KitchenStatus.NOT_EXISTS) {
                txStatus.setRollbackOnly();
                throw new TicketNotFoundException(orderId);
            }
            if (status != KitchenStatus.QUEUED) {
                txStatus.setRollbackOnly();
                throw new TicketConflictException(orderId, status.name(), KitchenStatus.QUEUED.name());
            }

            if (reasonCode.requiresSpoiledIngredients()) {
                List<IngredientQuantity> reserved = parseReserved(current.reservedJson());
                Set<String> reservedIds = new LinkedHashSet<>();
                for (IngredientQuantity ing : reserved) {
                    reservedIds.add(ing.ingredientId());
                }
                for (String spoiledId : spoiledList) {
                    if (!reservedIds.contains(spoiledId)) {
                        txStatus.setRollbackOnly();
                        throw new BadKitchenRequestException(
                                "Spoiled ingredient '" + spoiledId + "' does not belong to reserved ingredients of order " + orderId);
                    }
                }
            }

            String spoiledJson = writeJson(spoiledList);
            store.transitionQueuedToKitchenRejected(orderId, reasonCode.name(), spoiledJson, now);
            emitOrderCancelled(orderId, current.originBranch(), CancelledBy.KITCHEN, reasonCode, null, now);
            emitReleaseRequested(orderId, current.originBranch(), CancelledBy.KITCHEN, reasonCode, spoiledList, null, now);

            log.info("Kitchen rejected ticket instanceId={} orderId={} originBranch={} reason={} spoiled={}",
                    instanceId, orderId, current.originBranch(), reasonCode, spoiledList);
            return store.findTicket(orderId)
                    .map(this::toTicketResponse)
                    .orElseThrow(() -> new IllegalStateException("Ticket disappeared after reject: " + orderId));
        });
        if (response == null) {
            throw new IllegalStateException("rejectTicket transaction returned null for orderId=" + orderId);
        }
        return response;
    }

    public KitchenTicketResponse completeTicket(UUID orderId) {
        KitchenTicketResponse response = readCommittedTx.execute(txStatus -> {
            OffsetDateTime now = nowUtc();
            OrderRow current = store.ensureAndLockOrder(orderId);
            KitchenStatus status = KitchenStatus.valueOf(current.status());

            if (status == KitchenStatus.NOT_EXISTS) {
                txStatus.setRollbackOnly();
                throw new TicketNotFoundException(orderId);
            }
            if (status != KitchenStatus.COOKING) {
                txStatus.setRollbackOnly();
                throw new TicketConflictException(orderId, status.name(), KitchenStatus.COOKING.name());
            }

            store.transitionCookingToDone(orderId, now);
            emitOrderCompleted(orderId, current.originBranch(), null, now);

            log.info("Kitchen completed ticket instanceId={} orderId={} originBranch={}",
                    instanceId, orderId, current.originBranch());
            return store.findTicket(orderId)
                    .map(this::toTicketResponse)
                    .orElseThrow(() -> new IllegalStateException("Ticket disappeared after complete: " + orderId));
        });
        if (response == null) {
            throw new IllegalStateException("completeTicket transaction returned null for orderId=" + orderId);
        }
        return response;
    }

    public List<KitchenTicketResponse> listTickets(String statusFilter) {
        String normalized = null;
        if (statusFilter != null && !statusFilter.isBlank()) {
            normalized = statusFilter.trim().toUpperCase();
            if (!"ALL".equals(normalized)) {
                try {
                    KitchenStatus ks = KitchenStatus.valueOf(normalized);
                    if (ks == KitchenStatus.NOT_EXISTS) {
                        throw new BadKitchenRequestException("Invalid status filter: " + statusFilter);
                    }
                } catch (IllegalArgumentException ex) {
                    throw new BadKitchenRequestException("Invalid status filter: " + statusFilter);
                }
            }
        }
        return store.listTickets(normalized).stream()
                .map(this::toTicketResponse)
                .toList();
    }

    public Optional<KitchenTicketResponse> findTicket(UUID orderId) {
        return store.findTicket(orderId).map(this::toTicketResponse);
    }

    public List<KitchenAlertResponse> listAlerts(String kindFilter) {
        String normalized = null;
        if (kindFilter != null && !kindFilter.isBlank()) {
            normalized = kindFilter.trim().toUpperCase();
            if (!AlertKind.NEEDS_REVIEW.name().equals(normalized)
                    && !AlertKind.WRONG_BRANCH_CANCEL.name().equals(normalized)) {
                throw new BadKitchenRequestException(
                        "Invalid alert kind: " + kindFilter + ". Allowed: NEEDS_REVIEW, WRONG_BRANCH_CANCEL");
            }
        }
        return store.listAlerts(normalized).stream()
                .map(this::toAlertResponse)
                .toList();
    }

    private void reemitStoredTerminalResult(OrderRow current, UUID causationId, OffsetDateTime now) {
        KitchenStatus status = KitchenStatus.valueOf(current.status());
        String originBranch = current.originBranch();
        switch (status) {
            case CANCELLED -> {
                CancelledBy cancelledBy = CancelledBy.valueOf(current.cancelledBy());
                ReasonCode reasonCode = ReasonCode.valueOf(current.closeReason());
                emitOrderCancelled(current.orderId(), originBranch, cancelledBy, reasonCode, causationId, now);
            }
            case FAILED -> {
                ReasonCode reasonCode = ReasonCode.valueOf(current.closeReason());
                emitOrderFailed(current.orderId(), originBranch, reasonCode, causationId, now);
            }
            case DONE -> emitOrderCompleted(current.orderId(), originBranch, causationId, now);
            default -> throw new IllegalStateException(
                    "Cannot re-emit terminal result for status=" + status + " orderId=" + current.orderId());
        }
    }

    private void emitCancelPending(UUID orderId, String targetBranch, UUID causationId, OffsetDateTime now) {
        UUID outboxEventId = UUID.randomUUID();
        EventEnvelope<CancelPendingPayload> out = EventEnvelope.v1(
                outboxEventId,
                EventType.CancelPending,
                orderId,
                targetBranch,
                now.toInstant(),
                causationId,
                CancelPendingPayload.empty());
        store.insertOutboxEvent(
                outboxEventId,
                orderId,
                EventType.CancelPending.name(),
                EventTopics.statusEventsTopic(targetBranch),
                orderId.toString(),
                writeJson(out),
                now);
    }

    private void emitCancelRejected(
            UUID orderId,
            String originBranch,
            KitchenStatus kitchenStatus,
            UUID causationId,
            OffsetDateTime now
    ) {
        UUID outboxEventId = UUID.randomUUID();
        EventEnvelope<CancelRejectedPayload> out = EventEnvelope.v1(
                outboxEventId,
                EventType.CancelRejected,
                orderId,
                originBranch,
                now.toInstant(),
                causationId,
                new CancelRejectedPayload(kitchenStatus, CANCEL_REJECTED_MESSAGE));
        store.insertOutboxEvent(
                outboxEventId,
                orderId,
                EventType.CancelRejected.name(),
                EventTopics.statusEventsTopic(originBranch),
                orderId.toString(),
                writeJson(out),
                now);
    }

    private void emitOrderCancelled(
            UUID orderId,
            String originBranch,
            CancelledBy cancelledBy,
            ReasonCode reasonCode,
            UUID causationId,
            OffsetDateTime now
    ) {
        UUID outboxEventId = UUID.randomUUID();
        EventEnvelope<OrderCancelledPayload> out = EventEnvelope.v1(
                outboxEventId,
                EventType.OrderCancelled,
                orderId,
                originBranch,
                now.toInstant(),
                causationId,
                new OrderCancelledPayload(cancelledBy, reasonCode));
        store.insertOutboxEvent(
                outboxEventId,
                orderId,
                EventType.OrderCancelled.name(),
                EventTopics.statusEventsTopic(originBranch),
                orderId.toString(),
                writeJson(out),
                now);
    }

    private void emitOrderCompleted(UUID orderId, String originBranch, UUID causationId, OffsetDateTime now) {
        UUID outboxEventId = UUID.randomUUID();
        EventEnvelope<OrderCompletedPayload> out = EventEnvelope.v1(
                outboxEventId,
                EventType.OrderCompleted,
                orderId,
                originBranch,
                now.toInstant(),
                causationId,
                OrderCompletedPayload.empty());
        store.insertOutboxEvent(
                outboxEventId,
                orderId,
                EventType.OrderCompleted.name(),
                EventTopics.statusEventsTopic(originBranch),
                orderId.toString(),
                writeJson(out),
                now);
    }

    private void emitOrderFailed(
            UUID orderId,
            String originBranch,
            ReasonCode reasonCode,
            UUID causationId,
            OffsetDateTime now
    ) {
        UUID outboxEventId = UUID.randomUUID();
        EventEnvelope<OrderFailedPayload> out = EventEnvelope.v1(
                outboxEventId,
                EventType.OrderFailed,
                orderId,
                originBranch,
                now.toInstant(),
                causationId,
                new OrderFailedPayload(originBranch, reasonCode, reasonCode.defaultMessage()));
        store.insertOutboxEvent(
                outboxEventId,
                orderId,
                EventType.OrderFailed.name(),
                EventTopics.statusEventsTopic(originBranch),
                orderId.toString(),
                writeJson(out),
                now);
    }

    private void emitStockConsumed(UUID orderId, String originBranch, OffsetDateTime now) {
        UUID outboxEventId = UUID.randomUUID();
        EventEnvelope<StockConsumedPayload> out = EventEnvelope.v1(
                outboxEventId,
                EventType.StockConsumed,
                orderId,
                originBranch,
                now.toInstant(),
                null,
                StockConsumedPayload.empty());
        store.insertOutboxEvent(
                outboxEventId,
                orderId,
                EventType.StockConsumed.name(),
                EventTopics.STOCK_COMMANDS,
                orderId.toString(),
                writeJson(out),
                now);
    }

    private void emitReleaseRequested(
            UUID orderId,
            String originBranch,
            CancelledBy cancelledBy,
            ReasonCode reasonCode,
            List<String> spoiledIngredients,
            UUID causationId,
            OffsetDateTime now
    ) {
        UUID outboxEventId = UUID.randomUUID();
        EventEnvelope<ReleaseRequestedPayload> out = EventEnvelope.v1(
                outboxEventId,
                EventType.ReleaseRequested,
                orderId,
                originBranch,
                now.toInstant(),
                causationId,
                new ReleaseRequestedPayload(cancelledBy, reasonCode, spoiledIngredients));
        store.insertOutboxEvent(
                outboxEventId,
                orderId,
                EventType.ReleaseRequested.name(),
                EventTopics.STOCK_COMMANDS,
                orderId.toString(),
                writeJson(out),
                now);
    }

    private KitchenTicketResponse toTicketResponse(TicketViewRow row) {
        List<KitchenTicketResponse.DishItem> items = parseItems(row.itemsJson()).stream()
                .map(i -> new KitchenTicketResponse.DishItem(i.lineNo(), i.dishId(), i.dishName(), i.quantity()))
                .toList();
        List<KitchenTicketResponse.ReservedIngredient> reserved = parseReserved(row.reservedJson()).stream()
                .map(r -> new KitchenTicketResponse.ReservedIngredient(r.ingredientId(), r.quantity()))
                .toList();
        List<String> spoiled = parseStringList(row.spoiledJson());
        List<String> flagged = KitchenStatus.QUEUED.name().equals(row.status())
                ? row.flaggedIngredients()
                : List.of();
        return new KitchenTicketResponse(
                row.orderId(),
                row.status(),
                row.originBranch(),
                row.cancelledBy(),
                row.closeReason(),
                items,
                reserved,
                spoiled,
                !flagged.isEmpty(),
                flagged,
                row.orderCreatedAt() == null ? null : row.orderCreatedAt().toInstant(),
                row.updatedAt().toInstant());
    }

    private KitchenAlertResponse toAlertResponse(AlertRow row) {
        return new KitchenAlertResponse(
                row.id(),
                row.kind(),
                row.orderId(),
                row.eventId(),
                row.sourceTopic(),
                row.sourcePartition(),
                row.sourceOffset(),
                parseObjectMap(row.detailJson()),
                row.createdAt().toInstant());
    }

    private OffsetDateTime nowUtc() {
        return Instant.now(clock).atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MICROS);
    }

    private String writeJson(Object value) {
        return json.writeValueAsString(value);
    }

    private List<OrderItemLine> parseItems(String itemsJson) {
        if (itemsJson == null || itemsJson.isBlank()) {
            return List.of();
        }
        return json.readValue(itemsJson, new TypeReference<List<OrderItemLine>>() {});
    }

    private List<IngredientQuantity> parseReserved(String reservedJson) {
        if (reservedJson == null || reservedJson.isBlank()) {
            return List.of();
        }
        return json.readValue(reservedJson, new TypeReference<List<IngredientQuantity>>() {});
    }

    private List<String> parseStringList(String listJson) {
        if (listJson == null || listJson.isBlank()) {
            return List.of();
        }
        return json.readValue(listJson, new TypeReference<List<String>>() {});
    }

    private Map<String, String> parseStringMap(String mapJson) {
        if (mapJson == null || mapJson.isBlank()) {
            return Map.of();
        }
        return json.readValue(mapJson, new TypeReference<Map<String, String>>() {});
    }

    private Map<String, Object> parseObjectMap(String mapJson) {
        if (mapJson == null || mapJson.isBlank()) {
            return Map.of();
        }
        return json.readValue(mapJson, new TypeReference<Map<String, Object>>() {});
    }

    private static KitchenProcessingResult requireNonNullResult(KitchenProcessingResult result, UUID orderId) {
        if (result == null) {
            throw new IllegalStateException("Kitchen transaction returned null for orderId=" + orderId);
        }
        return result;
    }
}
