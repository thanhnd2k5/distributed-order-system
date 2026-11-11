package vn.edu.orders.inventory.application;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.ObjectMapper;
import vn.edu.orders.contracts.events.EventType;
import vn.edu.orders.contracts.events.FailedEvent;
import vn.edu.orders.contracts.events.FailureReasonCode;
import vn.edu.orders.contracts.events.OrderCreatedEvent;
import vn.edu.orders.contracts.events.OrderItemLine;
import vn.edu.orders.contracts.events.ReservedEvent;
import vn.edu.orders.inventory.application.ReservationResult.Outcome;
import vn.edu.orders.inventory.config.InventoryProperties;
import vn.edu.orders.inventory.infrastructure.InventoryReservationStore;
import vn.edu.orders.inventory.infrastructure.InventoryReservationStore.DecisionRecord;
import vn.edu.orders.inventory.infrastructure.InventoryReservationStore.LockedIngredient;
import vn.edu.orders.inventory.infrastructure.InventoryReservationStore.RecipeRow;

@Service
public class InventoryReservationService {
    private static final Logger log = LoggerFactory.getLogger(InventoryReservationService.class);

    private final InventoryReservationStore store;
    private final TransactionTemplate readCommittedTx;
    private final ObjectMapper json;
    private final String instanceId;

    public InventoryReservationService(
            InventoryReservationStore store,
            PlatformTransactionManager transactionManager,
            ObjectMapper json,
            InventoryProperties properties
    ) {
        this.store = store;
        this.readCommittedTx = new TransactionTemplate(transactionManager);
        this.readCommittedTx.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        this.json = json;
        this.instanceId = properties.instanceId();
    }

    public ReservationResult processOrderCreated(OrderCreatedEvent event, EventProcessingContext context) {
        OrderCreatedValidator.validate(event);
        String fingerprint = OrderCreatedValidator.fingerprint(event);

        ReservationResult result = readCommittedTx.execute(status -> applyInTransaction(event, fingerprint, context));
        if (result == null) {
            throw new IllegalStateException("Reservation transaction returned null for orderId=" + event.orderId());
        }
        return result;
    }

    private ReservationResult applyInTransaction(
            OrderCreatedEvent event,
            String fingerprint,
            EventProcessingContext context
    ) {
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.MICROS);
        UUID eventId = event.eventId();
        UUID orderId = event.orderId();

        // 1. Technical deduplication by eventId
        if (!store.tryInsertProcessedEvent(eventId, orderId, event.eventType().name(), fingerprint, now)) {
            DecisionRecord existing = store.findDecision(orderId).orElse(null);
            String decision = existing == null ? null : existing.decision();
            FailureReasonCode reasonCode = existing == null ? null : existing.reasonCode();
            log.info("Inventory duplicate event skipped instanceId={} orderId={} eventId={} partition={} offset={} outcome=DUPLICATE_EVENT existingDecision={}",
                    instanceId, orderId, eventId, context.partition(), context.offset(), decision);
            return new ReservationResult(eventId, orderId, Outcome.DUPLICATE_EVENT, decision, reasonCode);
        }

        // 2. Business idempotency by orderId (waits if another tx currently owns PROCESSING row)
        if (!store.tryInsertProcessingDecision(orderId, event.sourceType(), event.sourceSiteId(), fingerprint, now)) {
            DecisionRecord existing = store.findDecision(orderId)
                    .orElseThrow(() -> new IllegalStateException(
                            "Order decision disappeared after conflict for orderId=" + orderId));
            if ("PROCESSING".equals(existing.decision())) {
                throw new IllegalStateException(
                        "Unexpected committed PROCESSING decision for orderId=" + orderId);
            }
            if (existing.requestFingerprint().equals(fingerprint)) {
                store.updateProcessedOutcome(eventId, Outcome.DUPLICATE_ORDER.name());
                log.info("Inventory duplicate orderId skipped instanceId={} orderId={} eventId={} partition={} offset={} outcome=DUPLICATE_ORDER existingDecision={}",
                        instanceId, orderId, eventId, context.partition(), context.offset(), existing.decision());
                return new ReservationResult(
                        eventId, orderId, Outcome.DUPLICATE_ORDER, existing.decision(), existing.reasonCode());
            }
            store.updateProcessedOutcome(eventId, Outcome.PAYLOAD_CONFLICT.name());
            log.error("Inventory payload conflict detected instanceId={} orderId={} eventId={} partition={} offset={} outcome=PAYLOAD_CONFLICT existingDecision={} existingFingerprint={} incomingFingerprint={}",
                    instanceId, orderId, eventId, context.partition(), context.offset(),
                    existing.decision(), existing.requestFingerprint(), fingerprint);
            return new ReservationResult(
                    eventId, orderId, Outcome.PAYLOAD_CONFLICT, existing.decision(), existing.reasonCode());
        }

        // 3. Aggregate ingredient demand from recipes
        List<String> dishIds = event.payload().items().stream()
                .map(OrderItemLine::dishId)
                .distinct()
                .sorted()
                .toList();
        Map<String, List<RecipeRow>> recipesByDish = store.findRecipeRows(dishIds).stream()
                .collect(Collectors.groupingBy(RecipeRow::dishId));

        List<String> missingDishes = new ArrayList<>();
        TreeMap<String, Long> requiredByIngredient = new TreeMap<>();
        for (OrderItemLine item : event.payload().items()) {
            List<RecipeRow> recipe = recipesByDish.get(item.dishId());
            if (recipe == null || recipe.isEmpty()) {
                missingDishes.add(item.dishId());
                continue;
            }
            for (RecipeRow ingredientLine : recipe) {
                long lineDemand = Math.multiplyExact(ingredientLine.quantityPerDish(), (long) item.quantity());
                requiredByIngredient.merge(ingredientLine.ingredientId(), lineDemand, Math::addExact);
            }
        }

        if (!missingDishes.isEmpty()) {
            String message = "Không tìm thấy công thức cho món: " + String.join(", ", missingDishes);
            return commitFailedDecision(event, FailureReasonCode.RECIPE_NOT_FOUND, message, now, context);
        }

        // 4. Lock all required ingredients in deterministic ID order
        List<String> sortedIngredientIds = List.copyOf(requiredByIngredient.keySet());
        List<LockedIngredient> lockedRows = store.lockIngredientsInOrder(sortedIngredientIds);
        Map<String, LockedIngredient> lockedById = new LinkedHashMap<>();
        for (LockedIngredient row : lockedRows) {
            lockedById.put(row.ingredientId(), row);
        }

        // 5. Verify all ingredients before modifying any row
        List<String> shortages = new ArrayList<>();
        for (Map.Entry<String, Long> demand : requiredByIngredient.entrySet()) {
            String ingredientId = demand.getKey();
            long required = demand.getValue();
            LockedIngredient current = lockedById.get(ingredientId);
            if (current == null || current.availableQuantity() < required) {
                long available = current == null ? 0L : current.availableQuantity();
                shortages.add(ingredientId + " (cần " + required + ", còn " + available + ")");
            }
        }
        if (!shortages.isEmpty()) {
            String message = "Không đủ nguyên liệu: " + String.join("; ", shortages);
            return commitFailedDecision(event, FailureReasonCode.INSUFFICIENT_INGREDIENT, message, now, context);
        }

        // 6. Deduct ingredients atomically and persist reservation items
        for (Map.Entry<String, Long> demand : requiredByIngredient.entrySet()) {
            String ingredientId = demand.getKey();
            long required = demand.getValue();
            int updated = store.deductIngredientIfSufficient(ingredientId, required, now);
            if (updated != 1) {
                throw new IllegalStateException(
                        "Failed atomic stock deduction for ingredientId=" + ingredientId
                                + " orderId=" + orderId + " updatedRows=" + updated);
            }
            store.insertReservationItem(orderId, ingredientId, required);
        }
        store.markDecisionReserved(orderId, now);

        // 7. Write Reserved event to outbox in same transaction
        UUID outboxEventId = UUID.randomUUID();
        var reservedEvent = ReservedEvent.v1(
                outboxEventId,
                orderId,
                event.sourceType(),
                event.sourceSiteId(),
                now.toInstant(),
                orderId,
                event.payload().items());
        store.insertOutboxStatusEvent(
                outboxEventId, orderId, EventType.Reserved, json.writeValueAsString(reservedEvent), now);

        log.info("Inventory reserved order instanceId={} orderId={} eventId={} partition={} offset={} outcome=APPLIED decision=RESERVED outboxEventId={}",
                instanceId, orderId, eventId, context.partition(), context.offset(), outboxEventId);
        return new ReservationResult(eventId, orderId, Outcome.APPLIED, "RESERVED", null);
    }

    private ReservationResult commitFailedDecision(
            OrderCreatedEvent event,
            FailureReasonCode reasonCode,
            String reasonMessage,
            OffsetDateTime now,
            EventProcessingContext context
    ) {
        UUID orderId = event.orderId();
        UUID eventId = event.eventId();
        store.markDecisionFailed(orderId, reasonCode, reasonMessage, now);

        UUID outboxEventId = UUID.randomUUID();
        var failedEvent = FailedEvent.v1(
                outboxEventId,
                orderId,
                event.sourceType(),
                event.sourceSiteId(),
                now.toInstant(),
                reasonCode,
                reasonMessage);
        store.insertOutboxStatusEvent(
                outboxEventId, orderId, EventType.Failed, json.writeValueAsString(failedEvent), now);

        log.info("Inventory rejected order instanceId={} orderId={} eventId={} partition={} offset={} outcome=APPLIED decision=FAILED reasonCode={} outboxEventId={}",
                instanceId, orderId, eventId, context.partition(), context.offset(), reasonCode, outboxEventId);
        return new ReservationResult(eventId, orderId, Outcome.APPLIED, "FAILED", reasonCode);
    }
}
