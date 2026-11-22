package vn.edu.orders.inventory.application;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
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
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.ObjectMapper;
import vn.edu.orders.contracts.events.EventEnvelope;
import vn.edu.orders.contracts.events.EventTopics;
import vn.edu.orders.contracts.events.EventType;
import vn.edu.orders.contracts.events.IngredientQuantity;
import vn.edu.orders.contracts.events.InventoryStatus;
import vn.edu.orders.contracts.events.OrderCreatedPayload;
import vn.edu.orders.contracts.events.OrderFailedPayload;
import vn.edu.orders.contracts.events.OrderItemLine;
import vn.edu.orders.contracts.events.OrderReservedPayload;
import vn.edu.orders.contracts.events.ReasonCode;
import vn.edu.orders.inventory.config.InventoryProperties;
import vn.edu.orders.inventory.domain.InventoryQuantityUtils;
import vn.edu.orders.inventory.infrastructure.InventoryStore;
import vn.edu.orders.inventory.infrastructure.InventoryStore.InventoryOrderRow;
import vn.edu.orders.inventory.infrastructure.InventoryStore.RecipeLine;
import vn.edu.orders.inventory.infrastructure.InventoryStore.StockRow;

@Service
public class InventoryOrderService {
    private static final Logger log = LoggerFactory.getLogger(InventoryOrderService.class);

    private final InventoryStore store;
    private final DishStatusService dishStatusService;
    private final TransactionTemplate tx;
    private final ObjectMapper json;
    private final Clock clock;
    private final InventoryProperties properties;

    public InventoryOrderService(
            InventoryStore store,
            DishStatusService dishStatusService,
            PlatformTransactionManager transactionManager,
            ObjectMapper json,
            Clock clock,
            InventoryProperties properties
    ) {
        this.store = store;
        this.dishStatusService = dishStatusService;
        this.tx = new TransactionTemplate(transactionManager);
        this.tx.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        this.json = json;
        this.clock = clock;
        this.properties = properties;
    }

    /** Processes a readable OrderCreated event coming from topic {@code order-events.<branch>}. */
    public OrderProcessingResult processOrderCreated(
            EventEnvelope<OrderCreatedPayload> envelope,
            EventProcessingContext context
    ) {
        if (envelope == null || envelope.eventId() == null || envelope.orderId() == null) {
            throw new IllegalArgumentException("Envelope with eventId and orderId is required");
        }
        String originBranch = EventTopics.branchFromTopic(context.topic());
        return executeOrderCreatedTx(
                envelope.eventId(),
                envelope.orderId(),
                originBranch,
                envelope,
                null,
                context);
    }

    /** Records FAILED(INVALID_ORDER_DATA) for a readable OrderCreated whose payload could not be bound cleanly. */
    public OrderProcessingResult processInvalidOrderData(
            UUID eventId,
            UUID orderId,
            String invalidReasonDetail,
            EventProcessingContext context
    ) {
        String originBranch = EventTopics.branchFromTopic(context.topic());
        return executeOrderCreatedTx(eventId, orderId, originBranch, null, invalidReasonDetail, context);
    }

    private OrderProcessingResult executeOrderCreatedTx(
            UUID eventId,
            UUID orderId,
            String originBranch,
            EventEnvelope<OrderCreatedPayload> envelope,
            String invalidDataReasonOverride,
            EventProcessingContext context
    ) {
        OrderProcessingResult result = tx.execute(txStatus -> applyInTransaction(
                txStatus, eventId, orderId, originBranch, envelope, invalidDataReasonOverride, context));
        if (result == null) {
            throw new IllegalStateException("Order transaction returned null for orderId=" + orderId);
        }
        return result;
    }

    private OrderProcessingResult applyInTransaction(
            TransactionStatus txStatus,
            UUID eventId,
            UUID orderId,
            String originBranch,
            EventEnvelope<OrderCreatedPayload> envelope,
            String invalidDataReasonOverride,
            EventProcessingContext context
    ) {
        Instant nowInstant = clock.instant();
        OffsetDateTime now = OffsetDateTime.ofInstant(nowInstant, ZoneOffset.UTC).truncatedTo(ChronoUnit.MICROS);

        // Layer 1 dedup: event_id in processed_events -> ignore and emit nothing
        if (!store.tryInsertProcessedEvent(eventId, orderId, EventType.OrderCreated.name(), now)) {
            InventoryOrderRow existing = store.findOrder(orderId).orElse(null);
            log.info("Inventory duplicate eventId ignored instanceId={} orderId={} eventId={} topic={} partition={} offset={} existingStatus={}",
                    properties.instanceId(), orderId, eventId, context.topic(), context.partition(), context.offset(),
                    existing == null ? null : existing.status());
            return new OrderProcessingResult(
                    eventId,
                    orderId,
                    Outcome.DUPLICATE_EVENT,
                    existing == null ? null : existing.status(),
                    parseReasonCode(existing == null ? null : existing.reason()));
        }

        OffsetDateTime orderCreatedAt = extractOrderCreatedAt(envelope);

        // Layer 2 dedup: order_id PK on inventory_orders -> lock and return stored result ONLY to status-events.<originBranch>
        if (!store.tryInsertOrder(orderId, originBranch, InventoryStatus.HELD, null, orderCreatedAt, now)) {
            InventoryOrderRow existing = store.lockOrderForUpdate(orderId)
                    .orElseThrow(() -> new IllegalStateException("Order vanished after conflict: " + orderId));
            emitStoredResultToOriginBranchOnly(existing, eventId, now);
            ReasonCode existingReason = parseReasonCode(existing.reason());
            log.info("Inventory duplicate orderId returned stored result instanceId={} orderId={} eventId={} originBranch={} status={} reason={}",
                    properties.instanceId(), orderId, eventId, existing.originBranch(), existing.status(), existingReason);
            return new OrderProcessingResult(eventId, orderId, Outcome.DUPLICATE_ORDER, existing.status(), existingReason);
        }

        // Validate readable order data -> FAILED(INVALID_ORDER_DATA), NOT dlq
        String validationError = invalidDataReasonOverride != null
                ? invalidDataReasonOverride
                : validateOrderData(envelope);
        if (validationError != null) {
            return failNewOrder(
                    orderId, originBranch, eventId, ReasonCode.INVALID_ORDER_DATA, validationError, now, context);
        }

        // BR11 time limit check using injectable Clock
        Instant createdAt = envelope.payload().createdAt();
        if (createdAt.isBefore(nowInstant.minus(properties.orderExpiry()))) {
            return failNewOrder(
                    orderId,
                    originBranch,
                    eventId,
                    ReasonCode.ORDER_EXPIRED,
                    "Order created at " + createdAt + " exceeded expiry " + properties.orderExpiry(),
                    now,
                    context);
        }
        if (createdAt.isAfter(nowInstant.plus(properties.futureTolerance()))) {
            return failNewOrder(
                    orderId,
                    originBranch,
                    eventId,
                    ReasonCode.INVALID_CREATION_TIME,
                    "Order created at " + createdAt + " is in the future beyond tolerance " + properties.futureTolerance(),
                    now,
                    context);
        }

        // Aggregate ingredient demand per dish from current recipes
        List<OrderItemLine> items = envelope.payload().items();
        List<String> distinctDishes = items.stream()
                .map(OrderItemLine::dishId)
                .distinct()
                .sorted()
                .toList();
        Map<String, List<RecipeLine>> recipesByDish = store.findRecipesForDishes(distinctDishes).stream()
                .collect(Collectors.groupingBy(RecipeLine::dishId));

        List<String> missingDishes = new ArrayList<>();
        TreeMap<String, BigDecimal> requiredByIngredient = new TreeMap<>();
        for (OrderItemLine item : items) {
            List<RecipeLine> recipe = recipesByDish.get(item.dishId());
            if (recipe == null || recipe.isEmpty()) {
                missingDishes.add(item.dishId());
                continue;
            }
            for (RecipeLine line : recipe) {
                BigDecimal demand = line.quantity().multiply(BigDecimal.valueOf(item.quantity()));
                requiredByIngredient.merge(line.ingredientId(), demand, BigDecimal::add);
            }
        }
        if (!missingDishes.isEmpty()) {
            return failNewOrder(
                    orderId,
                    originBranch,
                    eventId,
                    ReasonCode.DISH_HAS_NO_RECIPE,
                    "Dish has no recipe: " + String.join(", ", missingDishes),
                    now,
                    context);
        }

        // SAVEPOINT reserve in ascending ingredient_id order (BR1, BR6, §13.4)
        Object savepoint = txStatus.createSavepoint();
        ReasonCode failureReason = null;
        String failureDetail = null;
        List<String> sortedIngredients = List.copyOf(requiredByIngredient.keySet());
        List<IngredientQuantity> reservedList = new ArrayList<>(sortedIngredients.size());

        for (String ingredientId : sortedIngredients) {
            BigDecimal required = requiredByIngredient.get(ingredientId);
            int updated = store.reserveStockIfAvailableAndNotFlagged(ingredientId, required, now);
            if (updated != 1) {
                StockRow current = store.lockStockRow(ingredientId).orElse(null);
                if (current != null && current.needsCheck()) {
                    failureReason = ReasonCode.INGREDIENT_AWAITING_STOCK_CHECK;
                    failureDetail = "Ingredient awaiting stock check: " + ingredientId;
                } else {
                    failureReason = ReasonCode.INSUFFICIENT_INGREDIENTS;
                    BigDecimal available = current == null
                            ? BigDecimal.ZERO
                            : InventoryQuantityUtils.normalize(current.available());
                    failureDetail = "Insufficient ingredient " + ingredientId
                            + ": required=" + InventoryQuantityUtils.normalize(required)
                            + ", available=" + available;
                }
                break;
            }
            store.insertReservation(orderId, ingredientId, required);
            reservedList.add(new IngredientQuantity(ingredientId, InventoryQuantityUtils.normalize(required)));
        }

        if (failureReason != null) {
            txStatus.rollbackToSavepoint(savepoint);
            return failNewOrder(orderId, originBranch, eventId, failureReason, failureDetail, now, context);
        }
        txStatus.releaseSavepoint(savepoint);

        // Recompute affected dish statuses AFTER stock deduction (dish_status locked before reading stock)
        dishStatusService.recomputeForIngredientsInTx(sortedIngredients, eventId, now);

        // Emit OrderReserved to both status-events.<originBranch> and kitchen-orders with distinct eventIds
        UUID branchEventId = UUID.randomUUID();
        var branchEnvelope = EventEnvelope.v1(
                branchEventId,
                EventType.OrderReserved,
                orderId,
                originBranch,
                nowInstant,
                eventId,
                OrderReservedPayload.forBranch());
        store.insertOutboxEvent(
                branchEventId,
                orderId,
                EventType.OrderReserved,
                EventTopics.statusEventsTopic(originBranch),
                orderId.toString(),
                json.writeValueAsString(branchEnvelope),
                now);

        UUID kitchenEventId = UUID.randomUUID();
        var kitchenEnvelope = EventEnvelope.v1(
                kitchenEventId,
                EventType.OrderReserved,
                orderId,
                originBranch,
                nowInstant,
                eventId,
                OrderReservedPayload.forKitchen(originBranch, createdAt, items, reservedList));
        store.insertOutboxEvent(
                kitchenEventId,
                orderId,
                EventType.OrderReserved,
                EventTopics.KITCHEN_ORDERS,
                orderId.toString(),
                json.writeValueAsString(kitchenEnvelope),
                now);

        log.info("Inventory held order instanceId={} orderId={} eventId={} originBranch={} partition={} offset={} reservedCount={}",
                properties.instanceId(), orderId, eventId, originBranch, context.partition(), context.offset(), reservedList.size());
        return new OrderProcessingResult(eventId, orderId, Outcome.APPLIED, InventoryStatus.HELD, null);
    }

    private OrderProcessingResult failNewOrder(
            UUID orderId,
            String originBranch,
            UUID causationId,
            ReasonCode reasonCode,
            String reasonMessage,
            OffsetDateTime now,
            EventProcessingContext context
    ) {
        store.markOrderFailed(orderId, reasonCode.name(), now);

        String message = (reasonMessage == null || reasonMessage.isBlank())
                ? reasonCode.defaultMessage()
                : reasonMessage;
        var failedPayload = new OrderFailedPayload(originBranch, reasonCode, message);

        UUID branchOutboxId = UUID.randomUUID();
        var branchEnvelope = EventEnvelope.v1(
                branchOutboxId,
                EventType.OrderFailed,
                orderId,
                originBranch,
                now.toInstant(),
                causationId,
                failedPayload);
        store.insertOutboxEvent(
                branchOutboxId,
                orderId,
                EventType.OrderFailed,
                EventTopics.statusEventsTopic(originBranch),
                orderId.toString(),
                json.writeValueAsString(branchEnvelope),
                now);

        UUID kitchenOutboxId = UUID.randomUUID();
        var kitchenEnvelope = EventEnvelope.v1(
                kitchenOutboxId,
                EventType.OrderFailed,
                orderId,
                originBranch,
                now.toInstant(),
                causationId,
                failedPayload);
        store.insertOutboxEvent(
                kitchenOutboxId,
                orderId,
                EventType.OrderFailed,
                EventTopics.KITCHEN_ORDERS,
                orderId.toString(),
                json.writeValueAsString(kitchenEnvelope),
                now);

        log.info("Inventory failed order instanceId={} orderId={} eventId={} originBranch={} partition={} offset={} reasonCode={}",
                properties.instanceId(), orderId, causationId, originBranch, context.partition(), context.offset(), reasonCode);
        return new OrderProcessingResult(causationId, orderId, Outcome.APPLIED, InventoryStatus.FAILED, reasonCode);
    }

    private void emitStoredResultToOriginBranchOnly(
            InventoryOrderRow existing,
            UUID causationId,
            OffsetDateTime now
    ) {
        String branchTopic = EventTopics.statusEventsTopic(existing.originBranch());
        UUID outboxEventId = UUID.randomUUID();

        if (existing.status() == InventoryStatus.FAILED) {
            ReasonCode reasonCode = parseReasonCode(existing.reason());
            if (reasonCode == null) {
                reasonCode = ReasonCode.INVALID_ORDER_DATA;
            }
            var failedEnvelope = EventEnvelope.v1(
                    outboxEventId,
                    EventType.OrderFailed,
                    existing.orderId(),
                    existing.originBranch(),
                    now.toInstant(),
                    causationId,
                    new OrderFailedPayload(existing.originBranch(), reasonCode, reasonCode.defaultMessage()));
            store.insertOutboxEvent(
                    outboxEventId,
                    existing.orderId(),
                    EventType.OrderFailed,
                    branchTopic,
                    existing.orderId().toString(),
                    json.writeValueAsString(failedEnvelope),
                    now);
            return;
        }

        // HELD, RELEASED, or CONSUMED -> emit OrderReserved ONLY to status-events.<originBranch>
        var reservedEnvelope = EventEnvelope.v1(
                outboxEventId,
                EventType.OrderReserved,
                existing.orderId(),
                existing.originBranch(),
                now.toInstant(),
                causationId,
                OrderReservedPayload.forBranch());
        store.insertOutboxEvent(
                outboxEventId,
                existing.orderId(),
                EventType.OrderReserved,
                branchTopic,
                existing.orderId().toString(),
                json.writeValueAsString(reservedEnvelope),
                now);
    }

    private static OffsetDateTime extractOrderCreatedAt(EventEnvelope<OrderCreatedPayload> envelope) {
        if (envelope == null || envelope.payload() == null || envelope.payload().createdAt() == null) {
            return null;
        }
        return OffsetDateTime.ofInstant(envelope.payload().createdAt(), ZoneOffset.UTC).truncatedTo(ChronoUnit.MICROS);
    }

    private static String validateOrderData(EventEnvelope<OrderCreatedPayload> envelope) {
        if (envelope == null) {
            return "Missing OrderCreated envelope";
        }
        if (envelope.occurredAt() == null) {
            return "Missing occurredAt";
        }
        OrderCreatedPayload payload = envelope.payload();
        if (payload == null) {
            return "Missing OrderCreated payload";
        }
        if (payload.createdAt() == null) {
            return "Missing payload.createdAt";
        }
        if (payload.channel() == null) {
            return "Missing payload.channel";
        }
        if (payload.items() == null || payload.items().isEmpty()) {
            return "Order items must not be empty";
        }
        for (OrderItemLine item : payload.items()) {
            if (item == null) {
                return "Order item must not be null";
            }
            if (item.dishId() == null || item.dishId().isBlank() || item.dishId().length() > 50) {
                return "Invalid dishId: " + item.dishId();
            }
            if (item.quantity() <= 0) {
                return "Non-positive quantity " + item.quantity() + " for dishId=" + item.dishId();
            }
        }
        return null;
    }

    private static ReasonCode parseReasonCode(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        try {
            return ReasonCode.valueOf(raw);
        } catch (IllegalArgumentException ignored) {
            return null;
        }
    }

    public enum Outcome {
        APPLIED,
        DUPLICATE_EVENT,
        DUPLICATE_ORDER
    }

    public record OrderProcessingResult(
            UUID eventId,
            UUID orderId,
            Outcome outcome,
            InventoryStatus status,
            ReasonCode reasonCode
    ) {
        public boolean isNewlyApplied() {
            return outcome == Outcome.APPLIED;
        }
    }
}
