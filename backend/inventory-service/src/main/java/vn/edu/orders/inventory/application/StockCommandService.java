package vn.edu.orders.inventory.application;

import java.time.Clock;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.ObjectMapper;
import vn.edu.orders.contracts.events.AlertKind;
import vn.edu.orders.contracts.events.EventEnvelope;
import vn.edu.orders.contracts.events.EventType;
import vn.edu.orders.contracts.events.InventoryStatus;
import vn.edu.orders.contracts.events.ReasonCode;
import vn.edu.orders.contracts.events.ReleaseRequestedPayload;
import vn.edu.orders.contracts.events.ReservationOutcome;
import vn.edu.orders.contracts.events.StockConsumedPayload;
import vn.edu.orders.inventory.config.InventoryProperties;
import vn.edu.orders.inventory.infrastructure.InventoryStore;
import vn.edu.orders.inventory.infrastructure.InventoryStore.InventoryOrderRow;
import vn.edu.orders.inventory.infrastructure.InventoryStore.ReservationLine;
import vn.edu.orders.inventory.infrastructure.InventoryStore.StockRow;

@Service
public class StockCommandService {
    private static final Logger log = LoggerFactory.getLogger(StockCommandService.class);

    private final InventoryStore store;
    private final DishStatusService dishStatusService;
    private final TransactionTemplate tx;
    private final ObjectMapper json;
    private final Clock clock;
    private final String instanceId;

    public StockCommandService(
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
        this.instanceId = properties.instanceId();
    }

    public CommandResult processStockConsumed(
            EventEnvelope<StockConsumedPayload> envelope,
            EventProcessingContext context
    ) {
        CommandResult result = tx.execute(status -> applyStockConsumed(envelope, context));
        if (result == null) {
            throw new IllegalStateException("StockConsumed transaction returned null");
        }
        return result;
    }

    public CommandResult processReleaseRequested(
            EventEnvelope<ReleaseRequestedPayload> envelope,
            EventProcessingContext context
    ) {
        CommandResult result = tx.execute(status -> applyReleaseRequested(envelope, context));
        if (result == null) {
            throw new IllegalStateException("ReleaseRequested transaction returned null");
        }
        return result;
    }

    private CommandResult applyStockConsumed(
            EventEnvelope<StockConsumedPayload> envelope,
            EventProcessingContext context
    ) {
        UUID eventId = envelope.eventId();
        UUID orderId = envelope.orderId();
        OffsetDateTime now = OffsetDateTime.ofInstant(clock.instant(), ZoneOffset.UTC).truncatedTo(ChronoUnit.MICROS);

        if (!store.tryInsertProcessedEvent(eventId, orderId, EventType.StockConsumed.name(), now)) {
            InventoryOrderRow existing = store.findOrder(orderId).orElse(null);
            log.info("Duplicate StockConsumed eventId ignored instanceId={} orderId={} eventId={} partition={} offset={}",
                    instanceId, orderId, eventId, context.partition(), context.offset());
            return new CommandResult(eventId, orderId, Outcome.DUPLICATE_EVENT,
                    existing == null ? null : existing.status());
        }

        if (!store.transitionOrderFromHeld(orderId, InventoryStatus.CONSUMED, null, now)) {
            return handleUnmatchedOrFinalizedCommand(orderId, eventId, EventType.StockConsumed, context, now);
        }

        List<ReservationLine> lines = store.findHeldReservationsInOrder(orderId);
        for (ReservationLine line : lines) {
            int updated = store.consumeReservedStock(line.ingredientId(), line.quantity(), now);
            if (updated != 1) {
                throw new IllegalStateException(
                        "Failed consuming reserved stock ingredientId=" + line.ingredientId()
                                + " orderId=" + orderId + " qty=" + line.quantity());
            }
        }
        store.markAllHeldReservationsOutcome(orderId, ReservationOutcome.CONSUMED);

        log.info("Inventory consumed reservation instanceId={} orderId={} eventId={} lines={}",
                instanceId, orderId, eventId, lines.size());
        return new CommandResult(eventId, orderId, Outcome.APPLIED, InventoryStatus.CONSUMED);
    }

    private CommandResult applyReleaseRequested(
            EventEnvelope<ReleaseRequestedPayload> envelope,
            EventProcessingContext context
    ) {
        UUID eventId = envelope.eventId();
        UUID orderId = envelope.orderId();
        ReleaseRequestedPayload payload = envelope.payload();
        ReasonCode reasonCode = payload.reasonCode();
        OffsetDateTime now = OffsetDateTime.ofInstant(clock.instant(), ZoneOffset.UTC).truncatedTo(ChronoUnit.MICROS);

        if (!store.tryInsertProcessedEvent(eventId, orderId, EventType.ReleaseRequested.name(), now)) {
            InventoryOrderRow existing = store.findOrder(orderId).orElse(null);
            log.info("Duplicate ReleaseRequested eventId ignored instanceId={} orderId={} eventId={} partition={} offset={}",
                    instanceId, orderId, eventId, context.partition(), context.offset());
            return new CommandResult(eventId, orderId, Outcome.DUPLICATE_EVENT,
                    existing == null ? null : existing.status());
        }

        if (!store.transitionOrderFromHeld(orderId, InventoryStatus.RELEASED, reasonCode.name(), now)) {
            return handleUnmatchedOrFinalizedCommand(orderId, eventId, EventType.ReleaseRequested, context, now);
        }

        List<ReservationLine> heldLines = store.findHeldReservationsInOrder(orderId);
        List<String> sortedIngredientIds = heldLines.stream().map(ReservationLine::ingredientId).toList();
        Map<String, StockRow> lockedBefore = store.lockStockRowsInOrder(sortedIngredientIds).stream()
                .collect(Collectors.toMap(StockRow::ingredientId, Function.identity()));

        Set<String> spoiledSet = reasonCode == ReasonCode.SPOILED_INGREDIENT
                ? Set.copyOf(payload.spoiledIngredients())
                : Set.of();

        List<String> newlyFlaggedIngredients = new ArrayList<>();
        for (ReservationLine line : heldLines) {
            String ingredientId = line.ingredientId();
            if (spoiledSet.contains(ingredientId)) {
                int updated = store.wasteAndFlagStock(ingredientId, line.quantity(), now);
                if (updated != 1) {
                    throw new IllegalStateException(
                            "Failed writing off spoiled stock ingredientId=" + ingredientId + " orderId=" + orderId);
                }
                store.markReservationOutcome(orderId, ingredientId, ReservationOutcome.WASTED);
                StockRow prev = lockedBefore.get(ingredientId);
                if (prev != null && !prev.needsCheck()) {
                    newlyFlaggedIngredients.add(ingredientId);
                }
            } else {
                int updated = store.releaseReservedStock(ingredientId, line.quantity(), now);
                if (updated != 1) {
                    throw new IllegalStateException(
                            "Failed releasing reserved stock ingredientId=" + ingredientId + " orderId=" + orderId);
                }
                store.markReservationOutcome(orderId, ingredientId, ReservationOutcome.RELEASED);
            }
        }

        // Lock order: inventory_orders -> stock -> dish_status -> menu_version
        dishStatusService.recomputeForIngredientsInTx(sortedIngredientIds, eventId, now);
        for (String flaggedId : newlyFlaggedIngredients) {
            dishStatusService.emitIngredientCheckChangedInTx(flaggedId, true, eventId, now);
        }

        log.info("Inventory released reservation instanceId={} orderId={} eventId={} reasonCode={} lines={} spoiled={}",
                instanceId, orderId, eventId, reasonCode, heldLines.size(), spoiledSet);
        return new CommandResult(eventId, orderId, Outcome.APPLIED, InventoryStatus.RELEASED);
    }

    private CommandResult handleUnmatchedOrFinalizedCommand(
            UUID orderId,
            UUID eventId,
            EventType eventType,
            EventProcessingContext context,
            OffsetDateTime now
    ) {
        InventoryOrderRow existing = store.findOrder(orderId).orElse(null);
        if (existing == null || existing.status() == InventoryStatus.FAILED) {
            String storedStatus = existing == null ? "NOT_FOUND" : existing.status().name();
            String detailJson = json.writeValueAsString(Map.of(
                    "eventType", eventType.name(),
                    "orderStatus", storedStatus,
                    "message", "Stock command arrived for missing or FAILED order"));
            store.insertAlertIdempotent(
                    AlertKind.UNMATCHED_COMMAND,
                    orderId,
                    eventId,
                    context.topic(),
                    context.partition(),
                    context.offset(),
                    detailJson,
                    now);
            log.warn("Unmatched stock command ignored instanceId={} orderId={} eventId={} eventType={} existingStatus={} topic={} partition={} offset={}",
                    instanceId, orderId, eventId, eventType, storedStatus,
                    context.topic(), context.partition(), context.offset());
            return new CommandResult(eventId, orderId, Outcome.UNMATCHED, existing == null ? null : existing.status());
        }

        // RELEASED or CONSUMED -> duplicate/late command, ignore cleanly without alert
        log.info("Stock command ignored for finalized order instanceId={} orderId={} eventId={} eventType={} existingStatus={}",
                instanceId, orderId, eventId, eventType, existing.status());
        return new CommandResult(eventId, orderId, Outcome.IGNORED_ALREADY_FINALIZED, existing.status());
    }

    public enum Outcome {
        APPLIED,
        DUPLICATE_EVENT,
        UNMATCHED,
        IGNORED_ALREADY_FINALIZED
    }

    public record CommandResult(
            UUID eventId,
            UUID orderId,
            Outcome outcome,
            InventoryStatus orderStatus
    ) {
    }
}
