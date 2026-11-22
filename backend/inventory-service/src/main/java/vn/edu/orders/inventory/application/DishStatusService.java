package vn.edu.orders.inventory.application;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.ObjectMapper;
import vn.edu.orders.contracts.events.DishStatus;
import vn.edu.orders.contracts.events.DishStatusChangedPayload;
import vn.edu.orders.contracts.events.EventEnvelope;
import vn.edu.orders.contracts.events.EventTopics;
import vn.edu.orders.contracts.events.EventType;
import vn.edu.orders.contracts.events.HeartbeatPayload;
import vn.edu.orders.contracts.events.IngredientCheckChangedPayload;
import vn.edu.orders.contracts.events.MenuVersion;
import vn.edu.orders.inventory.config.InventoryProperties;
import vn.edu.orders.inventory.infrastructure.InventoryStore;
import vn.edu.orders.inventory.infrastructure.InventoryStore.DishStatusRow;
import vn.edu.orders.inventory.infrastructure.InventoryStore.RecipeStockView;
import vn.edu.orders.inventory.infrastructure.InventoryStore.StockRow;

@Service
public class DishStatusService {
    private static final Logger log = LoggerFactory.getLogger(DishStatusService.class);
    private static final String INVENTORY_BRANCH = "INVENTORY";

    private final InventoryStore store;
    private final TransactionTemplate tx;
    private final ObjectMapper json;
    private final Clock clock;
    private final String instanceId;

    public DishStatusService(
            InventoryStore store,
            PlatformTransactionManager transactionManager,
            ObjectMapper json,
            Clock clock,
            InventoryProperties properties
    ) {
        this.store = store;
        this.tx = new TransactionTemplate(transactionManager);
        this.tx.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        this.json = json;
        this.clock = clock;
        this.instanceId = properties.instanceId();
    }

    /**
     * Recomputes status for dishes using any of the changed ingredients inside the caller's transaction.
     * Enforces lock order: stock -> dish_status (ORDER BY dish_id ASC) -> menu_version.
     */
    public void recomputeForIngredientsInTx(
            Collection<String> changedIngredientIds,
            UUID causationId,
            OffsetDateTime now
    ) {
        if (changedIngredientIds == null || changedIngredientIds.isEmpty()) {
            return;
        }
        List<String> affectedDishIds = store.findAffectedDishes(changedIngredientIds);
        recomputeDishesInTx(affectedDishIds, causationId, now);
    }

    /**
     * Recomputes status for an explicit set of dishes inside the caller's transaction.
     * Locks dish_status rows BEFORE reading stock!
     */
    public void recomputeDishesInTx(
            Collection<String> dishIds,
            UUID causationId,
            OffsetDateTime now
    ) {
        if (dishIds == null || dishIds.isEmpty()) {
            return;
        }
        List<DishStatusRow> lockedDishes = store.lockDishStatusesInOrder(dishIds);
        if (lockedDishes.isEmpty()) {
            return;
        }
        List<String> lockedIds = lockedDishes.stream().map(DishStatusRow::dishId).toList();
        Map<String, List<RecipeStockView>> recipesByDish = store.readRecipesWithStockForDishes(lockedIds).stream()
                .collect(Collectors.groupingBy(RecipeStockView::dishId));

        for (DishStatusRow current : lockedDishes) {
            DishStatus computed = computeStatus(recipesByDish.get(current.dishId()));
            if (computed != current.status()) {
                MenuVersion version = store.incrementSeq();
                store.updateDishStatus(current.dishId(), computed, version.epoch(), version.seq());

                UUID outboxEventId = UUID.randomUUID();
                var envelope = EventEnvelope.v1Menu(
                        outboxEventId,
                        EventType.DishStatusChanged,
                        INVENTORY_BRANCH,
                        now.toInstant(),
                        causationId,
                        new DishStatusChangedPayload(current.dishId(), computed, version.epoch(), version.seq()));
                store.insertOutboxEvent(
                        outboxEventId,
                        null,
                        EventType.DishStatusChanged,
                        EventTopics.MENU_EVENTS,
                        current.dishId(),
                        json.writeValueAsString(envelope),
                        now);
                log.info("Dish status changed instanceId={} dishId={} from={} to={} epoch={} seq={}",
                        instanceId, current.dishId(), current.status(), computed, version.epoch(), version.seq());
            }
        }
    }

    /**
     * Emits IngredientCheckChanged inside the caller's transaction, strictly after dish_status locks
     * so menu_version stays last in the lock order.
     */
    public void emitIngredientCheckChangedInTx(
            String ingredientId,
            boolean needsCheck,
            UUID causationId,
            OffsetDateTime now
    ) {
        MenuVersion version = store.incrementSeq();
        UUID outboxEventId = UUID.randomUUID();
        var envelope = EventEnvelope.v1Menu(
                outboxEventId,
                EventType.IngredientCheckChanged,
                INVENTORY_BRANCH,
                now.toInstant(),
                causationId,
                new IngredientCheckChangedPayload(ingredientId, needsCheck, version.epoch(), version.seq()));
        store.insertOutboxEvent(
                outboxEventId,
                null,
                EventType.IngredientCheckChanged,
                EventTopics.MENU_EVENTS,
                ingredientId,
                json.writeValueAsString(envelope),
                now);
        log.info("Ingredient check changed instanceId={} ingredientId={} needsCheck={} epoch={} seq={}",
                instanceId, ingredientId, needsCheck, version.epoch(), version.seq());
    }

    /**
     * Executes the epoch bump procedure per 02 §3:
     * locks all dish_status rows -> bumps (epoch+1, seq+1) -> reads stock -> rewrites every dish_status ->
     * emits IngredientCheckChanged for every ingredient (no DishStatusChanged emitted).
     */
    public MenuVersion bumpEpoch() {
        MenuVersion result = tx.execute(status -> {
            OffsetDateTime now = OffsetDateTime.ofInstant(clock.instant(), ZoneOffset.UTC).truncatedTo(ChronoUnit.MICROS);
            List<DishStatusRow> allDishes = store.lockDishStatusesInOrder(null);
            MenuVersion version = store.incrementEpochAndSeq();

            if (!allDishes.isEmpty()) {
                List<String> allDishIds = allDishes.stream().map(DishStatusRow::dishId).toList();
                Map<String, List<RecipeStockView>> recipesByDish = store.readRecipesWithStockForDishes(allDishIds).stream()
                        .collect(Collectors.groupingBy(RecipeStockView::dishId));

                for (DishStatusRow dish : allDishes) {
                    DishStatus computed = computeStatus(recipesByDish.get(dish.dishId()));
                    store.updateDishStatus(dish.dishId(), computed, version.epoch(), version.seq());
                }
            }

            List<StockRow> allStock = store.findAllStockOrdered(null);
            for (StockRow stockRow : allStock) {
                UUID outboxEventId = UUID.randomUUID();
                var envelope = EventEnvelope.v1Menu(
                        outboxEventId,
                        EventType.IngredientCheckChanged,
                        INVENTORY_BRANCH,
                        now.toInstant(),
                        null,
                        new IngredientCheckChangedPayload(
                                stockRow.ingredientId(), stockRow.needsCheck(), version.epoch(), version.seq()));
                store.insertOutboxEvent(
                        outboxEventId,
                        null,
                        EventType.IngredientCheckChanged,
                        EventTopics.MENU_EVENTS,
                        stockRow.ingredientId(),
                        json.writeValueAsString(envelope),
                        now);
            }
            log.info("Bumped inventory epoch instanceId={} epoch={} seq={} dishes={} ingredients={}",
                    instanceId, version.epoch(), version.seq(), allDishes.size(), allStock.size());
            return version;
        });
        if (result == null) {
            throw new IllegalStateException("Epoch bump transaction returned null");
        }
        return result;
    }

    public MenuSnapshot getSnapshot() {
        MenuSnapshot snapshot = tx.execute(status -> {
            MenuVersion version = store.readMenuVersion();
            List<DishStatusRow> dishes = store.findAllDishStatusesOrdered();
            return new MenuSnapshot(version.epoch(), version.seq(), dishes);
        });
        if (snapshot == null) {
            throw new IllegalStateException("Menu snapshot transaction returned null");
        }
        return snapshot;
    }

    public MenuVersion emitHeartbeat() {
        MenuVersion emitted = tx.execute(status -> {
            OffsetDateTime now = OffsetDateTime.ofInstant(clock.instant(), ZoneOffset.UTC).truncatedTo(ChronoUnit.MICROS);
            MenuVersion version = store.readMenuVersion();
            UUID outboxEventId = UUID.randomUUID();
            var envelope = EventEnvelope.v1Menu(
                    outboxEventId,
                    EventType.Heartbeat,
                    INVENTORY_BRANCH,
                    now.toInstant(),
                    null,
                    new HeartbeatPayload(version.epoch(), version.seq()));
            store.insertOutboxEvent(
                    outboxEventId,
                    null,
                    EventType.Heartbeat,
                    EventTopics.MENU_EVENTS,
                    EventTopics.HEARTBEAT_KEY,
                    json.writeValueAsString(envelope),
                    now);
            return version;
        });
        if (emitted == null) {
            throw new IllegalStateException("Heartbeat transaction returned null");
        }
        return emitted;
    }

    static DishStatus computeStatus(List<RecipeStockView> recipeLines) {
        if (recipeLines == null || recipeLines.isEmpty()) {
            return DishStatus.HIDDEN;
        }
        for (RecipeStockView line : recipeLines) {
            if (line.needsCheck()) {
                return DishStatus.SOLD_OUT;
            }
            BigDecimal available = line.available() == null ? BigDecimal.ZERO : line.available();
            if (available.compareTo(line.requiredQuantity()) < 0) {
                return DishStatus.SOLD_OUT;
            }
        }
        return DishStatus.AVAILABLE;
    }

    public record MenuSnapshot(int epoch, long seq, List<DishStatusRow> dishes) {
    }
}
