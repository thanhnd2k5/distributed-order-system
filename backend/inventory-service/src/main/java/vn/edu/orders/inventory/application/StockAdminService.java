package vn.edu.orders.inventory.application;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.server.ResponseStatusException;
import vn.edu.orders.inventory.config.InventoryProperties;
import vn.edu.orders.inventory.infrastructure.InventoryStore;
import vn.edu.orders.inventory.infrastructure.InventoryStore.StockRow;

@Service
public class StockAdminService {
    private static final Logger log = LoggerFactory.getLogger(StockAdminService.class);

    private final InventoryStore store;
    private final DishStatusService dishStatusService;
    private final TransactionTemplate tx;
    private final Clock clock;
    private final String instanceId;

    public StockAdminService(
            InventoryStore store,
            DishStatusService dishStatusService,
            PlatformTransactionManager transactionManager,
            Clock clock,
            InventoryProperties properties
    ) {
        this.store = store;
        this.dishStatusService = dishStatusService;
        this.tx = new TransactionTemplate(transactionManager);
        this.tx.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        this.clock = clock;
        this.instanceId = properties.instanceId();
    }

    /**
     * Clears needs_check for an ingredient and optionally reconciles available = countedOnHand - reserved.
     * Returns 409 CONFLICT if countedOnHand < reserved, 404 if unknown ingredient, 400 if negative.
     */
    public StockRow clearCheck(String ingredientId, BigDecimal countedOnHand) {
        if (ingredientId == null || ingredientId.isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "ingredientId must not be blank");
        }
        if (countedOnHand != null && countedOnHand.compareTo(BigDecimal.ZERO) < 0) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "countedOnHand must not be negative");
        }

        StockRow result = tx.execute(status -> {
            OffsetDateTime now = OffsetDateTime.ofInstant(clock.instant(), ZoneOffset.UTC).truncatedTo(ChronoUnit.MICROS);
            StockRow current = store.lockStockRow(ingredientId)
                    .orElseThrow(() -> new ResponseStatusException(
                            HttpStatus.NOT_FOUND, "Ingredient not found: " + ingredientId));

            BigDecimal newAvailable = null;
            if (countedOnHand != null) {
                BigDecimal adjusted = countedOnHand.subtract(current.reserved());
                if (adjusted.compareTo(BigDecimal.ZERO) < 0) {
                    throw new ResponseStatusException(
                            HttpStatus.CONFLICT,
                            "Counted on-hand stock (" + countedOnHand + ") is less than reserved quantity ("
                                    + current.reserved() + ") for ingredient " + ingredientId);
                }
                newAvailable = adjusted;
            }

            StockRow updated = store.clearCheck(ingredientId, newAvailable, now)
                    .orElseThrow(() -> new IllegalStateException("Ingredient vanished during clear-check: " + ingredientId));

            // Lock order: stock -> dish_status -> menu_version
            dishStatusService.recomputeForIngredientsInTx(List.of(ingredientId), null, now);
            if (current.needsCheck()) {
                dishStatusService.emitIngredientCheckChangedInTx(ingredientId, false, null, now);
            }

            log.info("Cleared stock check instanceId={} ingredientId={} countedOnHand={} newAvailable={} reserved={}",
                    instanceId, ingredientId, countedOnHand, updated.available(), updated.reserved());
            return updated;
        });
        if (result == null) {
            throw new IllegalStateException("clearCheck transaction returned null");
        }
        return result;
    }

    /** Adds received stock to available quantity and recomputes affected dish statuses in the same tx. */
    public StockRow receiveStock(String ingredientId, BigDecimal quantity) {
        if (ingredientId == null || ingredientId.isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "ingredientId must not be blank");
        }
        if (quantity == null || quantity.compareTo(BigDecimal.ZERO) <= 0) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "quantity must be greater than 0");
        }

        StockRow result = tx.execute(status -> {
            OffsetDateTime now = OffsetDateTime.ofInstant(clock.instant(), ZoneOffset.UTC).truncatedTo(ChronoUnit.MICROS);
            StockRow current = store.lockStockRow(ingredientId)
                    .orElseThrow(() -> new ResponseStatusException(
                            HttpStatus.NOT_FOUND, "Ingredient not found: " + ingredientId));

            StockRow updated = store.receiveStock(ingredientId, quantity, now)
                    .orElseThrow(() -> new IllegalStateException("Ingredient vanished during receipt: " + ingredientId));

            dishStatusService.recomputeForIngredientsInTx(List.of(current.ingredientId()), null, now);
            log.info("Received stock instanceId={} ingredientId={} added={} newAvailable={}",
                    instanceId, ingredientId, quantity, updated.available());
            return updated;
        });
        if (result == null) {
            throw new IllegalStateException("receiveStock transaction returned null");
        }
        return result;
    }

    /** Replaces or creates a recipe for a dish and recomputes dish status (Flow C). */
    public void upsertRecipe(String dishId, String dishName, Map<String, BigDecimal> ingredients) {
        if (dishId == null || dishId.isBlank() || ingredients == null || ingredients.isEmpty()) {
            throw new IllegalArgumentException("dishId and non-empty ingredients required");
        }
        tx.executeWithoutResult(status -> {
            OffsetDateTime now = OffsetDateTime.ofInstant(clock.instant(), ZoneOffset.UTC).truncatedTo(ChronoUnit.MICROS);
            var currentVersion = store.readMenuVersion();
            store.ensureDishStatusRowExists(dishId, dishName, currentVersion.epoch(), currentVersion.seq());
            store.deleteRecipesForDish(dishId);
            for (Map.Entry<String, BigDecimal> entry : ingredients.entrySet()) {
                store.insertRecipeLine(dishId, entry.getKey(), entry.getValue());
            }
            dishStatusService.recomputeDishesInTx(List.of(dishId), UUID.randomUUID(), now);
        });
    }

    /** Removes a dish by deleting its recipe (§3.4), moving its dish_status to HIDDEN. */
    public void deleteRecipe(String dishId) {
        if (dishId == null || dishId.isBlank()) {
            throw new IllegalArgumentException("dishId must not be blank");
        }
        tx.executeWithoutResult(status -> {
            OffsetDateTime now = OffsetDateTime.ofInstant(clock.instant(), ZoneOffset.UTC).truncatedTo(ChronoUnit.MICROS);
            store.deleteRecipesForDish(dishId);
            dishStatusService.recomputeDishesInTx(List.of(dishId), UUID.randomUUID(), now);
        });
    }
}
