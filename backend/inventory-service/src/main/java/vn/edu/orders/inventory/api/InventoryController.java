package vn.edu.orders.inventory.api;

import jakarta.validation.Valid;
import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import vn.edu.orders.contracts.events.AlertKind;
import vn.edu.orders.contracts.events.MenuVersion;
import vn.edu.orders.inventory.api.MenuSnapshotResponse.DishSnapshotItem;
import vn.edu.orders.inventory.application.DishStatusService;
import vn.edu.orders.inventory.application.StockAdminService;
import vn.edu.orders.inventory.infrastructure.InventoryMonitorService;
import vn.edu.orders.inventory.infrastructure.InventoryQueryStore;

@RestController
@RequestMapping("/api")
public class InventoryController {
    private final InventoryQueryStore queryStore;
    private final StockAdminService stockAdminService;
    private final DishStatusService dishStatusService;
    private final InventoryMonitorService monitorService;

    public InventoryController(
            InventoryQueryStore queryStore,
            StockAdminService stockAdminService,
            DishStatusService dishStatusService,
            InventoryMonitorService monitorService
    ) {
        this.queryStore = queryStore;
        this.stockAdminService = stockAdminService;
        this.dishStatusService = dishStatusService;
        this.monitorService = monitorService;
    }

    @GetMapping("/stock")
    public List<StockResponse> listStock(@RequestParam(required = false) Boolean needsCheck) {
        return queryStore.findStock(needsCheck);
    }

    @PostMapping("/stock/receipts")
    public StockResponse receiveStock(@Valid @RequestBody StockReceiptRequest request) {
        return StockResponse.fromRow(stockAdminService.receiveStock(request.ingredientId(), request.quantity()));
    }

    @PostMapping("/ingredients/{id}/clear-check")
    public StockResponse clearCheck(
            @PathVariable String id,
            @RequestBody(required = false) ClearCheckRequest request
    ) {
        BigDecimal countedOnHand = request == null ? null : request.countedOnHand();
        return StockResponse.fromRow(stockAdminService.clearCheck(id, countedOnHand));
    }

    @GetMapping("/menu/snapshot")
    public MenuSnapshotResponse getMenuSnapshot() {
        var snapshot = dishStatusService.getSnapshot();
        List<DishSnapshotItem> dishes = snapshot.dishes().stream()
                .map(d -> new DishSnapshotItem(d.dishId(), d.dishName(), d.status(), d.epoch(), d.seq()))
                .toList();
        return new MenuSnapshotResponse(snapshot.epoch(), snapshot.seq(), dishes);
    }

    @PostMapping("/admin/epoch/bump")
    public EpochBumpResponse bumpEpoch() {
        MenuVersion bumped = dishStatusService.bumpEpoch();
        return new EpochBumpResponse(bumped.epoch(), bumped.seq());
    }

    @GetMapping("/alerts")
    public List<AlertResponse> listAlerts(@RequestParam(required = false) AlertKind kind) {
        return queryStore.findAlerts(kind);
    }

    @GetMapping("/monitor")
    public MonitorResponse getMonitor() {
        return monitorService.getMonitorStatus();
    }

    @GetMapping("/ingredients")
    public List<IngredientResponse> listIngredients() {
        return queryStore.findIngredients();
    }

    @GetMapping("/recipes")
    public List<RecipeResponse> listRecipes() {
        return queryStore.findRecipes();
    }

    @GetMapping("/reservations/{orderId}")
    public ReservationResponse getReservation(@PathVariable UUID orderId) {
        return queryStore.findReservation(orderId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Reservation not found"));
    }
}
