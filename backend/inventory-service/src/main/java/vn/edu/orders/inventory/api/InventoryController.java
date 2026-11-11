package vn.edu.orders.inventory.api;

import java.util.List;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import vn.edu.orders.inventory.infrastructure.IngredientRepository;
import vn.edu.orders.inventory.infrastructure.InventoryQueryStore;

@RestController
@RequestMapping("/api")
public class InventoryController {
    private final IngredientRepository ingredients;
    private final InventoryQueryStore queryStore;

    public InventoryController(IngredientRepository ingredients, InventoryQueryStore queryStore) {
        this.ingredients = ingredients;
        this.queryStore = queryStore;
    }

    @GetMapping("/ingredients")
    public List<IngredientResponse> listIngredients() {
        return ingredients.findAllByOrderByIngredientIdAsc().stream()
                .map(i -> new IngredientResponse(
                        i.getIngredientId(),
                        i.getName(),
                        i.getUnit(),
                        i.getAvailableQuantity(),
                        i.getUpdatedAt()))
                .toList();
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
