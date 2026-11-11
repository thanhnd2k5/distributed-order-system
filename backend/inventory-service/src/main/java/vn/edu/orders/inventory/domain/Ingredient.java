package vn.edu.orders.inventory.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;

@Entity
@Table(name = "ingredients")
public class Ingredient {
    @Id
    @Column(name = "ingredient_id", length = 50, nullable = false)
    private String ingredientId;

    @Column(nullable = false, length = 200)
    private String name;

    @Column(nullable = false, length = 20)
    private String unit;

    @Column(name = "available_quantity", nullable = false)
    private long availableQuantity;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    protected Ingredient() {
    }

    public String getIngredientId() {
        return ingredientId;
    }

    public String getName() {
        return name;
    }

    public String getUnit() {
        return unit;
    }

    public long getAvailableQuantity() {
        return availableQuantity;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }
}
