package vn.edu.orders.inventory.infrastructure;

import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import vn.edu.orders.inventory.domain.Ingredient;

public interface IngredientRepository extends JpaRepository<Ingredient, String> {
    List<Ingredient> findAllByOrderByIngredientIdAsc();
}
