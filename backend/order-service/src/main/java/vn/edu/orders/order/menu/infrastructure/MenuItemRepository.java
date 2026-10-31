package vn.edu.orders.order.menu.infrastructure;

import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import vn.edu.orders.order.menu.domain.MenuItem;

public interface MenuItemRepository extends JpaRepository<MenuItem, String> {
    List<MenuItem> findByActiveTrueOrderByDishIdAsc();
}
