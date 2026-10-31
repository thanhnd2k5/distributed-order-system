package vn.edu.orders.order.menu.api;

import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import vn.edu.orders.order.menu.infrastructure.MenuItemRepository;

@RestController
@RequestMapping("/api/menu")
public class MenuController {
    private final MenuItemRepository menuItems;

    public MenuController(MenuItemRepository menuItems) {
        this.menuItems = menuItems;
    }

    @GetMapping
    public List<MenuItemResponse> getMenu() {
        return menuItems.findByActiveTrueOrderByDishIdAsc().stream()
                .map(item -> new MenuItemResponse(item.getDishId(), item.getName()))
                .toList();
    }
}
