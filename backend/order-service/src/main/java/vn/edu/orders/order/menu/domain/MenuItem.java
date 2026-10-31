package vn.edu.orders.order.menu.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "menu_items")
public class MenuItem {
    @Id
    @Column(name = "dish_id", length = 50, nullable = false)
    private String dishId;

    @Column(nullable = false, length = 200)
    private String name;

    @Column(nullable = false)
    private boolean active;

    protected MenuItem() {
    }

    public String getDishId() {
        return dishId;
    }

    public String getName() {
        return name;
    }
}
