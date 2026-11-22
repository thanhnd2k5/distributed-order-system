package vn.edu.orders.contracts.events;

public enum ReasonCode {
    INSUFFICIENT_INGREDIENTS(Category.INVENTORY_FAILED, "Insufficient ingredients"),
    INGREDIENT_AWAITING_STOCK_CHECK(Category.INVENTORY_FAILED, "Ingredient awaiting stock check"),
    DISH_HAS_NO_RECIPE(Category.INVENTORY_FAILED, "Dish has no recipe"),
    ORDER_EXPIRED(Category.INVENTORY_FAILED, "Order expired"),
    INVALID_CREATION_TIME(Category.INVENTORY_FAILED, "Invalid creation time (in the future)"),
    INVALID_ORDER_DATA(Category.INVENTORY_FAILED, "Invalid order data"),

    EQUIPMENT_FAILURE(Category.KITCHEN_REJECTION, "Equipment failure"),
    SHORT_STAFFED(Category.KITCHEN_REJECTION, "Short-staffed"),
    SPOILED_INGREDIENT(Category.KITCHEN_REJECTION, "Spoiled ingredient"),

    CUSTOMER_CHANGED_MIND(Category.CUSTOMER_CANCEL, "Customer changed their mind"),
    CUSTOMER_WAITED_TOO_LONG(Category.CUSTOMER_CANCEL, "Customer waited too long"),
    WRONG_ORDER_ENTERED(Category.CUSTOMER_CANCEL, "Cashier entered the wrong order");

    public enum Category {
        INVENTORY_FAILED,
        KITCHEN_REJECTION,
        CUSTOMER_CANCEL
    }

    private final Category category;
    private final String defaultMessage;

    ReasonCode(Category category, String defaultMessage) {
        this.category = category;
        this.defaultMessage = defaultMessage;
    }

    public Category category() {
        return category;
    }

    public String defaultMessage() {
        return defaultMessage;
    }

    public boolean isInventoryFailure() {
        return category == Category.INVENTORY_FAILED;
    }

    public boolean isKitchenRejection() {
        return category == Category.KITCHEN_REJECTION;
    }

    public boolean isCustomerCancel() {
        return category == Category.CUSTOMER_CANCEL;
    }

    public boolean requiresSpoiledIngredients() {
        return this == SPOILED_INGREDIENT;
    }
}
