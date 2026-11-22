package vn.edu.orders.contracts.events;

@Deprecated
public enum FailureReasonCode {
    INSUFFICIENT_INGREDIENT,
    RECIPE_NOT_FOUND;

    public ReasonCode toReasonCode() {
        return switch (this) {
            case INSUFFICIENT_INGREDIENT -> ReasonCode.INSUFFICIENT_INGREDIENTS;
            case RECIPE_NOT_FOUND -> ReasonCode.DISH_HAS_NO_RECIPE;
        };
    }
}
