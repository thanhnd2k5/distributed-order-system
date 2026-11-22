package vn.edu.orders.contracts.events;

public enum KitchenStatus {
    NOT_EXISTS,
    CANCEL_PENDING,
    QUEUED,
    COOKING,
    DONE,
    CANCELLED,
    FAILED;

    public boolean isTerminal() {
        return this == DONE || this == CANCELLED || this == FAILED;
    }

    public boolean isActiveTicket() {
        return this == QUEUED || this == COOKING;
    }

    public boolean isRejectableForCancel() {
        return this == COOKING || this == DONE;
    }
}
