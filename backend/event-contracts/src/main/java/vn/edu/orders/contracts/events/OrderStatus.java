package vn.edu.orders.contracts.events;

public enum OrderStatus {
    PENDING_INVENTORY(0),
    RESERVED(1),
    DONE(2),
    CANCELLED(2),
    FAILED(2);

    private final int statusRank;

    OrderStatus(int statusRank) {
        this.statusRank = statusRank;
    }

    public int statusRank() {
        return statusRank;
    }

    public boolean isTerminal() {
        return statusRank == 2;
    }

    public boolean canTransitionFrom(OrderStatus current) {
        return current == null || this.statusRank > current.statusRank;
    }
}
