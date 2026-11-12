package vn.edu.orders.order.status;

public record StatusEventContext(int partition, long offset) {
    public static StatusEventContext direct() {
        return new StatusEventContext(-1, -1L);
    }
}
