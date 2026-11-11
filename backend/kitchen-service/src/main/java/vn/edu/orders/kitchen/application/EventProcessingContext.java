package vn.edu.orders.kitchen.application;

public record EventProcessingContext(int partition, long offset) {
    public static EventProcessingContext direct() {
        return new EventProcessingContext(-1, -1L);
    }
}
