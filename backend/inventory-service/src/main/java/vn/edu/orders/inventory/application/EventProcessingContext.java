package vn.edu.orders.inventory.application;

public record EventProcessingContext(int partition, long offset) {
    public static final EventProcessingContext DIRECT = new EventProcessingContext(-1, -1L);
}
