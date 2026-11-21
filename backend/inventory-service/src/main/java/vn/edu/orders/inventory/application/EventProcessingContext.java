package vn.edu.orders.inventory.application;

public record EventProcessingContext(
        String topic,
        int partition,
        long offset,
        long timestampMs,
        String kafkaKey
) {
    public static EventProcessingContext direct(String topic) {
        return new EventProcessingContext(topic, -1, -1L, 0L, null);
    }

    public static EventProcessingContext direct(String topic, int partition, long offset) {
        return new EventProcessingContext(topic, partition, offset, 0L, null);
    }
}
