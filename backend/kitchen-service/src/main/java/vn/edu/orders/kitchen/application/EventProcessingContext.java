package vn.edu.orders.kitchen.application;

import java.util.concurrent.atomic.AtomicLong;

public record EventProcessingContext(String sourceTopic, int partition, long offset) {
    private static final AtomicLong SYNTHETIC_OFFSET = new AtomicLong(-1L);

    public EventProcessingContext(int partition, long offset) {
        this("direct", partition, offset);
    }

    public static EventProcessingContext direct() {
        return new EventProcessingContext("direct", -1, SYNTHETIC_OFFSET.getAndDecrement());
    }

    public static EventProcessingContext direct(String sourceTopic) {
        return new EventProcessingContext(sourceTopic, -1, SYNTHETIC_OFFSET.getAndDecrement());
    }
}
