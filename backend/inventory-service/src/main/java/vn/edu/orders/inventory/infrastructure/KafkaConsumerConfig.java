package vn.edu.orders.inventory.infrastructure;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

@Configuration
public class KafkaConsumerConfig {

    @Bean
    CommonErrorHandler inventoryErrorHandler() {
        // Transient DB or Kafka DLQ-publish errors retry indefinitely with exponential backoff
        // without committing offsets. Poison/unknown events are moved to DLQ inside the consumers.
        ExponentialBackOff transientBackOff = new ExponentialBackOff(1_000L, 2.0);
        transientBackOff.setMaxInterval(30_000L);
        transientBackOff.setMaxElapsedTime(Long.MAX_VALUE);
        transientBackOff.setMaxAttempts(Long.MAX_VALUE);
        return new DefaultErrorHandler(transientBackOff);
    }
}
