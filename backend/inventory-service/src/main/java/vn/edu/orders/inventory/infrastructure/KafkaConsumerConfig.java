package vn.edu.orders.inventory.infrastructure;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.CommonContainerStoppingErrorHandler;
import org.springframework.kafka.listener.CommonDelegatingErrorHandler;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;
import tools.jackson.core.JacksonException;
import vn.edu.orders.inventory.application.InvalidEventException;

@Configuration
public class KafkaConsumerConfig {

    @Bean
    CommonErrorHandler inventoryErrorHandler() {
        ExponentialBackOff transientBackOff = new ExponentialBackOff(1_000L, 2.0);
        transientBackOff.setMaxInterval(30_000L);
        transientBackOff.setMaxElapsedTime(Long.MAX_VALUE);
        transientBackOff.setMaxAttempts(Long.MAX_VALUE);

        DefaultErrorHandler transientErrorHandler = new DefaultErrorHandler(transientBackOff);
        CommonContainerStoppingErrorHandler poisonMessageHandler = new CommonContainerStoppingErrorHandler();

        CommonDelegatingErrorHandler delegating = new CommonDelegatingErrorHandler(transientErrorHandler);
        delegating.setCauseChainTraversing(true);
        delegating.addDelegate(InvalidEventException.class, poisonMessageHandler);
        delegating.addDelegate(JacksonException.class, poisonMessageHandler);
        return delegating;
    }
}
