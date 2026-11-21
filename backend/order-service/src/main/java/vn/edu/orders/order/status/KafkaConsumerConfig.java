package vn.edu.orders.order.status;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.CommonDelegatingErrorHandler;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.FixedBackOff;
import tools.jackson.core.JacksonException;
import vn.edu.orders.contracts.events.EventTopics;
import vn.edu.orders.contracts.events.InvalidEventException;
import vn.edu.orders.order.config.OrderLifecycleProperties;
import vn.edu.orders.order.config.SiteProperties;

@Configuration
public class KafkaConsumerConfig {

    @Bean("orderStatusTopic")
    public String orderStatusTopic(SiteProperties siteProperties) {
        return EventTopics.statusEventsTopic(siteProperties.id());
    }

    @Bean("orderStatusGroupId")
    public String orderStatusGroupId(SiteProperties siteProperties) {
        return "order-status-" + siteProperties.id() + "-v1";
    }

    @Bean("orderMenuGroupId")
    public String orderMenuGroupId(SiteProperties siteProperties) {
        return "order-menu-" + siteProperties.id() + "-v1";
    }

    @Bean
    CommonErrorHandler orderConsumerErrorHandler(
            DlqPublisher dlqPublisher,
            OrderLifecycleProperties properties
    ) {
        ExponentialBackOff transientBackOff = new ExponentialBackOff(1_000L, 2.0);
        transientBackOff.setMaxInterval(30_000L);
        transientBackOff.setMaxElapsedTime(Long.MAX_VALUE);
        transientBackOff.setMaxAttempts(Long.MAX_VALUE);
        DefaultErrorHandler transientErrorHandler = new DefaultErrorHandler(transientBackOff);

        long retriesBeforeDlq = Math.max(0L, properties.dlqMaxAttempts() - 1L);
        DefaultErrorHandler dlqErrorHandler = new DefaultErrorHandler(
                dlqPublisher,
                new FixedBackOff(50L, retriesBeforeDlq)
        );

        CommonDelegatingErrorHandler delegating = new CommonDelegatingErrorHandler(transientErrorHandler);
        delegating.setCauseChainTraversing(true);
        delegating.addDelegate(InvalidEventException.class, dlqErrorHandler);
        delegating.addDelegate(JacksonException.class, dlqErrorHandler);
        return delegating;
    }
}
