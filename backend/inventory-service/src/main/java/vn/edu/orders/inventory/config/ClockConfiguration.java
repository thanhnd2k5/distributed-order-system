package vn.edu.orders.inventory.config;

import java.time.Clock;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ClockConfiguration {
    @Bean
    @ConditionalOnMissingBean
    public Clock inventoryClock() {
        return Clock.systemUTC();
    }
}
