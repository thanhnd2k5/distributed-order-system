package vn.edu.orders.order.outbox;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

@Component
@EnableScheduling
@ConditionalOnProperty(name = "outbox.enabled", havingValue = "true")
class OutboxPublisher {
    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);
    private static final int BATCH_LIMIT = 10;

    private final JdbcClient jdbc;
    private final TransactionTemplate transaction;
    private final KafkaTemplate<String, String> kafka;
    private final String outbox;

    OutboxPublisher(JdbcClient jdbc, TransactionTemplate transaction, KafkaTemplate<String, String> kafka,
                    @Value("${spring.flyway.default-schema}") String schema) {
        this.jdbc = jdbc;
        this.transaction = transaction;
        this.kafka = kafka;
        this.outbox = "\"" + schema.replace("\"", "\"\"") + "\".outbox_events";
    }

    @Scheduled(fixedDelay = 500)
    void publishPending() {
        for (int i = 0; i < BATCH_LIMIT; i++) {
            if (!Boolean.TRUE.equals(transaction.execute(status -> publishOne()))) {
                return;
            }
        }
    }

    private boolean publishOne() {
        var pending = jdbc.sql("""
                SELECT event_id, topic, message_key, payload::text, attempt_count
                FROM %s
                WHERE sent_at IS NULL AND next_attempt_at <= now()
                ORDER BY created_at, event_id
                LIMIT 1 FOR UPDATE SKIP LOCKED
                """.formatted(outbox))
                .query((rs, row) -> new PendingEvent(rs.getObject("event_id", UUID.class),
                        rs.getString("topic"), rs.getString("message_key"), rs.getString("payload"),
                        rs.getInt("attempt_count")))
                .optional();
        if (pending.isEmpty()) {
            return false;
        }

        var event = pending.get();
        try {
            kafka.send(event.topic(), event.key(), event.payload()).get(6, TimeUnit.SECONDS);
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while publishing outbox event " + event.id(), failure);
        } catch (ExecutionException | TimeoutException | KafkaException
                 | org.apache.kafka.common.KafkaException failure) {
            scheduleRetry(event, failure instanceof ExecutionException ? failure.getCause() : failure);
            return false;
        }

        jdbc.sql("UPDATE " + outbox + " SET sent_at = clock_timestamp() WHERE event_id = :id")
                .param("id", event.id()).update();
        return true;
    }

    private void scheduleRetry(PendingEvent event, Throwable failure) {
        int delaySeconds = Math.min(10, 1 << Math.min(event.failedAttempts(), 4));
        jdbc.sql("""
                UPDATE %s SET attempt_count = attempt_count + 1,
                    next_attempt_at = clock_timestamp() + (:delay * interval '1 second')
                WHERE event_id = :id
                """.formatted(outbox))
                .param("id", event.id()).param("delay", delaySeconds).update();
        log.warn("Outbox send failed eventId={} orderId={} topic={} retryInSeconds={}: {}",
                event.id(), event.key(), event.topic(), delaySeconds, failure.toString());
    }

    private record PendingEvent(UUID id, String topic, String key, String payload, int failedAttempts) {
    }
}
