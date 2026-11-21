package vn.edu.orders.order.outbox;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;
import vn.edu.orders.order.config.MutableClock;
import vn.edu.orders.order.config.OrderLifecycleProperties;

/**
 * Reliable outbox relay implementing BR5/BR9 and Conditional Write (1) from 02 §5:
 * - Processes only the oldest unsent {@code id} per {@code order_id} so per-order
 *   ordering is preserved without one failing order blocking other orders.
 * - Probes Kafka connectivity before claiming any unattempted entry.
 * - Commits {@code attempted = TRUE} BEFORE calling Kafka; never discards on failure
 *   and retries with exponential backoff.
 */
@Service
public class OutboxRelayService {
    private static final Logger log = LoggerFactory.getLogger(OutboxRelayService.class);
    private static final int BATCH_LIMIT = 10;

    private final JdbcClient jdbc;
    private final TransactionTemplate transaction;
    private final KafkaTemplate<String, String> kafka;
    private final KafkaConnectivityProbe connectivityProbe;
    private final MutableClock clock;
    private final OrderLifecycleProperties properties;
    private final String outbox;

    public OutboxRelayService(
            JdbcClient jdbc,
            TransactionTemplate transaction,
            KafkaTemplate<String, String> kafka,
            KafkaConnectivityProbe connectivityProbe,
            MutableClock clock,
            OrderLifecycleProperties properties,
            @Value("${spring.flyway.default-schema}") String schema
    ) {
        this.jdbc = jdbc;
        this.transaction = transaction;
        this.kafka = kafka;
        this.connectivityProbe = connectivityProbe;
        this.clock = clock;
        this.properties = properties;
        this.outbox = "\"" + schema.replace("\"", "\"\"") + "\".outbox_events";
    }

    public int publishPendingOnce() {
        List<PendingEvent> due = findOldestDuePerOrder(BATCH_LIMIT);
        if (due.isEmpty()) {
            return 0;
        }

        Boolean probeOkThisBatch = null;
        int sentCount = 0;

        for (PendingEvent event : due) {
            if (!event.attempted()) {
                if (probeOkThisBatch == null) {
                    probeOkThisBatch = connectivityProbe.probe();
                }
                if (!probeOkThisBatch) {
                    continue;
                }
                Optional<Long> claimed = claimForSend(event.id());
                if (claimed.isEmpty()) {
                    // Entry was deleted by concurrent local cancel before sender set attempted=true.
                    continue;
                }
            }

            if (sendToKafka(event)) {
                markSent(event.id());
                connectivityProbe.recordSuccess(clock.instant());
                sentCount++;
            } else {
                // Retry with exponential backoff; never revert attempted=true or discard.
                connectivityProbe.recordFailure(clock.instant(), "Kafka send failed for outbox id=" + event.id());
            }
        }
        return sentCount;
    }

    /**
     * Conditional Write (1): sets {@code attempted = TRUE} and commits before any Kafka send.
     */
    public Optional<Long> claimForSend(long outboxId) {
        return transaction.execute(status -> jdbc.sql("""
                UPDATE %s
                SET attempted = TRUE
                WHERE id = :id AND sent_at IS NULL
                RETURNING id
                """.formatted(outbox))
                .param("id", outboxId)
                .query(Long.class)
                .optional());
    }

    public List<PendingEvent> findOldestDuePerOrder(int limit) {
        OffsetDateTime now = OffsetDateTime.ofInstant(clock.instant(), ZoneOffset.UTC);
        return jdbc.sql("""
                SELECT oldest.id, oldest.event_id, oldest.order_id, oldest.kind, oldest.topic,
                       oldest.message_key, oldest.payload_text, oldest.attempted, oldest.attempt_count
                FROM (
                    SELECT DISTINCT ON (order_id)
                           id, event_id, order_id, kind, topic, message_key,
                           payload::text AS payload_text, attempted, attempt_count, next_attempt_at
                    FROM %s
                    WHERE sent_at IS NULL
                    ORDER BY order_id, id ASC
                ) oldest
                WHERE oldest.next_attempt_at <= :now
                ORDER BY oldest.id ASC
                LIMIT :limit
                """.formatted(outbox))
                .param("now", now)
                .param("limit", limit)
                .query((rs, row) -> new PendingEvent(
                        rs.getLong("id"),
                        rs.getObject("event_id", UUID.class),
                        rs.getObject("order_id", UUID.class),
                        rs.getString("kind"),
                        rs.getString("topic"),
                        rs.getString("message_key"),
                        rs.getString("payload_text"),
                        rs.getBoolean("attempted"),
                        rs.getInt("attempt_count")
                ))
                .list();
    }

    private boolean sendToKafka(PendingEvent event) {
        try {
            kafka.send(event.topic(), event.key(), event.payload())
                    .get(properties.sendTimeoutMs(), TimeUnit.MILLISECONDS);
            return true;
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            scheduleRetry(event, failure);
            return false;
        } catch (ExecutionException | TimeoutException | KafkaException
                 | org.apache.kafka.common.KafkaException failure) {
            Throwable cause = failure instanceof ExecutionException && failure.getCause() != null
                    ? failure.getCause()
                    : failure;
            scheduleRetry(event, cause);
            return false;
        }
    }

    private void markSent(long outboxId) {
        Instant now = clock.instant();
        OffsetDateTime ts = OffsetDateTime.ofInstant(now, ZoneOffset.UTC);
        transaction.executeWithoutResult(status -> jdbc.sql("""
                UPDATE %s
                SET sent_at = :sentAt
                WHERE id = :id
                """.formatted(outbox))
                .param("sentAt", ts)
                .param("id", outboxId)
                .update());
    }

    private void scheduleRetry(PendingEvent event, Throwable failure) {
        int delaySeconds = Math.min(60, 1 << Math.min(event.attemptCount(), 6));
        OffsetDateTime nextAttemptAt = OffsetDateTime.ofInstant(
                clock.instant().plusSeconds(delaySeconds),
                ZoneOffset.UTC
        );
        transaction.executeWithoutResult(status -> jdbc.sql("""
                UPDATE %s
                SET attempt_count = attempt_count + 1,
                    next_attempt_at = :nextAttemptAt
                WHERE id = :id
                """.formatted(outbox))
                .param("nextAttemptAt", nextAttemptAt)
                .param("id", event.id())
                .update());
        log.warn("Outbox send failed outboxId={} eventId={} orderId={} kind={} topic={} retryInSeconds={}: {}",
                event.id(), event.eventId(), event.orderId(), event.kind(), event.topic(),
                delaySeconds, failure.toString());
    }

    public record PendingEvent(
            long id,
            UUID eventId,
            UUID orderId,
            String kind,
            String topic,
            String key,
            String payload,
            boolean attempted,
            int attemptCount
    ) {
    }
}
