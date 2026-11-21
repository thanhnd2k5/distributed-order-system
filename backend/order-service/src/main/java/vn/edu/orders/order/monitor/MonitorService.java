package vn.edu.orders.order.monitor;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import vn.edu.orders.order.config.MutableClock;
import vn.edu.orders.order.config.OrderLifecycleProperties;
import vn.edu.orders.order.config.SiteProperties;
import vn.edu.orders.order.monitor.MonitorResponse.ActiveCancelRequest;
import vn.edu.orders.order.monitor.MonitorResponse.ConsumerAlert;
import vn.edu.orders.order.monitor.MonitorResponse.ExpiringUnsentAlert;
import vn.edu.orders.order.monitor.MonitorResponse.PendingInventoryAlert;
import vn.edu.orders.order.outbox.KafkaConnectivityProbe;
import vn.edu.orders.order.outbox.KafkaConnectivityProbe.ProbeStatus;

@Service
public class MonitorService {
    private final JdbcClient jdbc;
    private final KafkaConnectivityProbe connectivityProbe;
    private final MutableClock clock;
    private final OrderLifecycleProperties properties;
    private final SiteProperties site;
    private final String orders;
    private final String outboxEvents;
    private final String alerts;

    public MonitorService(
            JdbcClient jdbc,
            KafkaConnectivityProbe connectivityProbe,
            MutableClock clock,
            OrderLifecycleProperties properties,
            SiteProperties site,
            @Value("${spring.flyway.default-schema}") String schema
    ) {
        this.jdbc = jdbc;
        this.connectivityProbe = connectivityProbe;
        this.clock = clock;
        this.properties = properties;
        this.site = site;
        String schemaPrefix = "\"" + schema.replace("\"", "\"\"") + "\".";
        this.orders = schemaPrefix + "orders";
        this.outboxEvents = schemaPrefix + "outbox_events";
        this.alerts = schemaPrefix + "alerts";
    }

    public MonitorResponse getMonitorSnapshot() {
        Instant now = clock.instant();
        ProbeStatus probe = connectivityProbe.latestStatus();

        long pendingOutboxCount = jdbc.sql("""
                SELECT count(*)
                FROM %s
                WHERE sent_at IS NULL
                """.formatted(outboxEvents))
                .query(Long.class)
                .single();

        OffsetDateTime pendingCutoff = OffsetDateTime.ofInstant(
                now.minus(properties.pendingInventoryAlertAfter()),
                ZoneOffset.UTC
        );
        List<PendingInventoryAlert> pendingInventoryAlerts = jdbc.sql("""
                SELECT order_id, created_at
                FROM %s
                WHERE status = 'PENDING_INVENTORY'
                  AND created_at < :cutoff
                ORDER BY created_at ASC, order_id ASC
                """.formatted(orders))
                .param("cutoff", pendingCutoff)
                .query((rs, row) -> {
                    Instant createdAt = rs.getObject("created_at", OffsetDateTime.class).toInstant();
                    long waitingSeconds = Math.max(0L, Duration.between(createdAt, now).getSeconds());
                    return new PendingInventoryAlert(
                            rs.getObject("order_id", UUID.class),
                            createdAt,
                            waitingSeconds
                    );
                })
                .list();

        Duration expiryThreshold = properties.expiryThreshold();
        Duration warningLead = expiryThreshold.minus(properties.expiryWarningBefore());
        OffsetDateTime expiringCutoff = OffsetDateTime.ofInstant(now.minus(warningLead), ZoneOffset.UTC);
        List<ExpiringUnsentAlert> expiringUnsentAlerts = jdbc.sql("""
                SELECT DISTINCT ON (oe.order_id)
                       oe.order_id, oe.id AS outbox_id, oe.kind, o.created_at
                FROM %s oe
                JOIN %s o ON o.order_id = oe.order_id
                WHERE oe.sent_at IS NULL
                  AND o.created_at <= :cutoff
                ORDER BY oe.order_id, oe.id ASC
                """.formatted(outboxEvents, orders))
                .param("cutoff", expiringCutoff)
                .query((rs, row) -> {
                    Instant createdAt = rs.getObject("created_at", OffsetDateTime.class).toInstant();
                    Instant expiresAt = createdAt.plus(expiryThreshold);
                    long remainingSeconds = Duration.between(now, expiresAt).getSeconds();
                    return new ExpiringUnsentAlert(
                            rs.getObject("order_id", UUID.class),
                            rs.getLong("outbox_id"),
                            rs.getString("kind"),
                            createdAt,
                            expiresAt,
                            remainingSeconds
                    );
                })
                .list();

        List<ActiveCancelRequest> activeCancelRequests = jdbc.sql("""
                SELECT order_id, status, cancel_flag, cancel_requested_at
                FROM %s
                WHERE cancel_flag <> 'NONE'
                ORDER BY cancel_requested_at ASC NULLS LAST, order_id ASC
                """.formatted(orders))
                .query((rs, row) -> {
                    OffsetDateTime reqTs = rs.getObject("cancel_requested_at", OffsetDateTime.class);
                    Instant requestedAt = reqTs == null ? null : reqTs.toInstant();
                    long waitedSeconds = requestedAt == null
                            ? 0L
                            : Math.max(0L, Duration.between(requestedAt, now).getSeconds());
                    return new ActiveCancelRequest(
                            rs.getObject("order_id", UUID.class),
                            rs.getString("status"),
                            rs.getString("cancel_flag"),
                            requestedAt,
                            waitedSeconds
                    );
                })
                .list();

        List<ConsumerAlert> recentAlerts = jdbc.sql("""
                SELECT id, kind, order_id, event_id, source_topic, source_partition,
                       source_offset, detail::text AS detail_text, created_at
                FROM %s
                ORDER BY id DESC
                LIMIT 50
                """.formatted(alerts))
                .query((rs, row) -> new ConsumerAlert(
                        rs.getLong("id"),
                        rs.getString("kind"),
                        rs.getObject("order_id", UUID.class),
                        rs.getObject("event_id", UUID.class),
                        rs.getString("source_topic"),
                        rs.getInt("source_partition"),
                        rs.getLong("source_offset"),
                        rs.getString("detail_text"),
                        rs.getObject("created_at", OffsetDateTime.class).toInstant()
                ))
                .list();

        return new MonitorResponse(
                site.id(),
                probe.connected(),
                probe.lastProbeAt(),
                probe.lastError(),
                pendingOutboxCount,
                pendingInventoryAlerts,
                expiringUnsentAlerts,
                activeCancelRequests,
                recentAlerts
        );
    }
}
