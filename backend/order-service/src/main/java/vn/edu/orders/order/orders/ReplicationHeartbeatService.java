package vn.edu.orders.order.orders;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import vn.edu.orders.order.config.MutableClock;
import vn.edu.orders.order.config.SiteProperties;

/**
 * Updates {@code replication_heartbeat} in the local branch database so
 * PostgreSQL logical replication propagates per-branch {@code dataAsOf} to Central DB.
 */
@Service
public class ReplicationHeartbeatService {
    private final JdbcClient jdbc;
    private final SiteProperties site;
    private final MutableClock clock;
    private final String heartbeatTable;

    public ReplicationHeartbeatService(
            JdbcClient jdbc,
            SiteProperties site,
            MutableClock clock,
            @Value("${spring.flyway.default-schema}") String schema
    ) {
        this.jdbc = jdbc;
        this.site = site;
        this.clock = clock;
        this.heartbeatTable = "\"" + schema.replace("\"", "\"\"") + "\".replication_heartbeat";
    }

    public Instant beat() {
        OffsetDateTime now = OffsetDateTime.ofInstant(clock.instant(), ZoneOffset.UTC);
        jdbc.sql("""
                INSERT INTO %s (branch_id, beat_at)
                VALUES (:branchId, :beatAt)
                ON CONFLICT (branch_id) DO UPDATE SET beat_at = EXCLUDED.beat_at
                """.formatted(heartbeatTable))
                .param("branchId", site.id())
                .param("beatAt", now)
                .update();
        return now.toInstant();
    }
}
