package vn.edu.orders.order.outbox;

import jakarta.annotation.PreDestroy;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import vn.edu.orders.order.config.MutableClock;
import vn.edu.orders.order.config.OrderLifecycleProperties;

/**
 * Probes Kafka broker connectivity via cluster metadata with a short timeout.
 * Reuses an {@link AdminClient} while healthy and recreates it after failures.
 */
@Component
public class KafkaAdminConnectivityProbe implements KafkaConnectivityProbe {
    private final String bootstrapServers;
    private final OrderLifecycleProperties properties;
    private final MutableClock clock;
    private final AtomicReference<ProbeStatus> status = new AtomicReference<>(ProbeStatus.initial());
    private final Object clientLock = new Object();
    private AdminClient adminClient;

    public KafkaAdminConnectivityProbe(
            @Value("${spring.kafka.bootstrap-servers:localhost:19092,localhost:19093,localhost:19094}") String bootstrapServers,
            OrderLifecycleProperties properties,
            MutableClock clock
    ) {
        this.bootstrapServers = bootstrapServers;
        this.properties = properties;
        this.clock = clock;
    }

    @Override
    public boolean probe() {
        long timeoutMs = properties.probeTimeoutMs();
        Instant now = clock.instant();
        try {
            AdminClient client = getOrCreateClient((int) timeoutMs);
            client.describeCluster(new DescribeClusterOptions().timeoutMs((int) timeoutMs))
                    .nodes()
                    .get(timeoutMs, TimeUnit.MILLISECONDS);
            recordSuccess(now);
            return true;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            resetClient();
            recordFailure(now, "Interrupted during Kafka metadata probe");
            return false;
        } catch (Exception ex) {
            resetClient();
            Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
            recordFailure(now, cause.getClass().getSimpleName() + ": " + cause.getMessage());
            return false;
        }
    }

    @Override
    public ProbeStatus latestStatus() {
        return status.get();
    }

    @Override
    public void recordSuccess(Instant at) {
        status.set(new ProbeStatus(true, at, null));
    }

    @Override
    public void recordFailure(Instant at, String error) {
        status.set(new ProbeStatus(false, at, error));
    }

    private AdminClient getOrCreateClient(int timeoutMs) {
        synchronized (clientLock) {
            if (adminClient == null) {
                int setupTimeout = Math.min(timeoutMs, 1000);
                adminClient = AdminClient.create(Map.of(
                        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                        AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, timeoutMs,
                        AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, timeoutMs,
                        AdminClientConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG, setupTimeout,
                        AdminClientConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG, setupTimeout,
                        AdminClientConfig.RETRIES_CONFIG, 0
                ));
            }
            return adminClient;
        }
    }

    private void resetClient() {
        synchronized (clientLock) {
            if (adminClient != null) {
                try {
                    adminClient.close(java.time.Duration.ofMillis(250));
                } catch (Exception ignored) {
                    // Best-effort close on broken client
                } finally {
                    adminClient = null;
                }
            }
        }
    }

    @PreDestroy
    public void shutdown() {
        resetClient();
    }
}
