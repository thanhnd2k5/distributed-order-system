package vn.edu.orders.order.outbox;

import java.time.Instant;

/**
 * Connectivity probe run before claiming unattempted outbox entries (BR9).
 */
public interface KafkaConnectivityProbe {
    boolean probe();

    ProbeStatus latestStatus();

    void recordSuccess(Instant at);

    void recordFailure(Instant at, String error);

    record ProbeStatus(boolean connected, Instant lastProbeAt, String lastError) {
        public static ProbeStatus initial() {
            return new ProbeStatus(false, null, "Not probed yet");
        }
    }
}
