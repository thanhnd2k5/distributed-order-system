package vn.edu.orders.order.config;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import org.springframework.stereotype.Component;

/**
 * Thread-safe injectable {@link Clock} bean backed by system UTC time in production
 * and controllable in tests without mocks.
 */
@Component
public class MutableClock extends Clock {
    private final AtomicReference<Clock> delegate = new AtomicReference<>(Clock.systemUTC());

    @Override
    public ZoneId getZone() {
        return delegate.get().getZone();
    }

    @Override
    public Clock withZone(ZoneId zone) {
        return delegate.get().withZone(zone);
    }

    @Override
    public Instant instant() {
        return delegate.get().instant();
    }

    public OffsetDateTime utcNow() {
        return OffsetDateTime.ofInstant(instant(), ZoneOffset.UTC);
    }

    public void setFixed(Instant instant) {
        delegate.set(Clock.fixed(Objects.requireNonNull(instant, "instant"), ZoneOffset.UTC));
    }

    public void advance(Duration duration) {
        Objects.requireNonNull(duration, "duration");
        delegate.updateAndGet(current -> Clock.fixed(current.instant().plus(duration), ZoneOffset.UTC));
    }

    public void reset() {
        delegate.set(Clock.systemUTC());
    }
}
