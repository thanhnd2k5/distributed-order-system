package vn.edu.orders.order.menu.application;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import vn.edu.orders.contracts.events.DishStatusChangedPayload;
import vn.edu.orders.contracts.events.EventEnvelope;
import vn.edu.orders.contracts.events.EventType;
import vn.edu.orders.contracts.events.HeartbeatPayload;
import vn.edu.orders.contracts.events.InvalidEventException;
import vn.edu.orders.order.config.MutableClock;
import vn.edu.orders.order.config.OrderLifecycleProperties;
import vn.edu.orders.order.menu.api.MenuItemResponse;
import vn.edu.orders.order.menu.api.MenuResponse;
import vn.edu.orders.order.menu.infrastructure.MenuCacheStore;
import vn.edu.orders.order.menu.infrastructure.MenuCacheStore.MenuCacheMeta;
import vn.edu.orders.order.menu.infrastructure.MenuSnapshotFetcher;
import vn.edu.orders.order.menu.infrastructure.MenuSnapshotFetcher.MenuSnapshotDish;
import vn.edu.orders.order.menu.infrastructure.MenuSnapshotFetcher.MenuSnapshotResponse;
import vn.edu.orders.order.outbox.KafkaConnectivityProbe;

@Service
public class MenuService {
    private static final Logger log = LoggerFactory.getLogger(MenuService.class);

    private final MenuCacheStore store;
    private final MenuSnapshotFetcher snapshotFetcher;
    private final KafkaConnectivityProbe connectivityProbe;
    private final MutableClock clock;
    private final OrderLifecycleProperties properties;
    private final TransactionTemplate tx;

    public MenuService(
            MenuCacheStore store,
            MenuSnapshotFetcher snapshotFetcher,
            KafkaConnectivityProbe connectivityProbe,
            MutableClock clock,
            OrderLifecycleProperties properties,
            PlatformTransactionManager transactionManager
    ) {
        this.store = store;
        this.snapshotFetcher = snapshotFetcher;
        this.connectivityProbe = connectivityProbe;
        this.clock = clock;
        this.properties = properties;
        this.tx = new TransactionTemplate(transactionManager);
        this.tx.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
    }

    public MenuResponse getMenu() {
        MenuCacheMeta meta = store.readMeta();
        Instant now = clock.instant();
        Instant lastHeardAt = meta.lastHeardAt();
        boolean stale = lastHeardAt == null
                || now.isAfter(lastHeardAt.plus(properties.staleThreshold()));
        List<MenuItemResponse> dishes = store.listVisibleMenuDishes();
        boolean kafkaConnected = connectivityProbe.latestStatus().connected();
        return new MenuResponse(stale, kafkaConnected, meta.epoch(), lastHeardAt, dishes);
    }

    public void syncSnapshot() {
        MenuSnapshotResponse snapshot = snapshotFetcher.fetchSnapshot();
        mergeSnapshot(snapshot);
    }

    public void mergeSnapshot(MenuSnapshotResponse snapshot) {
        if (snapshot == null) {
            throw new IllegalArgumentException("Snapshot must not be null");
        }
        OffsetDateTime now = OffsetDateTime.ofInstant(clock.instant(), ZoneOffset.UTC);
        tx.executeWithoutResult(status -> applySnapshotInsideTx(snapshot, snapshot.epoch(), now));
    }

    public void onDishStatusChanged(EventEnvelope<DishStatusChangedPayload> event) {
        if (event == null) {
            throw new InvalidEventException("DishStatusChanged envelope must not be null");
        }
        event.validateEnvelope();
        if (event.eventType() != EventType.DishStatusChanged) {
            throw new InvalidEventException(
                    "Expected DishStatusChanged but got " + event.eventType(), event.eventId(), event.orderId());
        }
        DishStatusChangedPayload payload = event.payload();
        if (payload.dishId() == null || payload.dishId().isBlank()) {
            throw new InvalidEventException("Missing dishId on DishStatusChanged", event.eventId(), null);
        }
        if (payload.status() == null) {
            throw new InvalidEventException("Missing status on DishStatusChanged", event.eventId(), null);
        }

        if (store.isEventProcessed(event.eventId())) {
            return;
        }

        MenuCacheMeta currentMeta = store.readMeta();
        MenuSnapshotResponse snapshot = null;
        if (payload.epoch() > currentMeta.epoch()) {
            snapshot = snapshotFetcher.fetchSnapshot();
        }

        MenuSnapshotResponse fetchedSnapshot = snapshot;
        OffsetDateTime now = OffsetDateTime.ofInstant(clock.instant(), ZoneOffset.UTC);
        tx.executeWithoutResult(status -> {
            if (!store.tryInsertProcessedEvent(event.eventId(), EventType.DishStatusChanged.name(), now)) {
                return;
            }
            if (fetchedSnapshot != null) {
                applySnapshotInsideTx(fetchedSnapshot, Math.max(fetchedSnapshot.epoch(), payload.epoch()), now);
            } else {
                store.touchLastHeardAt(now);
            }
            int updated = store.upsertDishIfNewer(
                    payload.dishId(),
                    null,
                    payload.status(),
                    payload.epoch(),
                    payload.seq()
            );
            if (updated == 0) {
                store.updateProcessedOutcome(event.eventId(), "STALE_VERSION_IGNORED");
                log.debug("Ignored stale DishStatusChanged dishId={} epoch={} seq={}",
                        payload.dishId(), payload.epoch(), payload.seq());
            }
        });
    }

    public void onHeartbeat(EventEnvelope<HeartbeatPayload> event) {
        if (event == null) {
            throw new InvalidEventException("Heartbeat envelope must not be null");
        }
        event.validateEnvelope();
        if (event.eventType() != EventType.Heartbeat) {
            throw new InvalidEventException(
                    "Expected Heartbeat but got " + event.eventType(), event.eventId(), event.orderId());
        }
        HeartbeatPayload payload = event.payload();

        if (store.isEventProcessed(event.eventId())) {
            return;
        }

        MenuCacheMeta currentMeta = store.readMeta();
        MenuSnapshotResponse snapshot = null;
        if (payload.epoch() > currentMeta.epoch()) {
            snapshot = snapshotFetcher.fetchSnapshot();
        }

        MenuSnapshotResponse fetchedSnapshot = snapshot;
        OffsetDateTime now = OffsetDateTime.ofInstant(clock.instant(), ZoneOffset.UTC);
        tx.executeWithoutResult(status -> {
            if (!store.tryInsertProcessedEvent(event.eventId(), EventType.Heartbeat.name(), now)) {
                return;
            }
            if (fetchedSnapshot != null) {
                applySnapshotInsideTx(fetchedSnapshot, Math.max(fetchedSnapshot.epoch(), payload.epoch()), now);
            } else {
                store.updateMeta(payload.epoch(), now);
            }
        });
    }

    private void applySnapshotInsideTx(MenuSnapshotResponse snapshot, int targetEpoch, OffsetDateTime now) {
        int currentEpoch = store.lockMetaForUpdate();
        int resolvedEpoch = Math.max(targetEpoch, snapshot.epoch());
        for (MenuSnapshotDish dish : snapshot.dishes()) {
            if (dish != null && dish.dishId() != null && !dish.dishId().isBlank() && dish.status() != null) {
                store.upsertDishIfNewer(dish.dishId(), dish.dishName(), dish.status(), dish.epoch(), dish.seq());
            }
        }
        if (resolvedEpoch > currentEpoch) {
            store.hideDishesBelowEpoch(resolvedEpoch);
        }
        store.updateMeta(resolvedEpoch, now);
    }
}
