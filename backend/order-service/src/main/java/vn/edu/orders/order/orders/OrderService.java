package vn.edu.orders.order.orders;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;
import tools.jackson.databind.ObjectMapper;
import vn.edu.orders.contracts.events.CancelFlag;
import vn.edu.orders.contracts.events.CancelRequestedPayload;
import vn.edu.orders.contracts.events.EventEnvelope;
import vn.edu.orders.contracts.events.EventType;
import vn.edu.orders.contracts.events.OrderCreatedPayload;
import vn.edu.orders.contracts.events.OrderItemLine;
import vn.edu.orders.contracts.events.OrderStatus;
import vn.edu.orders.order.config.MutableClock;
import vn.edu.orders.order.config.SiteProperties;
import vn.edu.orders.order.menu.infrastructure.MenuCacheStore;

@Service
public class OrderService {
    private final OrderStore store;
    private final MenuCacheStore menuCache;
    private final SiteProperties site;
    private final MutableClock clock;
    private final ObjectMapper json;

    public OrderService(
            OrderStore store,
            MenuCacheStore menuCache,
            SiteProperties site,
            MutableClock clock,
            ObjectMapper json
    ) {
        this.store = store;
        this.menuCache = menuCache;
        this.site = site;
        this.clock = clock;
        this.json = json;
    }

    @Transactional(isolation = Isolation.READ_COMMITTED)
    public Creation create(CreateOrderRequest request) {
        // Canonical dish order makes a retry independent of the order of lines in the request.
        List<CreateOrderRequest.Item> requested = request.items().stream()
                .sorted(Comparator.comparing(CreateOrderRequest.Item::dishId)).toList();
        List<String> dishIds = requested.stream().map(CreateOrderRequest.Item::dishId).distinct().toList();
        if (dishIds.size() != requested.size()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Duplicate dishId");
        }

        // Idempotent retry check before validating menu mutability.
        Optional<OrderSummary> alreadyCreated = store.byRequestIdOptional(request.clientRequestId());
        if (alreadyCreated.isPresent()) {
            return verifyExistingOrder(alreadyCreated.get(), requested);
        }

        // Strict validation before writing anything to orders or outbox_events (BR9).
        Map<String, String> names = menuCache.findOrderableDishNames(dishIds);
        List<OrderDetails.Item> items = new ArrayList<>();
        for (var item : requested) {
            String name = names.get(item.dishId());
            if (name == null) {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Unknown or inactive dish: " + item.dishId());
            }
            items.add(new OrderDetails.Item(items.size() + 1, item.dishId(), name, item.quantity()));
        }

        UUID orderId = UUID.randomUUID();
        OffsetDateTime now = OffsetDateTime.ofInstant(clock.instant(), ZoneOffset.UTC).truncatedTo(ChronoUnit.MICROS);
        if (!store.insertOrder(orderId, request.clientRequestId(), site, now)) {
            OrderSummary existing = store.byRequestId(request.clientRequestId());
            return verifyExistingOrder(existing, requested);
        }

        items.forEach(item -> store.insertItem(orderId, item));

        UUID eventId = UUID.randomUUID();
        List<OrderItemLine> eventItems = items.stream()
                .map(item -> new OrderItemLine(item.lineNo(), item.dishId(), item.dishName(), item.quantity()))
                .toList();
        var envelope = EventEnvelope.v1(
                eventId,
                EventType.OrderCreated,
                orderId,
                site.id(),
                now.toInstant(),
                new OrderCreatedPayload(now.toInstant(), site.sourceType(), eventItems)
        );
        store.insertOrderCreated(eventId, orderId, site.id(), json.writeValueAsString(envelope), now);
        return new Creation(store.byRequestId(request.clientRequestId()), true);
    }

    @Transactional(isolation = Isolation.READ_COMMITTED)
    public CancelResult cancel(UUID orderId, CancelOrderRequest request) {
        if (request == null || request.reasonCode() == null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "reasonCode is required");
        }
        if (!request.reasonCode().isCustomerCancel()) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "reasonCode must be a customer cancel reason, got: " + request.reasonCode()
            );
        }

        var locked = store.lockOrderForCancel(orderId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Order not found: " + orderId));

        if (locked.statusRank() >= OrderStatus.DONE.statusRank()) {
            throw new OrderConflictException(
                    orderId,
                    locked.status(),
                    "Order is already in terminal status " + locked.status()
            );
        }

        if (!CancelFlag.NONE.name().equals(locked.cancelFlag())) {
            OrderSummary current = store.byId(orderId)
                    .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Order not found: " + orderId));
            return new CancelResult(current, false, HttpStatus.ACCEPTED.value());
        }

        OffsetDateTime now = OffsetDateTime.ofInstant(clock.instant(), ZoneOffset.UTC).truncatedTo(ChronoUnit.MICROS);
        List<Long> deleted = store.deleteUnattemptedCreateOrderOutbox(orderId);
        if (!deleted.isEmpty()) {
            store.markLocallyCancelled(orderId, request.reasonCode(), now);
            OrderSummary updated = store.byId(orderId).orElseThrow();
            return new CancelResult(updated, true, HttpStatus.OK.value());
        }

        UUID eventId = UUID.randomUUID();
        var envelope = EventEnvelope.v1(
                eventId,
                EventType.CancelRequested,
                orderId,
                site.id(),
                now.toInstant(),
                new CancelRequestedPayload(request.reasonCode())
        );
        store.insertCancelRequestOutbox(eventId, orderId, site.id(), json.writeValueAsString(envelope), now);
        store.markCancelRequested(orderId, now);
        OrderSummary updated = store.byId(orderId).orElseThrow();
        return new CancelResult(updated, false, HttpStatus.ACCEPTED.value());
    }

    public List<OrderSummary> recent() {
        return store.recent();
    }

    public OrderDetails detail(UUID id) {
        OrderSummary order = store.byId(id)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Order not found"));
        return new OrderDetails(order, store.items(id));
    }

    private Creation verifyExistingOrder(OrderSummary existing, List<CreateOrderRequest.Item> requested) {
        var original = store.items(existing.orderId()).stream()
                .map(item -> new CreateOrderRequest.Item(item.dishId(), item.quantity())).toList();
        if (!original.equals(requested)) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "clientRequestId already used with different items");
        }
        return new Creation(existing, false);
    }

    public record Creation(OrderSummary order, boolean created) {
    }

    public record CancelResult(OrderSummary order, boolean localCancelled, int httpStatus) {
    }
}
