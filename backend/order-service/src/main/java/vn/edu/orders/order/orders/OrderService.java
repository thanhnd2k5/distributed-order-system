package vn.edu.orders.order.orders;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;
import tools.jackson.databind.ObjectMapper;
import vn.edu.orders.contracts.events.OrderCreatedEvent;
import vn.edu.orders.contracts.events.OrderItemLine;
import vn.edu.orders.order.config.SiteProperties;
import vn.edu.orders.order.menu.domain.MenuItem;
import vn.edu.orders.order.menu.infrastructure.MenuItemRepository;

@Service
class OrderService {
    private final OrderStore store;
    private final MenuItemRepository menu;
    private final SiteProperties site;
    private final ObjectMapper json;

    OrderService(OrderStore store, MenuItemRepository menu, SiteProperties site, ObjectMapper json) {
        this.store = store;
        this.menu = menu;
        this.site = site;
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

        UUID orderId = UUID.randomUUID();
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.MICROS);
        // The unique index arbitrates concurrent requests. The next SELECT sees the committed winner.
        if (!store.insertOrder(orderId, request.clientRequestId(), site, now)) {
            OrderSummary existing = store.byRequestId(request.clientRequestId());
            var original = store.items(existing.orderId()).stream()
                    .map(item -> new CreateOrderRequest.Item(item.dishId(), item.quantity())).toList();
            if (!original.equals(requested)) {
                throw new ResponseStatusException(HttpStatus.CONFLICT, "clientRequestId already used with different items");
            }
            return new Creation(existing, false);
        }

        Map<String, String> names = menu.findByActiveTrueAndDishIdIn(dishIds).stream()
                .collect(Collectors.toMap(MenuItem::getDishId, MenuItem::getName));
        List<OrderDetails.Item> items = new ArrayList<>();
        for (var item : requested) {
            String name = names.get(item.dishId());
            if (name == null) {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Unknown or inactive dish: " + item.dishId());
            }
            items.add(new OrderDetails.Item(items.size() + 1, item.dishId(), name, item.quantity()));
        }
        items.forEach(item -> store.insertItem(orderId, item));

        UUID eventId = UUID.randomUUID();
        List<OrderItemLine> eventItems = items.stream()
                .map(item -> new OrderItemLine(item.lineNo(), item.dishId(), item.dishName(), item.quantity()))
                .toList();
        var event = OrderCreatedEvent.v1(eventId, orderId, site.sourceType(), site.id(), now.toInstant(), eventItems);
        store.insertOrderCreated(eventId, orderId, json.writeValueAsString(event), now);
        return new Creation(store.byRequestId(request.clientRequestId()), true);
    }

    List<OrderSummary> recent() {
        return store.recent();
    }

    OrderDetails detail(UUID id) {
        OrderSummary order = store.byId(id)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Order not found"));
        return new OrderDetails(order, store.items(id));
    }

    record Creation(OrderSummary order, boolean created) {
    }
}
