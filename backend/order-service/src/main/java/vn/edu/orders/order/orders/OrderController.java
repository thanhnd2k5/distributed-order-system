package vn.edu.orders.order.orders;

import jakarta.validation.Valid;
import java.net.URI;
import java.util.List;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/orders")
class OrderController {
    private final OrderService orders;

    OrderController(OrderService orders) {
        this.orders = orders;
    }

    @PostMapping
    ResponseEntity<OrderSummary> create(@Valid @RequestBody CreateOrderRequest request) {
        var result = orders.create(request);
        URI location = URI.create("/api/orders/" + result.order().orderId());
        return result.created()
                ? ResponseEntity.created(location).body(result.order())
                : ResponseEntity.ok().location(location).body(result.order());
    }

    @PostMapping("/{orderId}/cancel")
    ResponseEntity<OrderSummary> cancel(
            @PathVariable UUID orderId,
            @Valid @RequestBody CancelOrderRequest request
    ) {
        var result = orders.cancel(orderId, request);
        return ResponseEntity.status(result.httpStatus()).body(result.order());
    }

    @GetMapping
    List<OrderSummary> list() {
        return orders.recent();
    }

    @GetMapping("/{orderId}")
    OrderDetails detail(@PathVariable UUID orderId) {
        return orders.detail(orderId);
    }

    @ExceptionHandler(OrderConflictException.class)
    ProblemDetail handleConflict(OrderConflictException ex) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, ex.getMessage());
        problem.setTitle("Order cannot be cancelled");
        problem.setProperty("orderId", ex.orderId());
        problem.setProperty("currentStatus", ex.currentStatus());
        return problem;
    }
}
