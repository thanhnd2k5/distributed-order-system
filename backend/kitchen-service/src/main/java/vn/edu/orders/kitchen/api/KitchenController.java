package vn.edu.orders.kitchen.api;

import java.util.List;
import java.util.UUID;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import vn.edu.orders.kitchen.application.KitchenOrderService;
import vn.edu.orders.kitchen.application.TicketNotFoundException;
import vn.edu.orders.kitchen.infrastructure.KitchenMonitorService;

@RestController
@RequestMapping("/api/kitchen")
public class KitchenController {
    private final KitchenOrderService orderService;
    private final KitchenMonitorService monitorService;

    public KitchenController(KitchenOrderService orderService, KitchenMonitorService monitorService) {
        this.orderService = orderService;
        this.monitorService = monitorService;
    }

    @GetMapping("/tickets")
    public List<KitchenTicketResponse> listTickets(@RequestParam(required = false) String status) {
        return orderService.listTickets(status);
    }

    @GetMapping("/tickets/{orderId}")
    public KitchenTicketResponse getTicket(@PathVariable UUID orderId) {
        return orderService.findTicket(orderId)
                .orElseThrow(() -> new TicketNotFoundException(orderId));
    }

    @PostMapping("/tickets/{orderId}/start")
    public KitchenTicketResponse startTicket(@PathVariable UUID orderId) {
        return orderService.startTicket(orderId);
    }

    @PostMapping("/tickets/{orderId}/reject")
    public KitchenTicketResponse rejectTicket(
            @PathVariable UUID orderId,
            @RequestBody(required = false) RejectTicketRequest request
    ) {
        return orderService.rejectTicket(orderId, request);
    }

    @PostMapping("/tickets/{orderId}/complete")
    public KitchenTicketResponse completeTicket(@PathVariable UUID orderId) {
        return orderService.completeTicket(orderId);
    }

    @GetMapping("/alerts")
    public List<KitchenAlertResponse> listAlerts(@RequestParam(required = false) String kind) {
        return orderService.listAlerts(kind);
    }

    @GetMapping("/monitor")
    public KitchenMonitorResponse getMonitor() {
        return monitorService.getMonitorStatus();
    }
}
