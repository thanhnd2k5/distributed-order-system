package vn.edu.orders.kitchen.api;

import java.util.List;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import vn.edu.orders.kitchen.application.KitchenTicketService;
import vn.edu.orders.kitchen.infrastructure.KitchenTicketStore;

@RestController
@RequestMapping("/api/kitchen")
public class KitchenController {
    private final KitchenTicketStore store;
    private final KitchenTicketService service;

    public KitchenController(KitchenTicketStore store, KitchenTicketService service) {
        this.store = store;
        this.service = service;
    }

    @GetMapping("/tickets")
    public List<KitchenTicketResponse> listTickets(@RequestParam(required = false) String status) {
        if (status != null && !status.isBlank()) {
            String normalized = status.trim().toUpperCase();
            if (!"WAITING".equals(normalized) && !"DONE".equals(normalized)) {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                        "Invalid status parameter: " + status + ". Allowed: WAITING, DONE");
            }
            return store.listTickets(normalized);
        }
        return store.listTickets(null);
    }

    @GetMapping("/tickets/{ticketId}")
    public KitchenTicketResponse getTicket(@PathVariable UUID ticketId) {
        return store.findTicketById(ticketId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND,
                        "Kitchen ticket not found: " + ticketId));
    }

    @PostMapping("/tickets/{ticketId}/complete")
    public KitchenTicketResponse completeTicket(@PathVariable UUID ticketId) {
        return service.completeTicket(ticketId);
    }
}
