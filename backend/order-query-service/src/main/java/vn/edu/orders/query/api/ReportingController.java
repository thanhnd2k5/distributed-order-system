package vn.edu.orders.query.api;

import java.time.Instant;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import vn.edu.orders.query.infrastructure.ReportingStore;

@RestController
@RequestMapping("/api/reporting")
public class ReportingController {
    private static final String REPLICA_NOTE =
            "Central Order Replica (asynchronous logical replication; data may lag behind source sites)";
    private static final Set<String> ALLOWED_SITES = Set.of("POS01", "POS02", "ONLINE");
    private static final Set<String> ALLOWED_STATUSES = Set.of(
            "PENDING_INVENTORY", "RESERVED", "FAILED", "DONE");

    private final ReportingStore store;

    public ReportingController(ReportingStore store) {
        this.store = store;
    }

    @GetMapping("/orders")
    public ReportingOrdersResponse listOrders(
            @RequestParam(required = false) String siteId,
            @RequestParam(required = false) String status,
            @RequestParam(defaultValue = "100") int limit
    ) {
        String normalizedSite = normalizeSite(siteId);
        String normalizedStatus = normalizeStatus(status);
        if (limit < 1 || limit > 500) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "limit must be between 1 and 500");
        }
        var orders = store.findOrders(normalizedSite, normalizedStatus, limit);
        return new ReportingOrdersResponse(REPLICA_NOTE, Instant.now(), orders.size(), orders);
    }

    @GetMapping("/orders/{orderId}")
    public ReportingOrderDetailsResponse getOrder(@PathVariable UUID orderId) {
        var details = store.findOrderDetails(orderId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND,
                        "Replicated order not found: " + orderId));
        return new ReportingOrderDetailsResponse(
                REPLICA_NOTE, Instant.now(), details.order(), details.items());
    }

    @GetMapping("/summary")
    public ReportingSummaryResponse getSummary() {
        var summary = store.loadSummary();
        return new ReportingSummaryResponse(
                REPLICA_NOTE,
                Instant.now(),
                summary.totalOrders(),
                summary.bySite(),
                summary.byStatus(),
                summary.subscriptions());
    }

    private static String normalizeSite(String siteId) {
        if (siteId == null || siteId.isBlank()) {
            return null;
        }
        String normalized = siteId.trim().toUpperCase(Locale.ROOT);
        if (!ALLOWED_SITES.contains(normalized)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "Invalid siteId parameter: " + siteId + ". Allowed: POS01, POS02, ONLINE");
        }
        return normalized;
    }

    private static String normalizeStatus(String status) {
        if (status == null || status.isBlank()) {
            return null;
        }
        String normalized = status.trim().toUpperCase(Locale.ROOT);
        if (!ALLOWED_STATUSES.contains(normalized)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "Invalid status parameter: " + status
                            + ". Allowed: PENDING_INVENTORY, RESERVED, FAILED, DONE");
        }
        return normalized;
    }
}
