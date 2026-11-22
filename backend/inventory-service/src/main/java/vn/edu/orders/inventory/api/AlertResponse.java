package vn.edu.orders.inventory.api;

import java.time.Instant;
import java.util.UUID;
import tools.jackson.databind.JsonNode;
import vn.edu.orders.contracts.events.AlertKind;

public record AlertResponse(
        long id,
        AlertKind kind,
        UUID orderId,
        UUID eventId,
        String sourceTopic,
        int sourcePartition,
        long sourceOffset,
        JsonNode detail,
        Instant createdAt
) {
}
