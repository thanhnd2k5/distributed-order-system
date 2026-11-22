package vn.edu.orders.kitchen.application;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import tools.jackson.databind.JsonNode;
import vn.edu.orders.contracts.events.CancelRequestedPayload;
import vn.edu.orders.contracts.events.EventEnvelope;
import vn.edu.orders.contracts.events.EventTopics;
import vn.edu.orders.contracts.events.EventType;
import vn.edu.orders.contracts.events.IngredientCheckChangedPayload;
import vn.edu.orders.contracts.events.IngredientQuantity;
import vn.edu.orders.contracts.events.InvalidEventException;
import vn.edu.orders.contracts.events.OrderFailedPayload;
import vn.edu.orders.contracts.events.OrderItemLine;
import vn.edu.orders.contracts.events.OrderReservedPayload;

public final class KitchenEventValidator {
    private KitchenEventValidator() {
    }

    public static void validateOrderReserved(EventEnvelope<OrderReservedPayload> envelope) {
        if (envelope == null) {
            throw new InvalidEventException("OrderReserved envelope must not be null");
        }
        envelope.validateEnvelope();
        if (envelope.eventType() != EventType.OrderReserved) {
            throw new InvalidEventException(
                    "Expected OrderReserved but got " + envelope.eventType(),
                    envelope.eventId(),
                    envelope.orderId());
        }
        UUID eventId = envelope.eventId();
        UUID orderId = envelope.orderId();
        OrderReservedPayload payload = envelope.payload();
        if (payload.originBranch() == null || payload.originBranch().isBlank()) {
            throw new InvalidEventException("OrderReserved payload.originBranch must not be blank", eventId, orderId);
        }
        if (payload.createdAt() == null) {
            throw new InvalidEventException("OrderReserved payload.createdAt must not be null", eventId, orderId);
        }
        if (payload.items() == null || payload.items().isEmpty()) {
            throw new InvalidEventException("OrderReserved payload.items must not be empty", eventId, orderId);
        }
        if (payload.reserved() == null || payload.reserved().isEmpty()) {
            throw new InvalidEventException("OrderReserved payload.reserved must not be empty", eventId, orderId);
        }

        Set<String> seenDishes = new HashSet<>();
        Set<Integer> seenLines = new HashSet<>();
        for (OrderItemLine item : payload.items()) {
            if (item == null) {
                throw new InvalidEventException("OrderItemLine must not be null", eventId, orderId);
            }
            if (item.lineNo() <= 0 || !seenLines.add(item.lineNo())) {
                throw new InvalidEventException("Invalid or duplicate lineNo: " + item.lineNo(), eventId, orderId);
            }
            if (item.dishId() == null || item.dishId().isBlank() || item.dishId().length() > 50) {
                throw new InvalidEventException("Invalid dishId: " + item.dishId(), eventId, orderId);
            }
            if (item.dishName() == null || item.dishName().isBlank() || item.dishName().length() > 200) {
                throw new InvalidEventException("Invalid dishName for dishId " + item.dishId(), eventId, orderId);
            }
            if (item.quantity() <= 0) {
                throw new InvalidEventException(
                        "Invalid quantity " + item.quantity() + " for dishId " + item.dishId(), eventId, orderId);
            }
            if (!seenDishes.add(item.dishId())) {
                throw new InvalidEventException("Duplicate dishId: " + item.dishId(), eventId, orderId);
            }
        }

        Set<String> seenIngredients = new HashSet<>();
        for (IngredientQuantity ing : payload.reserved()) {
            if (ing == null) {
                throw new InvalidEventException("IngredientQuantity must not be null", eventId, orderId);
            }
            if (ing.ingredientId() == null || ing.ingredientId().isBlank()) {
                throw new InvalidEventException("Reserved ingredientId must not be blank", eventId, orderId);
            }
            if (ing.quantity() == null || ing.quantity().compareTo(BigDecimal.ZERO) <= 0) {
                throw new InvalidEventException(
                        "Invalid reserved quantity for ingredientId " + ing.ingredientId(), eventId, orderId);
            }
            if (!seenIngredients.add(ing.ingredientId())) {
                throw new InvalidEventException(
                        "Duplicate reserved ingredientId: " + ing.ingredientId(), eventId, orderId);
            }
        }
    }

    public static void validateOrderFailed(EventEnvelope<OrderFailedPayload> envelope) {
        if (envelope == null) {
            throw new InvalidEventException("OrderFailed envelope must not be null");
        }
        envelope.validateEnvelope();
        if (envelope.eventType() != EventType.OrderFailed) {
            throw new InvalidEventException(
                    "Expected OrderFailed but got " + envelope.eventType(),
                    envelope.eventId(),
                    envelope.orderId());
        }
        OrderFailedPayload payload = envelope.payload();
        if (payload.originBranch() == null || payload.originBranch().isBlank()) {
            throw new InvalidEventException(
                    "OrderFailed payload.originBranch must not be blank", envelope.eventId(), envelope.orderId());
        }
        if (payload.reasonCode() == null || !payload.reasonCode().isInventoryFailure()) {
            throw new InvalidEventException(
                    "Invalid OrderFailed reasonCode: " + payload.reasonCode(), envelope.eventId(), envelope.orderId());
        }
    }

    public static String validateCancelRequested(String sourceTopic, EventEnvelope<CancelRequestedPayload> envelope) {
        if (envelope == null) {
            throw new InvalidEventException("CancelRequested envelope must not be null");
        }
        envelope.validateEnvelope();
        if (envelope.eventType() != EventType.CancelRequested) {
            throw new InvalidEventException(
                    "Expected CancelRequested but got " + envelope.eventType(),
                    envelope.eventId(),
                    envelope.orderId());
        }
        if (!EventTopics.isCancelEventsTopic(sourceTopic)) {
            throw new InvalidEventException(
                    "CancelRequested arrived on unexpected topic: " + sourceTopic,
                    envelope.eventId(),
                    envelope.orderId());
        }
        String requestingBranch;
        try {
            requestingBranch = EventTopics.branchFromTopic(sourceTopic);
        } catch (IllegalArgumentException ex) {
            throw new InvalidEventException(ex.getMessage(), envelope.eventId(), envelope.orderId(), ex);
        }
        CancelRequestedPayload payload = envelope.payload();
        if (payload.reasonCode() == null || !payload.reasonCode().isCustomerCancel()) {
            throw new InvalidEventException(
                    "Invalid CancelRequested reasonCode: " + payload.reasonCode(),
                    envelope.eventId(),
                    envelope.orderId());
        }
        return requestingBranch;
    }

    public static void validateIngredientCheckChanged(
            EventEnvelope<IngredientCheckChangedPayload> envelope,
            JsonNode payloadNode
    ) {
        if (envelope == null) {
            throw new InvalidEventException("IngredientCheckChanged envelope must not be null");
        }
        envelope.validateEnvelope();
        if (envelope.eventType() != EventType.IngredientCheckChanged) {
            throw new InvalidEventException(
                    "Expected IngredientCheckChanged but got " + envelope.eventType(),
                    envelope.eventId(),
                    envelope.orderId());
        }
        if (payloadNode != null) {
            JsonNode needsCheckNode = payloadNode.get("needsCheck");
            if (needsCheckNode == null || !needsCheckNode.isBoolean()) {
                throw new InvalidEventException(
                        "IngredientCheckChanged payload.needsCheck must be an explicit boolean",
                        envelope.eventId(),
                        envelope.orderId());
            }
        }
        IngredientCheckChangedPayload payload = envelope.payload();
        if (payload.ingredientId() == null || payload.ingredientId().isBlank()) {
            throw new InvalidEventException(
                    "IngredientCheckChanged payload.ingredientId must not be blank",
                    envelope.eventId(),
                    envelope.orderId());
        }
        if (payload.epoch() < 0 || payload.seq() < 0) {
            throw new InvalidEventException(
                    "IngredientCheckChanged version must be non-negative: epoch="
                            + payload.epoch() + ", seq=" + payload.seq(),
                    envelope.eventId(),
                    envelope.orderId());
        }
    }
}
