package vn.edu.orders.contracts.events;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JavaType;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EventContractV15Tests {
    private final ObjectMapper mapper = JsonMapper.builder().build();

    @Test
    void allThirteenEventEnvelopesRoundTripThroughJackson() throws Exception {
        UUID orderId = UUID.fromString("d967e06a-090d-4abc-9fb2-c5ec54306e08");
        UUID rootEventId = UUID.fromString("5ffac880-b377-43d2-ae6c-3c5df8805a63");
        Instant now = Instant.parse("2026-11-21T09:15:00Z");

        assertEnvelopeRoundTrip(
                EventEnvelope.v1(
                        rootEventId,
                        EventType.OrderCreated,
                        orderId,
                        EventTopics.BRANCH_POS01,
                        now,
                        new OrderCreatedPayload(
                                now,
                                SourceType.POS,
                                List.of(new OrderItemLine(1, "COM_GA", "Com ga", 2))
                        )
                ),
                OrderCreatedPayload.class
        );

        assertEnvelopeRoundTrip(
                EventEnvelope.v1(
                        UUID.randomUUID(),
                        EventType.CancelRequested,
                        orderId,
                        EventTopics.BRANCH_POS01,
                        now,
                        new CancelRequestedPayload(ReasonCode.CUSTOMER_CHANGED_MIND)
                ),
                CancelRequestedPayload.class
        );

        assertEnvelopeRoundTrip(
                EventEnvelope.v1(
                        UUID.randomUUID(),
                        EventType.OrderReserved,
                        orderId,
                        EventTopics.BRANCH_POS01,
                        now,
                        rootEventId,
                        OrderReservedPayload.forKitchen(
                                EventTopics.BRANCH_POS01,
                                now,
                                List.of(new OrderItemLine(1, "COM_GA", "Com ga", 2)),
                                List.of(new IngredientQuantity("THIT_GA", new BigDecimal("0.400")))
                        )
                ),
                OrderReservedPayload.class
        );

        var branchReserved = EventEnvelope.v1(
                UUID.randomUUID(),
                EventType.OrderReserved,
                orderId,
                EventTopics.BRANCH_POS01,
                now,
                rootEventId,
                OrderReservedPayload.forBranch()
        );
        String branchReservedJson = mapper.writeValueAsString(branchReserved);
        assertThat(mapper.readTree(branchReservedJson).get("payload")).isEmpty();
        assertEnvelopeRoundTrip(branchReserved, OrderReservedPayload.class);

        assertEnvelopeRoundTrip(
                EventEnvelope.v1(
                        UUID.randomUUID(),
                        EventType.OrderFailed,
                        orderId,
                        EventTopics.BRANCH_ONLINE,
                        now,
                        rootEventId,
                        new OrderFailedPayload(EventTopics.BRANCH_ONLINE, ReasonCode.INGREDIENT_AWAITING_STOCK_CHECK)
                ),
                OrderFailedPayload.class
        );

        assertEnvelopeRoundTrip(
                EventEnvelope.v1(
                        UUID.randomUUID(),
                        EventType.StockConsumed,
                        orderId,
                        "KITCHEN",
                        now,
                        StockConsumedPayload.empty()
                ),
                StockConsumedPayload.class
        );

        assertEnvelopeRoundTrip(
                EventEnvelope.v1(
                        UUID.randomUUID(),
                        EventType.ReleaseRequested,
                        orderId,
                        "KITCHEN",
                        now,
                        new ReleaseRequestedPayload(
                                CancelledBy.KITCHEN,
                                ReasonCode.SPOILED_INGREDIENT,
                                List.of("THIT_GA")
                        )
                ),
                ReleaseRequestedPayload.class
        );

        assertEnvelopeRoundTrip(
                EventEnvelope.v1(
                        UUID.randomUUID(),
                        EventType.CancelPending,
                        orderId,
                        EventTopics.BRANCH_POS02,
                        now,
                        CancelPendingPayload.empty()
                ),
                CancelPendingPayload.class
        );

        assertEnvelopeRoundTrip(
                EventEnvelope.v1(
                        UUID.randomUUID(),
                        EventType.CancelRejected,
                        orderId,
                        EventTopics.BRANCH_POS01,
                        now,
                        new CancelRejectedPayload(KitchenStatus.COOKING, "Cannot cancel: the kitchen has started cooking")
                ),
                CancelRejectedPayload.class
        );

        assertEnvelopeRoundTrip(
                EventEnvelope.v1(
                        UUID.randomUUID(),
                        EventType.OrderCancelled,
                        orderId,
                        EventTopics.BRANCH_POS01,
                        now,
                        new OrderCancelledPayload(CancelledBy.CUSTOMER, ReasonCode.CUSTOMER_WAITED_TOO_LONG)
                ),
                OrderCancelledPayload.class
        );

        assertEnvelopeRoundTrip(
                EventEnvelope.v1(
                        UUID.randomUUID(),
                        EventType.OrderCompleted,
                        orderId,
                        EventTopics.BRANCH_POS01,
                        now,
                        OrderCompletedPayload.empty()
                ),
                OrderCompletedPayload.class
        );

        var dishEvent = assertEnvelopeRoundTrip(
                EventEnvelope.v1Menu(
                        UUID.randomUUID(),
                        EventType.DishStatusChanged,
                        "INVENTORY",
                        now,
                        null,
                        new DishStatusChangedPayload("COM_GA", DishStatus.SOLD_OUT, 2, 17L)
                ),
                DishStatusChangedPayload.class
        );
        assertThat(dishEvent.orderId()).isNull();
        assertThat(dishEvent.payload().version()).isEqualTo(new MenuVersion(2, 17L));

        var checkEvent = assertEnvelopeRoundTrip(
                EventEnvelope.v1Menu(
                        UUID.randomUUID(),
                        EventType.IngredientCheckChanged,
                        "INVENTORY",
                        now,
                        null,
                        new IngredientCheckChangedPayload("THIT_GA", true, 2, 18L)
                ),
                IngredientCheckChangedPayload.class
        );
        assertThat(checkEvent.orderId()).isNull();

        var heartbeatEvent = assertEnvelopeRoundTrip(
                EventEnvelope.v1Menu(
                        UUID.randomUUID(),
                        EventType.Heartbeat,
                        "INVENTORY",
                        now,
                        null,
                        new HeartbeatPayload(2, 18L)
                ),
                HeartbeatPayload.class
        );
        assertThat(heartbeatEvent.orderId()).isNull();
    }

    @Test
    void unknownFieldsAreIgnoredForForwardCompatibility() throws Exception {
        String json = """
                {
                  "eventId": "5ffac880-b377-43d2-ae6c-3c5df8805a63",
                  "eventType": "CancelRequested",
                  "schemaVersion": 1,
                  "orderId": "d967e06a-090d-4abc-9fb2-c5ec54306e08",
                  "branchId": "POS01",
                  "occurredAt": "2026-11-21T09:15:00Z",
                  "causationId": null,
                  "futureEnvelopeField": "ignored",
                  "payload": {
                    "reasonCode": "WRONG_ORDER_ENTERED",
                    "futurePayloadField": 123
                  }
                }
                """;
        JavaType type = mapper.getTypeFactory().constructParametricType(EventEnvelope.class, CancelRequestedPayload.class);
        EventEnvelope<CancelRequestedPayload> parsed = mapper.readValue(json, type);
        parsed.validateEnvelope();
        assertThat(parsed.payload().reasonCode()).isEqualTo(ReasonCode.WRONG_ORDER_ENTERED);
    }

    @Test
    void eventTopicsAndPatternsWorkCorrectly() {
        assertThat(EventTopics.orderEventsTopic("POS01")).isEqualTo("order-events.POS01");
        assertThat(EventTopics.cancelEventsTopic("POS02")).isEqualTo("cancel-events.POS02");
        assertThat(EventTopics.statusEventsTopic("ONLINE")).isEqualTo("status-events.ONLINE");

        assertThat(EventTopics.branchFromTopic("order-events.POS01")).isEqualTo("POS01");
        assertThat(EventTopics.branchFromTopic("cancel-events.POS02")).isEqualTo("POS02");
        assertThat(EventTopics.branchFromTopic("status-events.ONLINE")).isEqualTo("ONLINE");

        assertThat(Pattern.compile(EventTopics.ORDER_EVENTS_PATTERN).matcher("order-events.POS01").matches()).isTrue();
        assertThat(Pattern.compile(EventTopics.ORDER_EVENTS_PATTERN).matcher("cancel-events.POS01").matches()).isFalse();
        assertThat(Pattern.compile(EventTopics.CANCEL_EVENTS_PATTERN).matcher("cancel-events.ONLINE").matches()).isTrue();
        assertThat(Pattern.compile(EventTopics.CANCEL_EVENTS_PATTERN).matcher("kitchen-orders").matches()).isFalse();

        assertThatThrownBy(() -> EventTopics.branchFromTopic("kitchen-orders"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> EventTopics.branchFromTopic("order-events."))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> EventTopics.orderEventsTopic("POS 01"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void enumsAndHelpersMatchSpecRules() {
        assertThat(OrderStatus.PENDING_INVENTORY.statusRank()).isEqualTo(0);
        assertThat(OrderStatus.RESERVED.statusRank()).isEqualTo(1);
        assertThat(OrderStatus.DONE.statusRank()).isEqualTo(2);
        assertThat(OrderStatus.CANCELLED.statusRank()).isEqualTo(2);
        assertThat(OrderStatus.FAILED.statusRank()).isEqualTo(2);

        assertThat(OrderStatus.RESERVED.canTransitionFrom(OrderStatus.PENDING_INVENTORY)).isTrue();
        assertThat(OrderStatus.CANCELLED.canTransitionFrom(OrderStatus.PENDING_INVENTORY)).isTrue();
        assertThat(OrderStatus.RESERVED.canTransitionFrom(OrderStatus.CANCELLED)).isFalse();
        assertThat(OrderStatus.FAILED.canTransitionFrom(OrderStatus.DONE)).isFalse();

        assertThat(KitchenStatus.QUEUED.isActiveTicket()).isTrue();
        assertThat(KitchenStatus.COOKING.isActiveTicket()).isTrue();
        assertThat(KitchenStatus.CANCEL_PENDING.isActiveTicket()).isFalse();
        assertThat(KitchenStatus.COOKING.isRejectableForCancel()).isTrue();
        assertThat(KitchenStatus.DONE.isRejectableForCancel()).isTrue();

        assertThat(ReasonCode.SPOILED_INGREDIENT.requiresSpoiledIngredients()).isTrue();
        assertThat(ReasonCode.EQUIPMENT_FAILURE.requiresSpoiledIngredients()).isFalse();
        assertThat(ReasonCode.INVALID_ORDER_DATA.isInventoryFailure()).isTrue();
        assertThat(ReasonCode.CUSTOMER_CHANGED_MIND.isCustomerCancel()).isTrue();

        assertThat(new MenuVersion(2, 1)).isGreaterThan(new MenuVersion(1, 999));
        assertThat(new MenuVersion(2, 5)).isGreaterThan(new MenuVersion(2, 4));
        assertThat(new MenuVersion(2, 5).isNewerThan(new MenuVersion(2, 5))).isFalse();
        assertThat(new MenuVersion(1, 0).isNewerThan(null)).isTrue();

        assertThat(EventType.fromValue("OrderCompleted")).contains(EventType.OrderCompleted);
        assertThat(EventType.fromValue("UnknownType")).isEmpty();
        assertThat(EventType.Heartbeat.isMenuEvent()).isTrue();
        assertThat(EventType.OrderCreated.requiresOrderId()).isTrue();
    }

    @Test
    void validateEnvelopeRejectsMissingFieldsAndUnsupportedSchemaVersion() {
        UUID eventId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();
        Instant now = Instant.now();

        assertThatThrownBy(() -> new EventEnvelope<>(
                eventId,
                EventType.OrderCreated,
                2,
                orderId,
                "POS01",
                now,
                null,
                OrderCompletedPayload.empty()
        ).validateEnvelope()).isInstanceOf(InvalidEventException.class);

        assertThatThrownBy(() -> EventEnvelope.v1(
                eventId,
                EventType.OrderCreated,
                null,
                "POS01",
                now,
                OrderCompletedPayload.empty()
        ).validateEnvelope()).isInstanceOf(InvalidEventException.class);
    }

    private <T> EventEnvelope<T> assertEnvelopeRoundTrip(EventEnvelope<T> envelope, Class<T> payloadClass)
            throws Exception {
        envelope.validateEnvelope();
        String raw = mapper.writeValueAsString(envelope);
        JavaType type = mapper.getTypeFactory().constructParametricType(EventEnvelope.class, payloadClass);
        EventEnvelope<T> parsed = mapper.readValue(raw, type);
        parsed.validateEnvelope();
        assertThat(parsed).isEqualTo(envelope);
        JsonNode root = mapper.readTree(raw);
        assertThat(root.get("eventType").asString()).isEqualTo(envelope.eventType().name());
        assertThat(root.get("schemaVersion").asInt()).isEqualTo(EventTopics.SCHEMA_VERSION_1);
        return parsed;
    }
}
