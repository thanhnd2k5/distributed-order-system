package vn.edu.orders.kitchen;

import com.jayway.jsonpath.JsonPath;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import vn.edu.orders.contracts.events.OrderItemLine;
import vn.edu.orders.contracts.events.ReservedEvent;
import vn.edu.orders.contracts.events.SourceType;
import vn.edu.orders.kitchen.api.KitchenTicketResponse;
import vn.edu.orders.kitchen.application.EventProcessingContext;
import vn.edu.orders.kitchen.application.InvalidEventException;
import vn.edu.orders.kitchen.application.KitchenTicketService;
import vn.edu.orders.kitchen.application.TicketProcessingResult;
import vn.edu.orders.kitchen.application.TicketProcessingResult.Outcome;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "outbox.enabled=false",
        "spring.kafka.listener.auto-startup=false",
        "spring.kafka.bootstrap-servers=localhost:19999",
        "spring.jpa.properties.hibernate.default_schema=b4_kitchen_test",
        "spring.flyway.default-schema=b4_kitchen_test",
        "spring.flyway.schemas=b4_kitchen_test",
        "kitchen.instance-id=kitchen-integration-test"
})
class KitchenIntegrationTests {
    @Value("${local.server.port}")
    private int port;

    @Autowired
    private JdbcTemplate jdbc;

    @Autowired
    private KitchenTicketService ticketService;

    private final HttpClient http = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    @BeforeEach
    void resetTestSchema() {
        jdbc.update("DELETE FROM b4_kitchen_test.outbox_events");
        jdbc.update("DELETE FROM b4_kitchen_test.kitchen_ticket_items");
        jdbc.update("DELETE FROM b4_kitchen_test.kitchen_tickets");
        jdbc.update("DELETE FROM b4_kitchen_test.processed_events");
    }

    @Test
    void processReservedCreatesTicketAndEnablesGetApi() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        ReservedEvent event = sampleReservedEvent(eventId, orderId, List.of(
                new OrderItemLine(1, "COM_GA", "Cơm gà", 2),
                new OrderItemLine(2, "CHAO_GA", "Cháo gà", 1)
        ));

        TicketProcessingResult result = ticketService.processReserved(event, EventProcessingContext.direct());
        assertThat(result.outcome()).isEqualTo(Outcome.APPLIED);
        assertThat(result.status()).isEqualTo("WAITING");
        assertThat(result.ticketId()).isNotNull();

        UUID ticketId = result.ticketId();

        var detailResponse = get("/api/kitchen/tickets/" + ticketId);
        assertThat(detailResponse.statusCode()).isEqualTo(200);
        assertThat(JsonPath.<String>read(detailResponse.body(), "$.status")).isEqualTo("WAITING");
        assertThat(JsonPath.<String>read(detailResponse.body(), "$.sourceSiteId")).isEqualTo("POS01");
        assertThat(JsonPath.<List<String>>read(detailResponse.body(), "$.items[*].dishId"))
                .containsExactly("COM_GA", "CHAO_GA");

        var listResponse = get("/api/kitchen/tickets");
        assertThat(listResponse.statusCode()).isEqualTo(200);
        assertThat(JsonPath.<List<String>>read(listResponse.body(), "$[*].ticketId"))
                .contains(ticketId.toString());

        long processedCount = jdbc.queryForObject(
                "SELECT count(*) FROM b4_kitchen_test.processed_events WHERE event_id = ? AND outcome = 'APPLIED'",
                Long.class, eventId);
        assertThat(processedCount).isEqualTo(1L);
    }

    @Test
    void technicalDedupByEventId() {
        UUID orderId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        ReservedEvent event = sampleReservedEvent(eventId, orderId, List.of(
                new OrderItemLine(1, "COM_GA", "Cơm gà", 1)
        ));

        TicketProcessingResult r1 = ticketService.processReserved(event, EventProcessingContext.direct());
        assertThat(r1.outcome()).isEqualTo(Outcome.APPLIED);

        TicketProcessingResult r2 = ticketService.processReserved(event, EventProcessingContext.direct());
        assertThat(r2.outcome()).isEqualTo(Outcome.DUPLICATE_EVENT);
        assertThat(r2.status()).isEqualTo("WAITING");
        assertThat(r2.ticketId()).isEqualTo(r1.ticketId());

        long ticketCount = jdbc.queryForObject("SELECT count(*) FROM b4_kitchen_test.kitchen_tickets", Long.class);
        assertThat(ticketCount).isEqualTo(1L);
        long itemCount = jdbc.queryForObject("SELECT count(*) FROM b4_kitchen_test.kitchen_ticket_items", Long.class);
        assertThat(itemCount).isEqualTo(1L);
    }

    @Test
    void businessIdempotencySameOrderIdNewEventId() {
        UUID orderId = UUID.randomUUID();
        ReservedEvent e1 = sampleReservedEvent(UUID.randomUUID(), orderId, List.of(
                new OrderItemLine(1, "COM_GA", "Cơm gà", 2)
        ));
        ReservedEvent e2 = sampleReservedEvent(UUID.randomUUID(), orderId, List.of(
                new OrderItemLine(1, "COM_GA", "Cơm gà", 2)
        ));

        TicketProcessingResult r1 = ticketService.processReserved(e1, EventProcessingContext.direct());
        assertThat(r1.outcome()).isEqualTo(Outcome.APPLIED);

        TicketProcessingResult r2 = ticketService.processReserved(e2, EventProcessingContext.direct());
        assertThat(r2.outcome()).isEqualTo(Outcome.DUPLICATE_ORDER);
        assertThat(r2.ticketId()).isEqualTo(r1.ticketId());
        assertThat(r2.status()).isEqualTo("WAITING");

        long processedCount = jdbc.queryForObject("SELECT count(*) FROM b4_kitchen_test.processed_events", Long.class);
        assertThat(processedCount).isEqualTo(2L);
        long ticketCount = jdbc.queryForObject("SELECT count(*) FROM b4_kitchen_test.kitchen_tickets", Long.class);
        assertThat(ticketCount).isEqualTo(1L);
    }

    @Test
    void payloadConflictSameOrderIdNewEventIdDifferentItems() {
        UUID orderId = UUID.randomUUID();
        ReservedEvent e1 = sampleReservedEvent(UUID.randomUUID(), orderId, List.of(
                new OrderItemLine(1, "COM_GA", "Cơm gà", 1)
        ));
        ReservedEvent e2 = sampleReservedEvent(UUID.randomUUID(), orderId, List.of(
                new OrderItemLine(1, "CHAO_GA", "Cháo gà", 5)
        ));

        TicketProcessingResult r1 = ticketService.processReserved(e1, EventProcessingContext.direct());
        assertThat(r1.outcome()).isEqualTo(Outcome.APPLIED);

        TicketProcessingResult r2 = ticketService.processReserved(e2, EventProcessingContext.direct());
        assertThat(r2.outcome()).isEqualTo(Outcome.PAYLOAD_CONFLICT);
        assertThat(r2.ticketId()).isEqualTo(r1.ticketId());

        var items = jdbc.queryForList("SELECT dish_id FROM b4_kitchen_test.kitchen_ticket_items WHERE ticket_id = ?",
                String.class, r1.ticketId());
        assertThat(items).containsExactly("COM_GA");
    }

    @Test
    void replayReservedOnDoneTicketNeverRevertsStatus() {
        UUID orderId = UUID.randomUUID();
        ReservedEvent e1 = sampleReservedEvent(UUID.randomUUID(), orderId, List.of(
                new OrderItemLine(1, "COM_GA", "Cơm gà", 1)
        ));

        TicketProcessingResult r1 = ticketService.processReserved(e1, EventProcessingContext.direct());
        assertThat(r1.outcome()).isEqualTo(Outcome.APPLIED);

        KitchenTicketResponse completed = ticketService.completeTicket(r1.ticketId());
        assertThat(completed.status()).isEqualTo("DONE");
        assertThat(completed.completedAt()).isNotNull();

        // Replay with new eventId
        ReservedEvent replayEvent = sampleReservedEvent(UUID.randomUUID(), orderId, List.of(
                new OrderItemLine(1, "COM_GA", "Cơm gà", 1)
        ));
        TicketProcessingResult r2 = ticketService.processReserved(replayEvent, EventProcessingContext.direct());
        assertThat(r2.outcome()).isEqualTo(Outcome.DUPLICATE_ORDER);
        assertThat(r2.status()).isEqualTo("DONE");

        var ticketAfter = ticketService.completeTicket(r1.ticketId());
        assertThat(ticketAfter.status()).isEqualTo("DONE");
        assertThat(ticketAfter.completedAt()).isEqualTo(completed.completedAt());
    }

    @Test
    void completeTicketEmitsOneDoneOutboxEventAndIsIdempotent() throws Exception {
        UUID orderId = UUID.randomUUID();
        ReservedEvent e1 = sampleReservedEvent(UUID.randomUUID(), orderId, List.of(
                new OrderItemLine(1, "COM_GA", "Cơm gà", 1)
        ));
        TicketProcessingResult r = ticketService.processReserved(e1, EventProcessingContext.direct());
        UUID ticketId = r.ticketId();

        var firstCall = post("/api/kitchen/tickets/" + ticketId + "/complete");
        assertThat(firstCall.statusCode()).isEqualTo(200);
        assertThat(JsonPath.<String>read(firstCall.body(), "$.status")).isEqualTo("DONE");
        String completedAtStr = JsonPath.read(firstCall.body(), "$.completedAt");
        assertThat(completedAtStr).isNotNull();

        long outboxCount = jdbc.queryForObject(
                "SELECT count(*) FROM b4_kitchen_test.outbox_events WHERE aggregate_id = ? AND event_type = 'Done'",
                Long.class, ticketId);
        assertThat(outboxCount).isEqualTo(1L);

        String messageKey = jdbc.queryForObject(
                "SELECT message_key FROM b4_kitchen_test.outbox_events WHERE aggregate_id = ?",
                String.class, ticketId);
        assertThat(messageKey).isEqualTo(orderId.toString());

        // Repeat complete call: must return 200 OK idempotent without a second outbox event
        var secondCall = post("/api/kitchen/tickets/" + ticketId + "/complete");
        assertThat(secondCall.statusCode()).isEqualTo(200);
        assertThat(JsonPath.<String>read(secondCall.body(), "$.completedAt")).isEqualTo(completedAtStr);

        long outboxCountAfter = jdbc.queryForObject(
                "SELECT count(*) FROM b4_kitchen_test.outbox_events WHERE aggregate_id = ?",
                Long.class, ticketId);
        assertThat(outboxCountAfter).isEqualTo(1L);
    }

    @Test
    void concurrentCompleteCallsUnderCountDownLatchOnlyEmitOneDoneOutboxEvent() throws Exception {
        UUID orderId = UUID.randomUUID();
        ReservedEvent e1 = sampleReservedEvent(UUID.randomUUID(), orderId, List.of(
                new OrderItemLine(1, "COM_GA", "Cơm gà", 2)
        ));
        TicketProcessingResult r = ticketService.processReserved(e1, EventProcessingContext.direct());
        UUID ticketId = r.ticketId();

        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch doneGate = new CountDownLatch(2);
        ExecutorService pool = Executors.newFixedThreadPool(2);

        AtomicReference<KitchenTicketResponse> res1 = new AtomicReference<>();
        AtomicReference<KitchenTicketResponse> res2 = new AtomicReference<>();

        pool.submit(() -> {
            try {
                startGate.await();
                res1.set(ticketService.completeTicket(ticketId));
            } catch (Exception ex) {
                ex.printStackTrace();
            } finally {
                doneGate.countDown();
            }
        });

        pool.submit(() -> {
            try {
                startGate.await();
                res2.set(ticketService.completeTicket(ticketId));
            } catch (Exception ex) {
                ex.printStackTrace();
            } finally {
                doneGate.countDown();
            }
        });

        startGate.countDown();
        boolean completedInTime = doneGate.await(10, TimeUnit.SECONDS);
        pool.shutdown();
        assertThat(completedInTime).isTrue();

        assertThat(res1.get()).isNotNull();
        assertThat(res2.get()).isNotNull();
        assertThat(res1.get().status()).isEqualTo("DONE");
        assertThat(res2.get().status()).isEqualTo("DONE");
        assertThat(res1.get().completedAt()).isEqualTo(res2.get().completedAt());

        long outboxCount = jdbc.queryForObject(
                "SELECT count(*) FROM b4_kitchen_test.outbox_events WHERE aggregate_id = ?",
                Long.class, ticketId);
        assertThat(outboxCount).isEqualTo(1L);
    }

    @Test
    void completeNonExistentTicketReturns404() throws Exception {
        var resp = post("/api/kitchen/tickets/" + UUID.randomUUID() + "/complete");
        assertThat(resp.statusCode()).isEqualTo(404);
    }

    @Test
    void listTicketsFilterByStatus() throws Exception {
        UUID order1 = UUID.randomUUID();
        UUID order2 = UUID.randomUUID();
        var r1 = ticketService.processReserved(sampleReservedEvent(UUID.randomUUID(), order1, List.of(
                new OrderItemLine(1, "COM_GA", "Cơm gà", 1)
        )), EventProcessingContext.direct());
        var r2 = ticketService.processReserved(sampleReservedEvent(UUID.randomUUID(), order2, List.of(
                new OrderItemLine(1, "CHAO_GA", "Cháo gà", 1)
        )), EventProcessingContext.direct());

        ticketService.completeTicket(r1.ticketId());

        var waitingResp = get("/api/kitchen/tickets?status=WAITING");
        assertThat(waitingResp.statusCode()).isEqualTo(200);
        assertThat(JsonPath.<List<String>>read(waitingResp.body(), "$[*].ticketId"))
                .containsExactly(r2.ticketId().toString());

        var doneResp = get("/api/kitchen/tickets?status=DONE");
        assertThat(doneResp.statusCode()).isEqualTo(200);
        assertThat(JsonPath.<List<String>>read(doneResp.body(), "$[*].ticketId"))
                .containsExactly(r1.ticketId().toString());
    }

    @Test
    void validationRejectsInvalidReservedEvents() {
        assertThatThrownBy(() -> ticketService.processReserved(null, EventProcessingContext.direct()))
                .isInstanceOf(InvalidEventException.class);

        assertThatThrownBy(() -> ticketService.processReserved(
                sampleReservedEvent(null, UUID.randomUUID(), List.of(new OrderItemLine(1, "COM_GA", "Cơm gà", 1))),
                EventProcessingContext.direct()))
                .isInstanceOf(InvalidEventException.class);

        assertThatThrownBy(() -> ticketService.processReserved(
                sampleReservedEvent(UUID.randomUUID(), null, List.of(new OrderItemLine(1, "COM_GA", "Cơm gà", 1))),
                EventProcessingContext.direct()))
                .isInstanceOf(InvalidEventException.class);

        assertThatThrownBy(() -> ticketService.processReserved(
                sampleReservedEvent(UUID.randomUUID(), UUID.randomUUID(), List.of()),
                EventProcessingContext.direct()))
                .isInstanceOf(InvalidEventException.class);

        assertThatThrownBy(() -> ticketService.processReserved(
                sampleReservedEvent(UUID.randomUUID(), UUID.randomUUID(), List.of(
                        new OrderItemLine(1, "COM_GA", "Cơm gà", 1),
                        new OrderItemLine(2, "COM_GA", "Cơm gà", 1)
                )),
                EventProcessingContext.direct()))
                .isInstanceOf(InvalidEventException.class);
    }

    private ReservedEvent sampleReservedEvent(UUID eventId, UUID orderId, List<OrderItemLine> items) {
        return ReservedEvent.v1(
                eventId,
                orderId,
                SourceType.POS,
                "POS01",
                Instant.now(),
                orderId,
                items
        );
    }

    private HttpResponse<String> get(String path) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                .timeout(Duration.ofSeconds(10))
                .GET()
                .build(), HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> post(String path) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                .timeout(Duration.ofSeconds(10))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build(), HttpResponse.BodyHandlers.ofString());
    }
}
