package vn.edu.orders.order.menu.infrastructure;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;
import vn.edu.orders.order.config.OrderLifecycleProperties;

@Component
public class HttpInventorySnapshotClient implements MenuSnapshotFetcher {
    private final OrderLifecycleProperties properties;
    private final ObjectMapper mapper;
    private final HttpClient http;

    public HttpInventorySnapshotClient(OrderLifecycleProperties properties, ObjectMapper mapper) {
        this.properties = properties;
        this.mapper = mapper;
        this.http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(3))
                .build();
    }

    @Override
    public MenuSnapshotResponse fetchSnapshot() {
        String baseUrl = properties.inventoryBaseUrl().replaceAll("/+$", "");
        URI uri = URI.create(baseUrl + "/api/menu/snapshot");
        HttpRequest request = HttpRequest.newBuilder(uri)
                .timeout(Duration.ofSeconds(5))
                .GET()
                .build();
        try {
            HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                throw new SnapshotFetchException(
                        "Inventory snapshot returned HTTP " + response.statusCode() + " from " + uri);
            }
            MenuSnapshotResponse snapshot = mapper.readValue(response.body(), MenuSnapshotResponse.class);
            if (snapshot == null) {
                throw new SnapshotFetchException("Inventory snapshot returned null body from " + uri);
            }
            return snapshot;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new SnapshotFetchException("Interrupted fetching menu snapshot from " + uri, ex);
        } catch (IOException ex) {
            throw new SnapshotFetchException("Failed to fetch menu snapshot from " + uri, ex);
        }
    }

    public static class SnapshotFetchException extends RuntimeException {
        public SnapshotFetchException(String message) {
            super(message);
        }

        public SnapshotFetchException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
