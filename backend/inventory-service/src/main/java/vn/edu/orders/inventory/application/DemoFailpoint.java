package vn.edu.orders.inventory.application;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import vn.edu.orders.inventory.config.DemoFailpointProperties;
import vn.edu.orders.inventory.config.InventoryProperties;

@Component
public class DemoFailpoint {
    private static final Logger log = LoggerFactory.getLogger(DemoFailpoint.class);

    private final DemoFailpointProperties properties;
    private final String instanceId;

    public DemoFailpoint(DemoFailpointProperties properties, InventoryProperties inventoryProperties) {
        this.properties = properties;
        this.instanceId = inventoryProperties.instanceId();
    }

    public void maybeHaltAfterCommit(ReservationResult result, int partition, long offset) {
        if (!properties.haltAfterCommitOnce() || !result.isNewlyApplied()) {
            return;
        }
        String rawPath = properties.markerFile();
        if (rawPath == null || rawPath.isBlank()) {
            throw new IllegalStateException("demo.failpoint.marker-file must be set when halt-after-commit-once=true");
        }
        Path marker = Path.of(rawPath);
        try {
            Path parent = marker.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Files.createFile(marker);
        } catch (FileAlreadyExistsException alreadyTriggered) {
            log.info("Demo failpoint D08 marker already exists, skipping halt instanceId={} orderId={} eventId={} marker={}",
                    instanceId, result.orderId(), result.eventId(), marker.toAbsolutePath());
            return;
        } catch (IOException ex) {
            throw new IllegalStateException("Failed creating D08 failpoint marker file " + marker, ex);
        }

        log.error("DEMO FAILPOINT D08 triggered: halting JVM after DB commit and before offset ack instanceId={} orderId={} eventId={} partition={} offset={} marker={}",
                instanceId, result.orderId(), result.eventId(), partition, offset, marker.toAbsolutePath());
        Runtime.getRuntime().halt(137);
    }
}
