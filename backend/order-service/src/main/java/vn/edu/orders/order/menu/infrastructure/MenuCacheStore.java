package vn.edu.orders.order.menu.infrastructure;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import vn.edu.orders.contracts.events.DishStatus;
import vn.edu.orders.order.menu.api.MenuItemResponse;

@Repository
public class MenuCacheStore {
    private final JdbcClient jdbc;
    private final String menuItems;
    private final String menuCache;
    private final String menuCacheMeta;
    private final String processedEvents;

    public MenuCacheStore(JdbcClient jdbc, @Value("${spring.flyway.default-schema}") String schema) {
        this.jdbc = jdbc;
        String schemaPrefix = "\"" + schema.replace("\"", "\"\"") + "\".";
        this.menuItems = schemaPrefix + "menu_items";
        this.menuCache = schemaPrefix + "menu_cache";
        this.menuCacheMeta = schemaPrefix + "menu_cache_meta";
        this.processedEvents = schemaPrefix + "processed_events";
    }

    public record MenuCacheMeta(int epoch, Instant lastHeardAt) {
    }

    public List<MenuItemResponse> listVisibleMenuDishes() {
        return jdbc.sql("""
                SELECT COALESCE(mc.dish_id, mi.dish_id) AS dish_id,
                       COALESCE(mi.name, mc.dish_name, mc.dish_id) AS name,
                       COALESCE(mc.status, CASE WHEN mi.active THEN 'AVAILABLE' ELSE 'HIDDEN' END) AS status,
                       COALESCE(mc.epoch, 1) AS epoch,
                       COALESCE(mc.seq, 0) AS seq
                FROM %s mi
                FULL OUTER JOIN %s mc ON mc.dish_id = mi.dish_id
                WHERE COALESCE(mc.status, CASE WHEN mi.active THEN 'AVAILABLE' ELSE 'HIDDEN' END) <> 'HIDDEN'
                ORDER BY dish_id ASC
                """.formatted(menuItems, menuCache))
                .query((rs, row) -> new MenuItemResponse(
                        rs.getString("dish_id"),
                        rs.getString("name"),
                        DishStatus.valueOf(rs.getString("status")),
                        rs.getInt("epoch"),
                        rs.getLong("seq")
                ))
                .list();
    }

    public Map<String, String> findOrderableDishNames(Collection<String> dishIds) {
        if (dishIds == null || dishIds.isEmpty()) {
            return Map.of();
        }
        Map<String, String> names = new LinkedHashMap<>();
        jdbc.sql("""
                SELECT COALESCE(mi.dish_id, mc.dish_id) AS dish_id,
                       COALESCE(mi.name, mc.dish_name, mc.dish_id) AS dish_name
                FROM %s mi
                FULL OUTER JOIN %s mc ON mc.dish_id = mi.dish_id
                WHERE COALESCE(mi.dish_id, mc.dish_id) IN (:dishIds)
                  AND (mi.active IS TRUE OR (mi.dish_id IS NULL AND mc.dish_id IS NOT NULL))
                """.formatted(menuItems, menuCache))
                .param("dishIds", dishIds)
                .query(rs -> {
                    names.put(rs.getString("dish_id"), rs.getString("dish_name"));
                });
        return names;
    }

    public MenuCacheMeta readMeta() {
        return jdbc.sql("""
                SELECT epoch, last_heard_at
                FROM %s
                WHERE id = TRUE
                """.formatted(menuCacheMeta))
                .query((rs, row) -> {
                    OffsetDateTime ts = rs.getObject("last_heard_at", OffsetDateTime.class);
                    return new MenuCacheMeta(rs.getInt("epoch"), ts == null ? null : ts.toInstant());
                })
                .single();
    }

    public int lockMetaForUpdate() {
        return jdbc.sql("""
                SELECT epoch
                FROM %s
                WHERE id = TRUE
                FOR UPDATE
                """.formatted(menuCacheMeta))
                .query(Integer.class)
                .single();
    }

    public boolean isEventProcessed(UUID eventId) {
        if (eventId == null) {
            return false;
        }
        Long count = jdbc.sql("""
                SELECT count(*)
                FROM %s
                WHERE event_id = :eventId
                """.formatted(processedEvents))
                .param("eventId", eventId)
                .query(Long.class)
                .single();
        return count > 0;
    }

    public boolean tryInsertProcessedEvent(UUID eventId, String eventType, OffsetDateTime now) {
        if (eventId == null) {
            return true;
        }
        return jdbc.sql("""
                INSERT INTO %s (event_id, order_id, event_type, processed_at, outcome)
                VALUES (:eventId, NULL, :eventType, :now, 'SUCCESS')
                ON CONFLICT (event_id) DO NOTHING
                """.formatted(processedEvents))
                .param("eventId", eventId)
                .param("eventType", eventType)
                .param("now", now)
                .update() == 1;
    }

    public void updateProcessedOutcome(UUID eventId, String outcome) {
        if (eventId == null) {
            return;
        }
        jdbc.sql("""
                UPDATE %s
                SET outcome = :outcome
                WHERE event_id = :eventId
                """.formatted(processedEvents))
                .param("outcome", outcome)
                .param("eventId", eventId)
                .update();
    }

    /**
     * Merges a single dish status into {@code menu_cache} only when
     * {@code (EXCLUDED.epoch, EXCLUDED.seq) > (menu_cache.epoch, menu_cache.seq)}.
     */
    public int upsertDishIfNewer(
            String dishId,
            String dishName,
            DishStatus status,
            int epoch,
            long seq
    ) {
        return jdbc.sql("""
                INSERT INTO %s (dish_id, dish_name, status, epoch, seq)
                VALUES (:dishId, :dishName, :status, :epoch, :seq)
                ON CONFLICT (dish_id) DO UPDATE
                SET status = EXCLUDED.status,
                    dish_name = COALESCE(EXCLUDED.dish_name, %s.dish_name),
                    epoch = EXCLUDED.epoch,
                    seq = EXCLUDED.seq
                WHERE %s.epoch < EXCLUDED.epoch
                   OR (%s.epoch = EXCLUDED.epoch AND %s.seq < EXCLUDED.seq)
                """.formatted(menuCache, menuCache, menuCache, menuCache, menuCache))
                .param("dishId", dishId)
                .param("dishName", dishName)
                .param("status", status.name())
                .param("epoch", epoch)
                .param("seq", seq)
                .update();
    }

    public int hideDishesBelowEpoch(int epoch) {
        return jdbc.sql("""
                UPDATE %s
                SET status = 'HIDDEN',
                    epoch = :epoch,
                    seq = 0
                WHERE epoch < :epoch
                """.formatted(menuCache))
                .param("epoch", epoch)
                .update();
    }

    public void updateMeta(int epoch, OffsetDateTime lastHeardAt) {
        jdbc.sql("""
                UPDATE %s
                SET epoch = GREATEST(epoch, :epoch),
                    last_heard_at = :lastHeardAt
                WHERE id = TRUE
                """.formatted(menuCacheMeta))
                .param("epoch", epoch)
                .param("lastHeardAt", lastHeardAt)
                .update();
    }

    public void touchLastHeardAt(OffsetDateTime lastHeardAt) {
        jdbc.sql("""
                UPDATE %s
                SET last_heard_at = :lastHeardAt
                WHERE id = TRUE
                """.formatted(menuCacheMeta))
                .param("lastHeardAt", lastHeardAt)
                .update();
    }
}
