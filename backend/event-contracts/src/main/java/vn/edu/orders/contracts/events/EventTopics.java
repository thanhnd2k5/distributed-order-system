package vn.edu.orders.contracts.events;

import java.util.List;
import java.util.Objects;

public final class EventTopics {
    public static final int SCHEMA_VERSION_1 = 1;
    public static final int SUPPORTED_SCHEMA_VERSION = SCHEMA_VERSION_1;

    public static final String ORDER_EVENTS_PREFIX = "order-events.";
    public static final String CANCEL_EVENTS_PREFIX = "cancel-events.";
    public static final String STATUS_EVENTS_PREFIX = "status-events.";

    public static final String ORDER_EVENTS_PATTERN = "order-events\\..*";
    public static final String CANCEL_EVENTS_PATTERN = "cancel-events\\..*";
    public static final String STATUS_EVENTS_PATTERN = "status-events\\..*";

    public static final String KITCHEN_ORDERS = "kitchen-orders";
    public static final String STOCK_COMMANDS = "stock-commands";
    public static final String MENU_EVENTS = "menu-events";
    public static final String DLQ = "dlq";

    public static final String HEARTBEAT_KEY = "heartbeat";

    public static final String BRANCH_POS01 = "POS01";
    public static final String BRANCH_POS02 = "POS02";
    public static final String BRANCH_ONLINE = "ONLINE";
    public static final List<String> KNOWN_BRANCHES = List.of(BRANCH_POS01, BRANCH_POS02, BRANCH_ONLINE);

    private EventTopics() {
    }

    public static String orderEventsTopic(String branchId) {
        return ORDER_EVENTS_PREFIX + normalizeBranch(branchId);
    }

    public static String cancelEventsTopic(String branchId) {
        return CANCEL_EVENTS_PREFIX + normalizeBranch(branchId);
    }

    public static String statusEventsTopic(String branchId) {
        return STATUS_EVENTS_PREFIX + normalizeBranch(branchId);
    }

    public static boolean isOrderEventsTopic(String topic) {
        return topic != null && topic.startsWith(ORDER_EVENTS_PREFIX) && topic.length() > ORDER_EVENTS_PREFIX.length();
    }

    public static boolean isCancelEventsTopic(String topic) {
        return topic != null && topic.startsWith(CANCEL_EVENTS_PREFIX) && topic.length() > CANCEL_EVENTS_PREFIX.length();
    }

    public static boolean isStatusEventsTopic(String topic) {
        return topic != null && topic.startsWith(STATUS_EVENTS_PREFIX) && topic.length() > STATUS_EVENTS_PREFIX.length();
    }

    public static String branchFromTopic(String topic) {
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("Topic must not be blank");
        }
        String branch;
        if (topic.startsWith(ORDER_EVENTS_PREFIX)) {
            branch = topic.substring(ORDER_EVENTS_PREFIX.length());
        } else if (topic.startsWith(CANCEL_EVENTS_PREFIX)) {
            branch = topic.substring(CANCEL_EVENTS_PREFIX.length());
        } else if (topic.startsWith(STATUS_EVENTS_PREFIX)) {
            branch = topic.substring(STATUS_EVENTS_PREFIX.length());
        } else {
            throw new IllegalArgumentException("Topic does not carry a branch suffix: " + topic);
        }
        return normalizeBranch(branch);
    }

    private static String normalizeBranch(String branchId) {
        Objects.requireNonNull(branchId, "branchId must not be null");
        String trimmed = branchId.trim();
        if (trimmed.isEmpty() || trimmed.contains(".") || trimmed.chars().anyMatch(Character::isWhitespace)) {
            throw new IllegalArgumentException("Invalid branchId: '" + branchId + "'");
        }
        return trimmed;
    }
}
