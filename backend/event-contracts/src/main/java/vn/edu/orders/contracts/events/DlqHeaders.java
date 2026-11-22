package vn.edu.orders.contracts.events;

public final class DlqHeaders {
    public static final String ORIGINAL_TOPIC = "dlq-original-topic";
    public static final String ORIGINAL_PARTITION = "dlq-original-partition";
    public static final String ORIGINAL_OFFSET = "dlq-original-offset";
    public static final String CONSUMER_NAME = "dlq-consumer-name";
    public static final String ERROR = "dlq-error";
    public static final String ERROR_CLASS = "dlq-error-class";
    public static final String FAILED_AT = "dlq-failed-at";

    private DlqHeaders() {
    }
}
