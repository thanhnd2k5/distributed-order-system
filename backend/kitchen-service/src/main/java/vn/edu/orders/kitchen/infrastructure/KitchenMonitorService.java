package vn.edu.orders.kitchen.infrastructure;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import vn.edu.orders.contracts.events.EventTopics;
import vn.edu.orders.kitchen.api.KitchenMonitorResponse;
import vn.edu.orders.kitchen.api.KitchenMonitorResponse.TopicLagStatus;
import vn.edu.orders.kitchen.config.KitchenProperties;

@Service
public class KitchenMonitorService {
    private static final Pattern CANCEL_PATTERN = Pattern.compile(EventTopics.CANCEL_EVENTS_PATTERN);

    private final KitchenOrderStore store;
    private final KitchenProperties properties;
    private final Clock clock;
    private final String bootstrapServers;
    private final String consumerGroup;

    public KitchenMonitorService(
            KitchenOrderStore store,
            KitchenProperties properties,
            Clock clock,
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${spring.kafka.consumer.group-id}") String consumerGroup
    ) {
        this.store = store;
        this.properties = properties;
        this.clock = clock;
        this.bootstrapServers = bootstrapServers;
        this.consumerGroup = consumerGroup;
    }

    public KitchenMonitorResponse getMonitorStatus() {
        Instant now = clock.instant();
        long pendingOutbox = store.countPendingOutbox();
        long thresholdSeconds = properties.slowConsumerThresholdSeconds();
        long timeoutMs = properties.monitorAdminTimeoutMs();

        Map<String, Object> adminProps = Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) timeoutMs,
                AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, (int) timeoutMs
        );

        try (AdminClient admin = AdminClient.create(adminProps)) {
            Set<String> allTopics = admin.listTopics().names().get(timeoutMs, TimeUnit.MILLISECONDS);
            List<String> subscribedTopics = allTopics.stream()
                    .filter(t -> EventTopics.KITCHEN_ORDERS.equals(t)
                            || EventTopics.MENU_EVENTS.equals(t)
                            || CANCEL_PATTERN.matcher(t).matches())
                    .sorted()
                    .toList();

            if (subscribedTopics.isEmpty()) {
                return new KitchenMonitorResponse(
                        properties.instanceId(),
                        consumerGroup,
                        true,
                        null,
                        thresholdSeconds,
                        false,
                        pendingOutbox,
                        List.of(),
                        now);
            }

            Map<String, TopicDescription> descriptions = admin.describeTopics(subscribedTopics)
                    .allTopicNames().get(timeoutMs, TimeUnit.MILLISECONDS);
            Map<TopicPartition, OffsetAndMetadata> committedOffsets = admin.listConsumerGroupOffsets(consumerGroup)
                    .partitionsToOffsetAndMetadata().get(timeoutMs, TimeUnit.MILLISECONDS);

            List<TopicPartition> allPartitions = new ArrayList<>();
            for (String topic : subscribedTopics) {
                TopicDescription desc = descriptions.get(topic);
                if (desc != null) {
                    desc.partitions().forEach(p -> allPartitions.add(new TopicPartition(topic, p.partition())));
                }
            }

            Map<String, Long> unreadByTopic = new HashMap<>();
            Map<String, Instant> oldestByTopic = new HashMap<>();
            for (String topic : subscribedTopics) {
                unreadByTopic.put(topic, 0L);
            }

            if (!allPartitions.isEmpty()) {
                Map<String, Object> consumerProps = Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                        ConsumerConfig.CLIENT_ID_CONFIG, "kitchen-monitor-" + UUID.randomUUID(),
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                        ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) timeoutMs,
                        ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, (int) timeoutMs
                );
                try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
                    Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(allPartitions, Duration.ofMillis(timeoutMs));
                    Map<TopicPartition, Long> endOffsets = consumer.endOffsets(allPartitions, Duration.ofMillis(timeoutMs));

                    for (TopicPartition tp : allPartitions) {
                        OffsetAndMetadata committedMeta = committedOffsets.get(tp);
                        long beginning = beginningOffsets.getOrDefault(tp, 0L);
                        long committed = committedMeta != null ? Math.max(committedMeta.offset(), beginning) : beginning;
                        long end = endOffsets.getOrDefault(tp, 0L);
                        long unread = Math.max(0L, end - committed);
                        if (unread > 0L) {
                            unreadByTopic.merge(tp.topic(), unread, Long::sum);
                            consumer.assign(Collections.singletonList(tp));
                            consumer.seek(tp, committed);
                            var records = consumer.poll(Duration.ofMillis(Math.min(1000L, timeoutMs)));
                            for (ConsumerRecord<String, String> rec : records.records(tp)) {
                                Instant ts = Instant.ofEpochMilli(rec.timestamp());
                                oldestByTopic.merge(tp.topic(), ts, (a, b) -> a.isBefore(b) ? a : b);
                                break;
                            }
                        }
                    }
                }
            }

            boolean anyWarning = false;
            List<TopicLagStatus> topicStatuses = new ArrayList<>(subscribedTopics.size());
            for (String topic : subscribedTopics) {
                long unread = unreadByTopic.getOrDefault(topic, 0L);
                Instant oldest = oldestByTopic.get(topic);
                long ageSeconds = oldest == null ? 0L : Math.max(0L, Duration.between(oldest, now).getSeconds());
                boolean slowWarning = oldest != null && ageSeconds > thresholdSeconds;
                if (slowWarning) {
                    anyWarning = true;
                }
                topicStatuses.add(new TopicLagStatus(topic, unread, oldest, ageSeconds, slowWarning));
            }
            topicStatuses.sort(Comparator.comparing(TopicLagStatus::topic));

            return new KitchenMonitorResponse(
                    properties.instanceId(),
                    consumerGroup,
                    true,
                    null,
                    thresholdSeconds,
                    anyWarning,
                    pendingOutbox,
                    topicStatuses,
                    now);
        } catch (Exception ex) {
            return new KitchenMonitorResponse(
                    properties.instanceId(),
                    consumerGroup,
                    false,
                    ex.getMessage(),
                    thresholdSeconds,
                    false,
                    pendingOutbox,
                    List.of(),
                    now);
        }
    }
}
