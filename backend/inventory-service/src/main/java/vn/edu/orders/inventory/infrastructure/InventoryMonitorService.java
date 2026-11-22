package vn.edu.orders.inventory.infrastructure;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import vn.edu.orders.contracts.events.EventTopics;
import vn.edu.orders.inventory.api.MonitorResponse;
import vn.edu.orders.inventory.api.MonitorResponse.TopicLag;
import vn.edu.orders.inventory.config.InventoryProperties;

@Service
public class InventoryMonitorService {
    private static final Logger log = LoggerFactory.getLogger(InventoryMonitorService.class);

    private final InventoryQueryStore queryStore;
    private final InventoryProperties properties;
    private final Clock clock;
    private final String bootstrapServers;
    private final String groupId;

    public InventoryMonitorService(
            InventoryQueryStore queryStore,
            InventoryProperties properties,
            Clock clock,
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${spring.kafka.consumer.group-id}") String groupId
    ) {
        this.queryStore = queryStore;
        this.properties = properties;
        this.clock = clock;
        this.bootstrapServers = bootstrapServers;
        this.groupId = groupId;
    }

    public MonitorResponse getMonitorStatus() {
        long needsCheckCount = queryStore.countNeedsCheckIngredients();
        long pendingOutboxCount = queryStore.countPendingOutbox();
        long alertsCount = queryStore.countAlerts();
        long thresholdSeconds = properties.slowConsumerThreshold().toSeconds();

        boolean kafkaReachable = false;
        boolean anySlowWarning = false;
        List<TopicLag> topicLags = List.of();

        try {
            topicLags = inspectKafkaLag(thresholdSeconds);
            kafkaReachable = true;
            anySlowWarning = topicLags.stream().anyMatch(TopicLag::slowWarning);
        } catch (Exception ex) {
            log.debug("Kafka monitor probe unavailable (bootstrap={}): {}", bootstrapServers, ex.toString());
        }

        return new MonitorResponse(
                properties.instanceId(),
                kafkaReachable,
                thresholdSeconds,
                anySlowWarning,
                topicLags,
                needsCheckCount,
                pendingOutboxCount,
                alertsCount);
    }

    private List<TopicLag> inspectKafkaLag(long thresholdSeconds) throws Exception {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        adminProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 1500);
        adminProps.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 2000);

        try (AdminClient admin = AdminClient.create(adminProps)) {
            Set<String> allTopics = admin.listTopics().names().get(2, TimeUnit.SECONDS);
            List<String> relevantTopics = allTopics.stream()
                    .filter(t -> EventTopics.isOrderEventsTopic(t) || EventTopics.STOCK_COMMANDS.equals(t))
                    .sorted()
                    .toList();
            if (relevantTopics.isEmpty()) {
                return List.of();
            }

            Map<String, TopicDescription> descriptions = admin.describeTopics(relevantTopics)
                    .allTopicNames().get(2, TimeUnit.SECONDS);
            List<TopicPartition> partitions = new ArrayList<>();
            for (String topic : relevantTopics) {
                TopicDescription desc = descriptions.get(topic);
                if (desc != null) {
                    for (TopicPartitionInfo p : desc.partitions()) {
                        partitions.add(new TopicPartition(topic, p.partition()));
                    }
                }
            }
            if (partitions.isEmpty()) {
                return List.of();
            }

            Map<TopicPartition, OffsetAndMetadata> committed = admin.listConsumerGroupOffsets(groupId)
                    .partitionsToOffsetAndMetadata().get(2, TimeUnit.SECONDS);

            return measureUnreadAges(relevantTopics, partitions, committed, thresholdSeconds);
        }
    }

    private List<TopicLag> measureUnreadAges(
            List<String> relevantTopics,
            List<TopicPartition> partitions,
            Map<TopicPartition, OffsetAndMetadata> committed,
            long thresholdSeconds
    ) {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "inventory-monitor-" + UUID.randomUUID());
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        consumerProps.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "1500");
        consumerProps.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "2000");

        Map<String, Long> unreadByTopic = new TreeMap<>();
        Map<String, Integer> partitionCountByTopic = new HashMap<>();
        Map<String, Long> maxAgeByTopic = new HashMap<>();
        for (String topic : relevantTopics) {
            unreadByTopic.put(topic, 0L);
            partitionCountByTopic.put(topic, 0);
        }

        long nowMs = clock.millis();
        try (KafkaConsumer<String, String> peekConsumer = new KafkaConsumer<>(consumerProps)) {
            Map<TopicPartition, Long> endOffsets = peekConsumer.endOffsets(partitions, Duration.ofMillis(1500));
            Map<TopicPartition, Long> beginningOffsets = peekConsumer.beginningOffsets(partitions, Duration.ofMillis(1500));

            for (TopicPartition tp : partitions) {
                partitionCountByTopic.merge(tp.topic(), 1, Integer::sum);
                long endOffset = endOffsets.getOrDefault(tp, 0L);
                long beginOffset = beginningOffsets.getOrDefault(tp, 0L);
                OffsetAndMetadata meta = committed.get(tp);
                long startOffset = meta == null ? beginOffset : Math.max(meta.offset(), beginOffset);
                long unread = Math.max(0L, endOffset - startOffset);
                unreadByTopic.merge(tp.topic(), unread, Long::sum);

                if (unread > 0) {
                    Long oldestTs = peekRecordTimestamp(peekConsumer, tp, startOffset);
                    if (oldestTs != null) {
                        long ageSeconds = Math.max(0L, (nowMs - oldestTs) / 1000L);
                        maxAgeByTopic.merge(tp.topic(), ageSeconds, Math::max);
                    }
                }
            }
        }

        List<TopicLag> result = new ArrayList<>();
        for (String topic : relevantTopics) {
            long totalUnread = unreadByTopic.getOrDefault(topic, 0L);
            Long oldestAge = maxAgeByTopic.get(topic);
            boolean slowWarning = totalUnread > 0 && oldestAge != null && oldestAge >= thresholdSeconds;
            result.add(new TopicLag(
                    topic,
                    partitionCountByTopic.getOrDefault(topic, 0),
                    totalUnread,
                    oldestAge,
                    slowWarning));
        }
        return result;
    }

    private Long peekRecordTimestamp(
            KafkaConsumer<String, String> consumer,
            TopicPartition tp,
            long offset
    ) {
        consumer.assign(List.of(tp));
        consumer.seek(tp, offset);
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
        for (ConsumerRecord<String, String> record : records.records(tp)) {
            if (record.offset() >= offset) {
                return record.timestamp();
            }
        }
        return null;
    }
}
