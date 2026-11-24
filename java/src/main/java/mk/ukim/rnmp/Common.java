package mk.ukim.rnmp;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.ParameterTool;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Collection;
import java.util.concurrent.ExecutionException;

/** Shared Kafka and event-time helpers */
public final class Common {
    private static final ObjectMapper JSON = new ObjectMapper();

    private Common() {}

    public static String bootstrap(ParameterTool args) {
        return args.get("bootstrap-servers", args.get("bootstrap", "localhost:9092"));
    }

    public static OffsetsInitializer startingOffsets(String value) {
        return switch (value) {
            case "earliest" -> OffsetsInitializer.earliest();
            case "latest" -> OffsetsInitializer.latest();
            default -> throw new IllegalArgumentException("--start-offset must be earliest or latest");
        };
    }

    public static KafkaSource<GitHubEvent> eventSource(String bootstrap, String topic, String groupId,
                                                       OffsetsInitializer offsets, boolean bounded) {
        var source = KafkaSource.<GitHubEvent>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(offsets)
                .setDeserializer(new EventDeserializer());
        if (bounded) source.setBounded(OffsetsInitializer.latest());
        return source.build();
    }

    public static WatermarkStrategy<GitHubEvent> watermarks(long disorderSeconds, long idleSeconds) {
        WatermarkStrategy<GitHubEvent> strategy = WatermarkStrategy
                .<GitHubEvent>forBoundedOutOfOrderness(Duration.ofSeconds(disorderSeconds))
                .withTimestampAssigner((event, kafkaTimestamp) -> event.timestamp);
        if (idleSeconds > 0) strategy = strategy.withIdleness(Duration.ofSeconds(idleSeconds));
        return strategy;
    }

    public static void prepareTopics(String bootstrapServers, Collection<String> topics) throws Exception {

        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", bootstrapServers))) {
            var created = admin.createTopics(topics.stream()
                    .map(topic -> new NewTopic(topic, 1, (short) 1).configs(Map.of("retention.ms", "-1")))
                    .toList());
            for (var future : created.values().values()) {
                try {
                    future.get();
                } catch (ExecutionException error) {
                    if (!(error.getCause() instanceof TopicExistsException)) throw error;
                }
            }
        }
    }

    public static String json(Map<String, Object> value) throws IOException {
        return JSON.writeValueAsString(value);
    }

    public static KafkaSink<OutputRecord> sink(String bootstrapServers, String topic) {
        return KafkaSink.<OutputRecord>builder()
                .setBootstrapServers(bootstrapServers)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setRecordSerializer(new JsonSerializer(topic))
                .build();
    }

    public static class EventDeserializer implements KafkaRecordDeserializationSchema<GitHubEvent> {
        @Override
        public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<GitHubEvent> output) throws IOException {
            if (record.value() == null) {
                throw new IOException("Expected a JSON event value");
            }
            JsonNode row = JSON.readTree(record.value());
            JsonNode timestamp = row.get("timestamp");
            if (timestamp == null || !timestamp.isIntegralNumber() || !timestamp.canConvertToLong()
                    || timestamp.longValue() < 0) {
                throw new IOException("Event timestamp must be epoch milliseconds on or after 1970-01-01");
            }
            JsonNode actor = row.path("actor").path("id");
            if ((!actor.isIntegralNumber() && !actor.isTextual()) || actor.asText().isBlank()) {
                throw new IOException("GitHub event actor.id must identify the acting user");
            }

            output.collect(new GitHubEvent(
                    actor.asText(), row.path("actor").path("login").asText(null),
                    row.path("repo").path("id").asText(null),
                    row.path("repo").path("name").asText(null),
                    row.path("type").asText(null), timestamp.longValue()));
        }

        @Override
        public TypeInformation<GitHubEvent> getProducedType() {
            return TypeInformation.of(GitHubEvent.class);
        }
    }

    public static class JsonSerializer implements KafkaRecordSerializationSchema<OutputRecord> {
        private final String topic;

        public JsonSerializer(String topic) {
            this.topic = topic;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(OutputRecord value, KafkaSinkContext context, Long timestamp) {
            return new ProducerRecord<>(topic, null, value.timestamp,
                    value.key.getBytes(StandardCharsets.UTF_8), value.json.getBytes(StandardCharsets.UTF_8));
        }
    }
}
