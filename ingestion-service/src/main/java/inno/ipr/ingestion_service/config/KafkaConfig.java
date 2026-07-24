package inno.ipr.ingestion_service.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka topology and resilience for the ingestion consumer.
 *
 * <ul>
 *   <li>{@link NewTopic} beans declare {@code city-telemetry} and its dead-letter topic; the
 *       autoconfigured {@code KafkaAdmin} creates them on startup, so no manual topic setup is
 *       needed. {@code city-telemetry} is partitioned to preserve per-{@code sensor_id} ordering
 *       (messages are keyed by sensor id).</li>
 *   <li>{@link DefaultErrorHandler} retries a failed record a couple of times, then a
 *       {@link DeadLetterPublishingRecoverer} republishes it to {@code city-telemetry.DLQ} — the
 *       Dead Letter Queue pattern that isolates poison messages instead of blocking the partition.</li>
 * </ul>
 */
@Configuration
public class KafkaConfig {

    public static final String TOPIC = "city-telemetry";
    public static final String DLQ_TOPIC = "city-telemetry.DLQ";

    private static final int PARTITIONS = 3;
    private static final short REPLICAS = 1; // single-broker local dev; Strimzi sets this in K8s

    @Bean
    NewTopic cityTelemetryTopic() {
        return TopicBuilder.name(TOPIC).partitions(PARTITIONS).replicas(REPLICAS).build();
    }

    @Bean
    NewTopic cityTelemetryDlqTopic() {
        return TopicBuilder.name(DLQ_TOPIC).partitions(PARTITIONS).replicas(REPLICAS).build();
    }

    /**
     * Dedicated producer for dead-lettering. {@link StringSerializer} matches the {@code String}
     * payloads the listener consumes ({@code StringDeserializer}), so a poison record republishes to
     * the DLQ verbatim. Defined explicitly rather than relying on the autoconfigured template, which
     * this consumer-only service does not otherwise need.
     */
    @Bean
    KafkaTemplate<String, String> dlqKafkaTemplate(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        ProducerFactory<String, String> factory = new DefaultKafkaProducerFactory<>(config);
        return new KafkaTemplate<>(factory);
    }

    /**
     * Error handler for the listener container: two retries with a 1s pause, then dead-letter.
     * {@code DefaultErrorHandler} is picked up automatically by Spring Boot's Kafka autoconfiguration
     * when exposed as a bean, so it applies to the {@code @KafkaListener} without further wiring.
     */
    @Bean
    DefaultErrorHandler errorHandler(KafkaTemplate<String, String> dlqKafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(dlqKafkaTemplate,
                (record, exception) -> new TopicPartition(DLQ_TOPIC, record.partition()));
        return new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 2L));
    }
}
