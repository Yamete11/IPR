package inno.ipr.ai_incident_responder_service.config;

import inno.ipr.ai_incident_responder_service.exception.InvalidAlertException;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.apache.kafka.clients.admin.NewTopic;
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
 * Kafka wiring for the responder — a consumer of {@code infrastructure-alerts} with a DLQ producer.
 *
 * <ul>
 *   <li>{@link NewTopic} declares the dead-letter topic; the autoconfigured {@code KafkaAdmin}
 *       creates it. (The source topic {@code infrastructure-alerts} is owned/created by
 *       anomaly-detector-service.)</li>
 *   <li>{@link DefaultErrorHandler} retries transient failures (Claude/DB) a few times, then a
 *       {@link DeadLetterPublishingRecoverer} republishes to {@code infrastructure-alerts.DLQ}.
 *       {@link InvalidAlertException} (poison message) is registered as <em>not retryable</em>, so a
 *       malformed alert dead-letters immediately instead of burning retries.</li>
 * </ul>
 */
@Configuration
public class KafkaConfig {

    public static final String ALERTS_TOPIC = "infrastructure-alerts";
    public static final String DLQ_TOPIC = "infrastructure-alerts.DLQ";

    private static final int PARTITIONS = 3;
    private static final short REPLICAS = 1; // single-broker local dev; Strimzi sets this in K8s

    @Bean
    NewTopic infrastructureAlertsDlqTopic() {
        return TopicBuilder.name(DLQ_TOPIC).partitions(PARTITIONS).replicas(REPLICAS).build();
    }

    /**
     * Dedicated producer for dead-lettering. {@link StringSerializer} matches the {@code String}
     * payloads the listener consumes, so a poison record republishes to the DLQ verbatim.
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
     * Error handler for the listener container: three retries with a 2s pause for transient
     * failures, then dead-letter. Poison messages ({@link InvalidAlertException}) skip the retries
     * and dead-letter at once. Exposed as a bean so Boot's Kafka autoconfiguration applies it.
     */
    @Bean
    DefaultErrorHandler errorHandler(KafkaTemplate<String, String> dlqKafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(dlqKafkaTemplate,
                (record, exception) -> new TopicPartition(DLQ_TOPIC, record.partition()));
        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, new FixedBackOff(2000L, 3L));
        handler.addNotRetryableExceptions(InvalidAlertException.class);
        return handler;
    }
}
