package inno.ipr.anomaly_detector_service.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka topology for the detector — a producer only (no {@code @KafkaListener}, no DLQ, since
 * nothing is consumed).
 *
 * <ul>
 *   <li>The {@link NewTopic} bean declares {@code infrastructure-alerts}; the autoconfigured
 *       {@code KafkaAdmin} creates it on startup. It is partitioned and alerts are keyed by
 *       {@code sensor_id}, so all alerts for one sensor keep their order on a single partition.</li>
 *   <li>An explicit {@link KafkaTemplate} with {@link StringSerializer} for key and value — the
 *       payload is JSON serialised to a {@code String} by the publisher — mirroring the producer
 *       shape in ingestion-service's Kafka config.</li>
 * </ul>
 */
@Configuration
public class KafkaConfig {

    public static final String ALERTS_TOPIC = "infrastructure-alerts";

    private static final int PARTITIONS = 3;
    private static final short REPLICAS = 1; // single-broker local dev; Strimzi sets this in K8s

    @Bean
    NewTopic infrastructureAlertsTopic() {
        return TopicBuilder.name(ALERTS_TOPIC).partitions(PARTITIONS).replicas(REPLICAS).build();
    }

    @Bean
    KafkaTemplate<String, String> alertKafkaTemplate(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        ProducerFactory<String, String> factory = new DefaultKafkaProducerFactory<>(config);
        return new KafkaTemplate<>(factory);
    }
}
