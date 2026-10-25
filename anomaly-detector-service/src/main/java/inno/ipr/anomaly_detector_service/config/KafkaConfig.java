package inno.ipr.anomaly_detector_service.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Kafka topology for the detector, which only produces. Alerts go through Boot's autoconfigured
 * {@code KafkaTemplate} (its default {@code StringSerializer}s carry the JSON the publisher writes),
 * keyed by {@code sensor_id} so one sensor's alerts keep their order on a single partition.
 *
 * <p>{@link #infrastructureAlertsTopic()} declares {@code infrastructure-alerts}; the autoconfigured
 * {@code KafkaAdmin} creates it on startup. {@code REPLICAS} is 1 for the single-broker local setup;
 * in Kubernetes the Strimzi {@code KafkaTopic} owns the topic.
 */
@Configuration
public class KafkaConfig {

    public static final String ALERTS_TOPIC = "infrastructure-alerts";

    private static final int PARTITIONS = 3;
    private static final short REPLICAS = 1;

    @Bean
    NewTopic infrastructureAlertsTopic() {
        return TopicBuilder.name(ALERTS_TOPIC).partitions(PARTITIONS).replicas(REPLICAS).build();
    }
}
