package inno.ipr.ai_incident_responder_service.config;

import inno.ipr.ai_incident_responder_service.exception.InvalidAlertException;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.BackOffHandler;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.ContainerPausingBackOffHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.ListenerContainerPauseService;
import org.springframework.kafka.listener.ListenerContainerRegistry;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.util.backoff.ExponentialBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka wiring for the responder — a consumer of {@code infrastructure-alerts} with a DLQ producer.
 *
 * <ul>
 *   <li>{@link NewTopic} declares the dead-letter topic; the autoconfigured {@code KafkaAdmin}
 *       creates it. (The source topic {@code infrastructure-alerts} is owned/created by
 *       anomaly-detector-service.)</li>
 *   <li>{@link DefaultErrorHandler} splits the two failure modes apart, mirroring
 *       ingestion-service: a poison alert ({@link InvalidAlertException}) dead-letters at once,
 *       while transient Claude/DB failures are retried with an unlimited exponential back-off.</li>
 * </ul>
 */
@Configuration
public class KafkaConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConfig.class);

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
     * Scheduler that resumes a paused listener container once its back-off elapses. Declared here
     * rather than relying on the autoconfigured one, which Boot only provides when the app enables
     * scheduling (this service does not).
     */
    @Bean
    TaskScheduler kafkaBackOffScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(1);
        scheduler.setThreadNamePrefix("kafka-backoff-");
        return scheduler;
    }

    /**
     * Error handler for the listener container. The two failure modes need opposite policies:
     *
     * <ul>
     *   <li><b>Bad data</b> ({@link InvalidAlertException}) is non-retryable — no amount of waiting
     *       makes a malformed alert parseable — so it goes straight to
     *       {@code infrastructure-alerts.DLQ} instead of stalling its partition first.</li>
     *   <li><b>Everything else</b> — a Claude outage, an open circuit breaker, TimescaleDB or age-db
     *       restarting — is retried with an unlimited exponential back-off. A short fixed budget is
     *       actively harmful here: the breaker stays open for 30s at a stretch, so a handful of
     *       retries spaced a couple of seconds apart all fail fast with
     *       {@code CallNotPermittedException} and dead-letter a <em>valid</em> alert into a topic
     *       nothing consumes — the precise outcome the breaker exists to prevent. Kafka is already
     *       holding the alert; waiting for Claude to come back beats losing an incident.</li>
     * </ul>
     *
     * <p>The back-off runs through a {@link ContainerPausingBackOffHandler}, which pauses the
     * container instead of sleeping the consumer thread. Blocking would run a poll batch past
     * {@code max.poll.interval.ms} during any sustained outage and drop the consumer into a
     * rebalance loop.
     */
    @Bean
    DefaultErrorHandler errorHandler(KafkaTemplate<String, String> dlqKafkaTemplate,
                                     ListenerContainerRegistry registry,
                                     TaskScheduler kafkaBackOffScheduler) {
        DeadLetterPublishingRecoverer dlq = new DeadLetterPublishingRecoverer(dlqKafkaTemplate,
                (record, exception) -> new TopicPartition(DLQ_TOPIC, record.partition()));

        // Log the offending record before it leaves for the DLQ — otherwise the only trace an
        // operator gets is an exception message with no sight of the record that caused it.
        ConsumerRecordRecoverer recoverer = (record, exception) -> {
            log.error("Dead-lettering record from {}-{}@{} to {}: {} | payload={}",
                    record.topic(), record.partition(), record.offset(), DLQ_TOPIC,
                    exception.getMessage(), record.value(), exception);
            dlq.accept(record, exception);
        };

        ExponentialBackOff backOff = new ExponentialBackOff(1_000L, 2.0);
        backOff.setMaxInterval(30_000L); // unlimited attempts: max elapsed time stays at its default
        BackOffHandler pausing = new ContainerPausingBackOffHandler(
                new ListenerContainerPauseService(registry, kafkaBackOffScheduler));

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff, pausing);
        handler.addNotRetryableExceptions(InvalidAlertException.class);
        return handler;
    }
}
