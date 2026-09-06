package inno.ipr.ingestion_service.config;

import inno.ipr.ingestion_service.exception.InvalidTelemetryException;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DataIntegrityViolationException;
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
 * Kafka topology and resilience for the ingestion consumer.
 *
 * <ul>
 *   <li>{@link NewTopic} beans declare {@code city-telemetry} and its dead-letter topic; the
 *       autoconfigured {@code KafkaAdmin} creates them on startup, so no manual topic setup is
 *       needed. {@code city-telemetry} is partitioned to preserve per-{@code sensor_id} ordering
 *       (messages are keyed by sensor id).</li>
 *   <li>{@link DefaultErrorHandler} classifies failures: poison records go straight to
 *       {@code city-telemetry.DLQ} via a {@link DeadLetterPublishingRecoverer} — the Dead Letter
 *       Queue pattern that isolates them instead of blocking the partition — while transient
 *       infrastructure failures are retried indefinitely rather than discarded.</li>
 * </ul>
 *
 * <p>The listener is a batch listener ({@code spring.kafka.listener.type: batch}). The same
 * {@link DefaultErrorHandler} handles both modes; per-record classification within a batch relies
 * on the listener throwing {@code BatchListenerFailedException} with the failing index (see
 * {@code TelemetryConsumer}). The classifier traverses causes, so the non-retryable types below are
 * recognised whether they arrive bare or wrapped in that exception.
 */
@Configuration
public class KafkaConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConfig.class);

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
     * Scheduler that resumes a paused listener container once its back-off elapses. Declared here
     * rather than relying on Boot's, which is only autoconfigured when the app enables scheduling.
     */
    @Bean
    TaskScheduler kafkaBackOffScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(1);
        scheduler.setThreadNamePrefix("kafka-backoff-");
        return scheduler;
    }

    /**
     * Error handler for the listener container. It splits the two failure modes apart, because one
     * uniform retry policy gets both of them backwards:
     *
     * <ul>
     *   <li><b>Bad data</b> ({@link InvalidTelemetryException}, {@link DataIntegrityViolationException})
     *       is non-retryable — no amount of waiting makes a malformed payload insertable — so it goes
     *       straight to {@code city-telemetry.DLQ} instead of stalling its partition first.</li>
     *   <li><b>Infrastructure failures</b> (TimescaleDB restarting, pool exhausted) are retried with
     *       an unlimited exponential back-off, so valid telemetry is never dead-lettered into a topic
     *       nothing consumes. Kafka is already holding the data; waiting for the database beats
     *       punching an unrecoverable hole in the hypertable.</li>
     * </ul>
     *
     * <p>The back-off runs through a {@link ContainerPausingBackOffHandler}, which pauses the
     * container instead of sleeping the consumer thread. Blocking would run a poll batch past
     * {@code max.poll.interval.ms} during any sustained outage and drop the consumer into a
     * rebalance loop.
     *
     * <p>{@code DefaultErrorHandler} is picked up automatically by Spring Boot's Kafka
     * autoconfiguration when exposed as a bean, so it applies to the {@code @KafkaListener} without
     * further wiring.
     */
    @Bean
    DefaultErrorHandler errorHandler(KafkaTemplate<String, String> dlqKafkaTemplate,
                                     ListenerContainerRegistry registry,
                                     TaskScheduler kafkaBackOffScheduler) {
        DeadLetterPublishingRecoverer dlq = new DeadLetterPublishingRecoverer(dlqKafkaTemplate,
                (record, exception) -> new TopicPartition(DLQ_TOPIC, record.partition()));

        // Log the offending payload before it leaves for the DLQ — otherwise the only trace an
        // operator gets is an exception message with no sight of the record that caused it.
        ConsumerRecordRecoverer recoverer = (record, exception) -> {
            log.error("Dead-lettering record from {}-{}@{} to {}: {} | payload={}",
                    record.topic(), record.partition(), record.offset(), DLQ_TOPIC,
                    exception.getMessage(), rawPayload(exception, record), exception);
            dlq.accept(record, exception);
        };

        ExponentialBackOff backOff = new ExponentialBackOff(1_000L, 2.0);
        backOff.setMaxInterval(30_000L); // unlimited attempts: max elapsed time stays at its default
        BackOffHandler pausing = new ContainerPausingBackOffHandler(
                new ListenerContainerPauseService(registry, kafkaBackOffScheduler));

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff, pausing);
        handler.addNotRetryableExceptions(
                InvalidTelemetryException.class, DataIntegrityViolationException.class);
        return handler;
    }

    /**
     * The payload as the parser saw it, pulled off {@link InvalidTelemetryException} where it is
     * retained for exactly this purpose. The container wraps listener failures, so the cause chain
     * has to be walked. Falls back to the record's own value for failures raised elsewhere.
     */
    private static String rawPayload(Exception exception, ConsumerRecord<?, ?> record) {
        for (Throwable t = exception; t != null; t = t.getCause()) {
            if (t instanceof InvalidTelemetryException invalid) {
                return invalid.payload();
            }
            if (t.getCause() == t) {
                break;
            }
        }
        return String.valueOf(record.value());
    }
}
