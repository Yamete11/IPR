package inno.ipr.ingestion_service.config;

import inno.ipr.ingestion_service.exception.InvalidTelemetryException;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.BackOffHandler;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.ContainerPausingBackOffHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.ListenerContainerPauseService;
import org.springframework.kafka.listener.ListenerContainerRegistry;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.ExponentialBackOff;

/**
 * Kafka topics and the error policy of the {@code city-telemetry} listener.
 *
 * <p>{@link #cityTelemetryTopic()} and {@link #cityTelemetryDlqTopic()} declare the topics so the
 * autoconfigured {@code KafkaAdmin} creates them for local development; in Kubernetes Strimzi owns
 * them (6 partitions) and these beans are no-ops. {@code REPLICAS} is 1 for the single local broker.
 *
 * <p>{@link #errorHandler(KafkaTemplate, ListenerContainerRegistry, TaskScheduler)} splits failures
 * into two kinds, because one uniform retry policy gets both backwards:
 * <ul>
 *   <li><b>Bad data</b> ({@link InvalidTelemetryException}, {@link DataIntegrityViolationException})
 *       is never retried — waiting does not make a malformed payload insertable — and goes straight
 *       to {@code city-telemetry.DLQ}.</li>
 *   <li><b>Everything else</b> (TimescaleDB restarting, pool exhausted) is retried without limit.
 *       Kafka is already holding the data, so waiting for the database beats dropping valid
 *       telemetry into a topic nothing consumes.</li>
 * </ul>
 * The classification walks the cause chain, so it also works when the listener wraps the exception
 * in {@code BatchListenerFailedException} to name the failing record of a batch. The handler is
 * picked up by Boot's Kafka autoconfiguration simply by being a bean. It dead-letters through Boot's
 * autoconfigured {@link KafkaTemplate}, whose default {@code StringSerializer}s republish the record
 * verbatim.
 *
 * <p>{@code deadLetter} logs the record before publishing it, so the operator sees the offending
 * payload, not just an exception message. The DLQ partition is left to the producer ({@code -1})
 * instead of mirroring the source partition: {@code city-telemetry} has 6 partitions in Kubernetes,
 * and pinning a record onto a DLQ partition that does not exist fails the publish and stalls the
 * source partition.
 *
 * <p>{@code unlimitedBackOff} waits 1 s, 2 s, 4 s … capped at 30 s, with no limit on attempts (the
 * max elapsed time keeps its unbounded default).
 *
 * <p>{@code pauseWhileWaiting} pauses the listener container during the back-off instead of
 * sleeping the consumer thread: a sleep longer than {@code max.poll.interval.ms} would make the
 * broker evict the consumer and start a rebalance loop. {@link #kafkaBackOffScheduler()} resumes the
 * container afterwards; it is declared here because Boot only provides a scheduler when
 * {@code @EnableScheduling} is on, and this app does not schedule anything else.
 */
@Configuration
public class KafkaConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConfig.class);

    public static final String TOPIC = "city-telemetry";
    public static final String DLQ_TOPIC = "city-telemetry.DLQ";

    private static final int PARTITIONS = 3;
    private static final short REPLICAS = 1;

    @Bean
    NewTopic cityTelemetryTopic() {
        return TopicBuilder.name(TOPIC).partitions(PARTITIONS).replicas(REPLICAS).build();
    }

    @Bean
    NewTopic cityTelemetryDlqTopic() {
        return TopicBuilder.name(DLQ_TOPIC).partitions(PARTITIONS).replicas(REPLICAS).build();
    }

    @Bean
    TaskScheduler kafkaBackOffScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(1);
        scheduler.setThreadNamePrefix("kafka-backoff-");
        return scheduler;
    }

    @Bean
    DefaultErrorHandler errorHandler(KafkaTemplate<String, String> kafkaTemplate,
                                     ListenerContainerRegistry registry,
                                     TaskScheduler kafkaBackOffScheduler) {
        DefaultErrorHandler handler = new DefaultErrorHandler(
                deadLetter(kafkaTemplate),
                unlimitedBackOff(),
                pauseWhileWaiting(registry, kafkaBackOffScheduler));
        handler.addNotRetryableExceptions(InvalidTelemetryException.class, DataIntegrityViolationException.class);
        return handler;
    }

    private static ConsumerRecordRecoverer deadLetter(KafkaTemplate<String, String> kafkaTemplate) {
        DeadLetterPublishingRecoverer publisher = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (record, exception) -> new TopicPartition(DLQ_TOPIC, -1));
        return (record, exception) -> {
            log.error("Dead-lettering record from {}-{}@{} to {}: {} | payload={}",
                    record.topic(), record.partition(), record.offset(), DLQ_TOPIC,
                    exception.getMessage(), record.value(), exception);
            publisher.accept(record, exception);
        };
    }

    private static BackOff unlimitedBackOff() {
        ExponentialBackOff backOff = new ExponentialBackOff(1_000L, 2.0);
        backOff.setMaxInterval(30_000L);
        return backOff;
    }

    private static BackOffHandler pauseWhileWaiting(ListenerContainerRegistry registry, TaskScheduler scheduler) {
        return new ContainerPausingBackOffHandler(new ListenerContainerPauseService(registry, scheduler));
    }
}
