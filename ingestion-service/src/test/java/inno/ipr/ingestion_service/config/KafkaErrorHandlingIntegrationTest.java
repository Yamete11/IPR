package inno.ipr.ingestion_service.config;

import inno.ipr.ingestion_service.repository.MetricRepository;
import inno.ipr.ingestion_service.telemetry.TelemetryParser;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Verifies the failure classification in {@link KafkaConfig} against a real (in-JVM) broker.
 *
 * <p>The distinction under test is the one a single uniform retry policy gets backwards in both
 * directions: bad data must dead-letter immediately rather than stall its partition, while a
 * transient database failure must be retried rather than dropped into a topic nothing consumes.
 * Both halves are behavioural — they live in the {@code DefaultErrorHandler} wiring, not in any unit
 * that can be called directly — so this drives them through the broker end to end.
 *
 * <p>The datasource is excluded and {@link MetricRepository} mocked: the point here is the error
 * handler's reaction to a repository outcome, and injecting the outcome directly is both faster and
 * far more precise than provoking a real database into failing on cue.
 */
@SpringBootTest(properties = {
        "spring.autoconfigure.exclude=org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration",
        "spring.sql.init.mode=never",
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.auto-offset-reset=earliest",
})
@EmbeddedKafka(partitions = 1)
class KafkaErrorHandlingIntegrationTest {

    /** Generous relative to the 1s/2s/4s… back-off, so a slow CI box does not turn into a failure. */
    private static final Duration TIMEOUT = Duration.ofSeconds(60);

    /** Four consecutive failures — one more than the initial attempt plus two retries. */
    private static final int FAILURES_EXCEEDING_A_BOUNDED_POLICY = 4;

    @Autowired
    private EmbeddedKafkaBroker broker;

    @MockitoBean
    private MetricRepository metrics;

    /**
     * Spied rather than mocked, purely to count delivery attempts: a poison record never reaches the
     * repository, so the parser is the only place a redelivery is observable.
     */
    @MockitoSpyBean
    private TelemetryParser parser;

    private Producer<String, String> producer;
    private Consumer<String, String> dlqConsumer;

    @BeforeEach
    void setUp() {
        producer = new KafkaProducer<>(
                KafkaTestUtils.producerProps(broker), new StringSerializer(), new StringSerializer());
        // A fresh group per test reading from the beginning: the DLQ accumulates across tests, so
        // assertions match on a per-test marker rather than on the topic being empty.
        Map<String, Object> props = KafkaTestUtils.consumerProps(broker, "dlq-" + UUID.randomUUID(), false);
        dlqConsumer = new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer());
        dlqConsumer.subscribe(List.of(KafkaConfig.DLQ_TOPIC));
    }

    @AfterEach
    void tearDown() {
        producer.close(Duration.ofSeconds(5));
        dlqConsumer.close(Duration.ofSeconds(5));
    }

    @Test
    @DisplayName("a malformed payload dead-letters without ever reaching the database")
    void poisonRecordIsDeadLetteredAndNeverPersisted() {
        String marker = marker();
        String payload = "{\"sensor_id\":\"" + marker + "\"";

        send(payload);

        assertThat(awaitDlqRecord(marker)).isEqualTo(payload);
        verify(metrics, never()).save(any());
    }

    @Test
    @DisplayName("a validation failure dead-letters on the first attempt, with no retry")
    void invalidTelemetryIsNotRetried() {
        String marker = marker();
        // 'value' omitted — well-formed JSON that must not be ingested as 0.0.
        String payload = """
                {"timestamp":"%s","sensor_id":"%s","metric_type":"substation_voltage","status":"OK"}"""
                .formatted(Instant.now(), marker);

        send(payload);

        assertThat(awaitDlqRecord(marker)).isEqualTo(payload);
        verify(metrics, never()).save(any());
        // The actual no-retry claim: one delivery, straight to the DLQ. Under a policy that retries
        // everything this is 3, and the partition sits blocked for the whole back-off first.
        verify(parser, times(1)).parse(payload);
    }

    @Test
    @DisplayName("a valid reading is persisted and never dead-lettered")
    void validRecordIsPersisted() {
        String marker = marker();
        given(metrics.save(any())).willReturn(1);

        send(valid(marker, 4.25));

        await().atMost(TIMEOUT).untilAsserted(() -> verify(metrics).save(any()));
        assertThat(pollDlq(marker, Duration.ofSeconds(5))).isNull();
    }

    /**
     * The half that used to lose data: a database blip made {@code save} throw, the record exhausted
     * its two retries within ~3 seconds and was dead-lettered — valid telemetry gone from the
     * hypertable for good, since nothing consumes the DLQ.
     */
    @Test
    @DisplayName("a transient database failure is retried until it succeeds, not dead-lettered")
    void transientDatabaseFailureIsRetriedNotDeadLettered() {
        String marker = marker();
        AtomicInteger attempts = new AtomicInteger();
        // Deliberately more failures than the old fixed policy's budget of three attempts. Anything
        // less and this test passes against that policy too, proving nothing: the record would have
        // recovered inside the old retry window rather than because retries are now unbounded.
        willAnswer(invocation -> {
            if (attempts.incrementAndGet() <= FAILURES_EXCEEDING_A_BOUNDED_POLICY) {
                throw new RecoverableDataAccessException("connection reset by peer");
            }
            return 1;
        }).given(metrics).save(any());

        send(valid(marker, 7.7));

        await().atMost(TIMEOUT)
                .untilAsserted(() -> verify(metrics, atLeast(FAILURES_EXCEEDING_A_BOUNDED_POLICY + 1)).save(any()));
        assertThat(pollDlq(marker, Duration.ofSeconds(5)))
                .as("valid telemetry must never be dead-lettered because the database was briefly down")
                .isNull();
    }

    /**
     * The counterpart: a row the database will never accept is not worth retrying forever, or the
     * partition stalls behind it indefinitely.
     */
    @Test
    @DisplayName("an unsatisfiable write dead-letters instead of retrying forever")
    void dataIntegrityViolationIsDeadLettered() {
        String marker = marker();
        willThrow(new DataIntegrityViolationException("value out of range"))
                .given(metrics).save(any());

        String payload = valid(marker, 1.0);
        send(payload);

        assertThat(awaitDlqRecord(marker)).isEqualTo(payload);
        verify(metrics, times(1)).save(any());
    }

    // ------------------------------------------------------------------ helpers

    private static String marker() {
        return "s-" + UUID.randomUUID();
    }

    private static String valid(String sensorId, double value) {
        return """
                {"timestamp":"%s","sensor_id":"%s","metric_type":"pipe_pressure","value":%s,"status":"OK"}"""
                .formatted(Instant.now(), sensorId, value);
    }

    private void send(String payload) {
        producer.send(new ProducerRecord<>(KafkaConfig.TOPIC, payload));
        producer.flush();
    }

    /** Fails the test if no DLQ record carrying {@code marker} arrives within {@link #TIMEOUT}. */
    private String awaitDlqRecord(String marker) {
        String found = pollDlq(marker, TIMEOUT);
        assertThat(found)
                .as("expected a record containing '%s' on %s", marker, KafkaConfig.DLQ_TOPIC)
                .isNotNull();
        return found;
    }

    /** @return the matching DLQ payload, or {@code null} if none arrives before the deadline. */
    private String pollDlq(String marker, Duration timeout) {
        List<String> seen = new ArrayList<>();
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            for (ConsumerRecord<String, String> record : dlqConsumer.poll(Duration.ofMillis(500))) {
                seen.add(record.value());
                if (record.value() != null && record.value().contains(marker)) {
                    return record.value();
                }
            }
        }
        return null;
    }
}
