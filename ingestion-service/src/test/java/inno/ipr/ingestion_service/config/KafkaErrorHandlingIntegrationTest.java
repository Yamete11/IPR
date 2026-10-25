package inno.ipr.ingestion_service.config;

import inno.ipr.ingestion_service.repository.MetricRepository;
import inno.ipr.ingestion_service.telemetry.TelemetryMessage;
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
import org.mockito.ArgumentCaptor;
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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Verifies the failure classification in {@link KafkaConfig} — and the batch listener's use of
 * {@code BatchListenerFailedException} to keep it per-record — against a real (in-JVM) broker.
 *
 * <p>The distinction under test is the one a single uniform retry policy gets backwards in both
 * directions: bad data must dead-letter immediately rather than stall its partition, while a
 * transient database failure must be retried rather than dropped into a topic nothing consumes.
 * Batching adds a third obligation: a poison record must not drag the valid records polled with it
 * into the DLQ, and must not cause them to be written twice. All three are behavioural — they live
 * in the {@code DefaultErrorHandler} wiring and the listener's exception contract, not in any unit
 * that can be called directly — so this drives them through the broker end to end.
 *
 * <p>The datasource is excluded and {@link MetricRepository} mocked: the point here is the error
 * handler's reaction to a repository outcome, and injecting the outcome directly is both faster and
 * far more precise than provoking a real database into failing on cue. With no datasource there is
 * no {@code db} health contributor for the readiness group to include, hence the
 * {@code readiness.include=readinessState} override. {@link TelemetryParser} is spied rather than
 * mocked, purely to count delivery attempts: a poison record never reaches the repository, so the
 * parser is the only place a redelivery is observable.
 *
 * <p>{@code TIMEOUT} is generous relative to the 1s/2s/4s… back-off, so a slow CI box does not turn
 * into a failure. {@code FAILURES_EXCEEDING_A_BOUNDED_POLICY} is four consecutive failures — one
 * more than the initial attempt plus two retries.
 *
 * <p>{@link #setUp()} stubs {@code saveAll} to report one stored row per message: a mock returns
 * {@code null} for {@code int[]}, which the consumer treats as "nothing to report", but the honest
 * default for a healthy repository is one stored row per message. Each test reads the DLQ with a
 * fresh consumer group from the beginning; the DLQ accumulates across tests, so assertions match on
 * a per-test marker rather than on the topic being empty.
 *
 * <ul>
 *   <li>{@link #invalidTelemetryIsNotRetried()}: the payload omits {@code value} — well-formed JSON
 *       that must not be ingested as 0.0. The actual no-retry claim is the single parser call: one
 *       delivery, straight to the DLQ. Under a policy that retries everything this is 3, and the
 *       partition sits blocked for the whole back-off first.</li>
 *   <li>{@link #poisonRecordInsideABatchDeadLettersAloneAndNeighboursPersistOnce()}: the
 *       batch-specific obligation. Three records are sent with one flush, so they land in the same
 *       poll whenever the broker allows it; the middle one is poison. The listener must persist the
 *       first, dead-letter only the second, and let the third be redelivered. Neither neighbour may
 *       be dead-lettered, and each must be handed to the repository exactly once across every batch
 *       it saw — the first is not written again when the redelivery happens.</li>
 *   <li>{@link #transientDatabaseFailureIsRetriedNotDeadLettered()}: the half that used to lose
 *       data — a database blip made {@code save} throw, the record exhausted its two retries within
 *       ~3 seconds and was dead-lettered, valid telemetry gone from the hypertable for good, since
 *       nothing consumes the DLQ. It injects deliberately more failures than the old fixed policy's
 *       budget of three attempts; anything less and the test passes against that policy too,
 *       proving nothing, because the record would have recovered inside the old retry window rather
 *       than because retries are now unbounded.</li>
 *   <li>{@link #dataIntegrityViolationIsDeadLettered()}: the counterpart — a row the database will
 *       never accept is not worth retrying forever, or the partition stalls behind it indefinitely.
 *       With batching the rejection first arrives for the whole batch; the listener must fall back
 *       to single-row inserts to pin the blame on one record. Expected: one batch attempt, one
 *       row-level attempt to locate the culprit, no retries of either.</li>
 * </ul>
 *
 * <p>{@link #awaitDlqRecord(String)} fails the test if no DLQ record carrying the marker arrives
 * within {@code TIMEOUT}; {@link #pollDlq(String, Duration)} returns the matching DLQ payload, or
 * {@code null} if none arrives before the deadline.
 */
@SpringBootTest(properties = {
        "spring.autoconfigure.exclude=org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration",
        "spring.sql.init.mode=never",
        "management.endpoint.health.group.readiness.include=readinessState",
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.auto-offset-reset=earliest",
})
@EmbeddedKafka(partitions = 1)
class KafkaErrorHandlingIntegrationTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(60);

    private static final int FAILURES_EXCEEDING_A_BOUNDED_POLICY = 4;

    @Autowired
    private EmbeddedKafkaBroker broker;

    @MockitoBean
    private MetricRepository metrics;

    @MockitoSpyBean
    private TelemetryParser parser;

    private Producer<String, String> producer;
    private Consumer<String, String> dlqConsumer;

    @BeforeEach
    void setUp() {
        given(metrics.saveAll(any())).willAnswer(invocation -> allStored(invocation.getArgument(0)));

        producer = new KafkaProducer<>(
                KafkaTestUtils.producerProps(broker), new StringSerializer(), new StringSerializer());
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
        verify(metrics, never()).saveAll(any());
    }

    @Test
    @DisplayName("a validation failure dead-letters on the first attempt, with no retry")
    void invalidTelemetryIsNotRetried() {
        String marker = marker();
        String payload = """
                {"timestamp":"%s","sensor_id":"%s","metric_type":"substation_voltage","status":"OK"}"""
                .formatted(Instant.now(), marker);

        send(payload);

        assertThat(awaitDlqRecord(marker)).isEqualTo(payload);
        verify(metrics, never()).save(any());
        verify(metrics, never()).saveAll(any());
        verify(parser, times(1)).parse(payload);
    }

    @Test
    @DisplayName("a valid reading is persisted in a batch and never dead-lettered")
    void validRecordIsPersisted() {
        String marker = marker();

        send(valid(marker, 4.25));

        await().atMost(TIMEOUT).untilAsserted(() -> verify(metrics).saveAll(argThat(batchContaining(marker))));
        assertThat(pollDlq(marker, Duration.ofSeconds(5))).isNull();
    }

    @Test
    @DisplayName("a poison record inside a batch dead-letters alone; its neighbours persist exactly once")
    void poisonRecordInsideABatchDeadLettersAloneAndNeighboursPersistOnce() {
        String before = marker();
        String poisonMarker = marker();
        String after = marker();
        String poison = "{\"sensor_id\":\"" + poisonMarker + "\"";

        producer.send(new ProducerRecord<>(KafkaConfig.TOPIC, valid(before, 1.0)));
        producer.send(new ProducerRecord<>(KafkaConfig.TOPIC, poison));
        producer.send(new ProducerRecord<>(KafkaConfig.TOPIC, valid(after, 2.0)));
        producer.flush();

        assertThat(awaitDlqRecord(poisonMarker)).isEqualTo(poison);
        await().atMost(TIMEOUT).untilAsserted(() -> {
            verify(metrics, atLeastOnce()).saveAll(argThat(batchContaining(before)));
            verify(metrics, atLeastOnce()).saveAll(argThat(batchContaining(after)));
        });

        assertThat(pollDlq(before, Duration.ofSeconds(3))).isNull();
        assertThat(pollDlq(after, Duration.ofSeconds(3))).isNull();
        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<TelemetryMessage>> batches = ArgumentCaptor.forClass(List.class);
        verify(metrics, atLeastOnce()).saveAll(batches.capture());
        List<String> persisted = batches.getAllValues().stream()
                .flatMap(List::stream)
                .map(TelemetryMessage::sensorId)
                .filter(id -> id.equals(before) || id.equals(after))
                .toList();
        assertThat(persisted).containsExactlyInAnyOrder(before, after);
        verify(metrics, never()).save(any());
    }

    @Test
    @DisplayName("a transient database failure is retried until it succeeds, not dead-lettered")
    void transientDatabaseFailureIsRetriedNotDeadLettered() {
        String marker = marker();
        AtomicInteger attempts = new AtomicInteger();
        willAnswer(invocation -> {
            if (attempts.incrementAndGet() <= FAILURES_EXCEEDING_A_BOUNDED_POLICY) {
                throw new RecoverableDataAccessException("connection reset by peer");
            }
            return allStored(invocation.getArgument(0));
        }).given(metrics).saveAll(any());

        send(valid(marker, 7.7));

        await().atMost(TIMEOUT)
                .untilAsserted(() -> verify(metrics, atLeast(FAILURES_EXCEEDING_A_BOUNDED_POLICY + 1)).saveAll(any()));
        assertThat(pollDlq(marker, Duration.ofSeconds(5)))
                .as("valid telemetry must never be dead-lettered because the database was briefly down")
                .isNull();
    }

    @Test
    @DisplayName("an unsatisfiable write dead-letters the offending row instead of retrying forever")
    void dataIntegrityViolationIsDeadLettered() {
        String marker = marker();
        willThrow(new DataIntegrityViolationException("value out of range")).given(metrics).saveAll(any());
        willThrow(new DataIntegrityViolationException("value out of range")).given(metrics).save(any());

        String payload = valid(marker, 1.0);
        send(payload);

        assertThat(awaitDlqRecord(marker)).isEqualTo(payload);
        verify(metrics, times(1)).saveAll(any());
        verify(metrics, times(1)).save(any());
    }

    private static int[] allStored(List<TelemetryMessage> batch) {
        int[] counts = new int[batch.size()];
        Arrays.fill(counts, 1);
        return counts;
    }

    private static org.mockito.ArgumentMatcher<List<TelemetryMessage>> batchContaining(String sensorId) {
        return batch -> batch != null && batch.stream().anyMatch(m -> sensorId.equals(m.sensorId()));
    }

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

    private String awaitDlqRecord(String marker) {
        String found = pollDlq(marker, TIMEOUT);
        assertThat(found)
                .as("expected a record containing '%s' on %s", marker, KafkaConfig.DLQ_TOPIC)
                .isNotNull();
        return found;
    }

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
