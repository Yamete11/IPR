package inno.ipr.ai_incident_responder_service.consumer;

import inno.ipr.ai_incident_responder_service.ai.IncidentPlan;
import inno.ipr.ai_incident_responder_service.ai.IncidentPlanner;
import inno.ipr.ai_incident_responder_service.config.KafkaConfig;
import inno.ipr.ai_incident_responder_service.correlation.IncidentCorrelator;
import inno.ipr.ai_incident_responder_service.domain.GraphContext;
import inno.ipr.ai_incident_responder_service.domain.ImpactedObject;
import inno.ipr.ai_incident_responder_service.domain.Incident;
import inno.ipr.ai_incident_responder_service.domain.MetricSummary;
import inno.ipr.ai_incident_responder_service.repository.GraphRepository;
import inno.ipr.ai_incident_responder_service.repository.IncidentRepository;
import inno.ipr.ai_incident_responder_service.repository.MetricHistoryRepository;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
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
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * The alert pipeline through a real (in-JVM) broker with the databases and the planner mocked:
 * an alert becomes a persisted incident carrying the graph-derived context; poison dead-letters
 * without a plan; a redelivered alert never reaches the planner; and an open circuit breaker is
 * retried, not dead-lettered — the property that keeps a Claude outage from losing incidents.
 *
 * <p>Mock mode is on so no {@code AnthropicClient} is created; the {@link IncidentPlanner} is then
 * replaced by a Mockito bean so each test can script the planner's behaviour. The two Hikari pools
 * are declared but never opened — nothing here touches a database — so the datasource is kept out
 * of the readiness group.
 *
 * <ul>
 *   <li>{@code duplicateSkipsThePlanner}: after the idempotency check, the test gives the consumer
 *       a moment to (wrongly) proceed, then proves it did not.</li>
 *   <li>{@code openBreakerIsRetriedNotDeadLettered}: the circuit-breaker contract end to end —
 *       while the breaker is open the alert is retried under back-off rather than dead-lettered,
 *       and it is persisted once the breaker admits calls.</li>
 *   <li>{@code pollDlq(String, Duration)} returns the matching DLQ payload, or {@code null} if none
 *       arrives before the deadline.</li>
 * </ul>
 */
@SpringBootTest(properties = {
        "ai.claude.mock=true",
        "spring.sql.init.mode=never",
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "management.endpoint.health.group.readiness.include=readinessState",
})
@EmbeddedKafka(partitions = 1)
class AlertPipelineIntegrationTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(60);

    @Autowired
    private EmbeddedKafkaBroker broker;

    @Autowired
    private CircuitBreaker claudeCircuitBreaker;

    @MockitoBean
    private IncidentRepository incidents;

    @MockitoBean
    private MetricHistoryRepository metrics;

    @MockitoBean
    private GraphRepository graph;

    @MockitoBean
    private IncidentPlanner planner;

    @MockitoBean
    private IncidentCorrelator correlator;

    private Producer<String, String> producer;
    private Consumer<String, String> dlqConsumer;

    @BeforeEach
    void setUp() {
        given(metrics.findLastHour(anyString(), anyString(), any()))
                .willReturn(new MetricSummary(60, 0.0, 4.2, 3.0, 0.0, Instant.now()));
        ImpactedObject node = new ImpactedObject("node-5", "DistributionNode", Map.of("name", "North"));
        given(graph.loadContext(anyString(), anyBoolean())).willReturn(new GraphContext(node, List.of(node,
                new ImpactedObject("bld-12", "ResidentialBuilding", Map.of()),
                new ImpactedObject("bld-19", "ResidentialBuilding", Map.of())), List.of(), List.of()));
        given(correlator.findRoot(any(), any())).willReturn(Optional.empty());
        given(correlator.persist(any(), any())).willReturn(new IncidentCorrelator.Outcome(true, null, 0));
        given(planner.model()).willReturn("mock");
        given(planner.plan(any())).willReturn(new IncidentPlan("HIGH", "Zero pressure at node-5",
                List.of("DistributionNode node-5", "ResidentialBuilding bld-12", "ResidentialBuilding bld-19"),
                List.of("Dispatch a crew.", "Confirm restoration.")));

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
    @DisplayName("a valid alert becomes a persisted incident carrying the graph context and the model")
    void alertBecomesIncident() {
        String alertId = alertId();

        send(alert(alertId, "s-00042"));

        ArgumentCaptor<Incident> saved = ArgumentCaptor.forClass(Incident.class);
        await().atMost(TIMEOUT).untilAsserted(() -> verify(correlator).persist(saved.capture(), any()));
        Incident incident = saved.getValue();
        assertThat(incident.alertId()).isEqualTo(alertId);
        assertThat(incident.sensorId()).isEqualTo("s-00042");
        assertThat(incident.criticality()).isEqualTo("HIGH");
        assertThat(incident.model()).isEqualTo("mock");
        assertThat(incident.impactedObjectsJson()).contains("node-5", "bld-12", "bld-19");
        verify(graph).loadContext(eq("s-00042"), anyBoolean());
        verify(metrics).findLastHour(eq("s-00042"), eq("pipe_pressure"), any());
        assertThat(pollDlq(alertId, Duration.ofSeconds(3))).isNull();
    }

    @Test
    @DisplayName("a malformed alert dead-letters immediately and never reaches the planner")
    void poisonIsDeadLettered() {
        String marker = alertId();
        String poison = "{\"alert_id\":\"" + marker + "\"";

        send(poison);

        assertThat(pollDlq(marker, TIMEOUT)).isEqualTo(poison);
        verify(planner, never()).plan(any());
        verify(correlator, never()).persist(any(), any());
    }

    @Test
    @DisplayName("an alert missing its sensor dead-letters as poison too")
    void missingSensorIsDeadLettered() {
        String marker = alertId();
        String payload = "{\"alert_id\":\"" + marker + "\",\"metric_type\":\"pipe_pressure\",\"value\":0}";

        send(payload);

        assertThat(pollDlq(marker, TIMEOUT)).isEqualTo(payload);
        verify(planner, never()).plan(any());
    }

    @Test
    @DisplayName("a redelivered alert is recognised before the planner is called")
    void duplicateSkipsThePlanner() {
        String alertId = alertId();
        given(incidents.existsByAlertId(alertId)).willReturn(true);

        send(alert(alertId, "s-00042"));

        await().atMost(TIMEOUT).untilAsserted(() -> verify(incidents).existsByAlertId(alertId));
        assertThat(pollDlq(alertId, Duration.ofSeconds(2))).isNull();
        verify(planner, never()).plan(any());
        verify(correlator, never()).persist(any(), any());
    }

    @Test
    @DisplayName("an open circuit breaker is retried until it closes, never dead-lettered")
    void openBreakerIsRetriedNotDeadLettered() {
        String alertId = alertId();
        AtomicInteger attempts = new AtomicInteger();
        willAnswer(invocation -> {
            if (attempts.incrementAndGet() <= 2) {
                throw CallNotPermittedException.createCallNotPermittedException(claudeCircuitBreaker);
            }
            return new IncidentPlan("MEDIUM", "recovered", List.of("node-5"), List.of("Inspect."));
        }).given(planner).plan(any());

        send(alert(alertId, "s-00108"));

        await().atMost(TIMEOUT).untilAsserted(() -> verify(planner, atLeast(3)).plan(any()));
        ArgumentCaptor<Incident> saved = ArgumentCaptor.forClass(Incident.class);
        await().atMost(TIMEOUT).untilAsserted(() -> verify(correlator).persist(saved.capture(), any()));
        assertThat(saved.getValue().alertId()).isEqualTo(alertId);
        assertThat(saved.getValue().criticality()).isEqualTo("MEDIUM");
        assertThat(pollDlq(alertId, Duration.ofSeconds(3)))
                .as("a valid alert must never be dead-lettered because Claude was briefly unavailable")
                .isNull();
    }

    private static String alertId() {
        return "a-" + UUID.randomUUID();
    }

    private static String alert(String alertId, String sensorId) {
        return """
                {"alert_id":"%s","sensor_id":"%s","metric_type":"pipe_pressure","value":0.0,"detected_at":"%s","rule":"pipe_pressure_min<0.5"}"""
                .formatted(alertId, sensorId, Instant.now());
    }

    private void send(String payload) {
        producer.send(new ProducerRecord<>(KafkaConfig.ALERTS_TOPIC, payload));
        producer.flush();
    }

    private String pollDlq(String marker, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            for (ConsumerRecord<String, String> record : dlqConsumer.poll(Duration.ofMillis(500))) {
                if (record.value() != null && record.value().contains(marker)) {
                    return record.value();
                }
            }
        }
        return null;
    }
}
