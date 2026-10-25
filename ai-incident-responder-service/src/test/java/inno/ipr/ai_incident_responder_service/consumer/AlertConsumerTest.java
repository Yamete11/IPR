package inno.ipr.ai_incident_responder_service.consumer;

import inno.ipr.ai_incident_responder_service.ai.IncidentPlan;
import inno.ipr.ai_incident_responder_service.ai.IncidentPlanner;
import inno.ipr.ai_incident_responder_service.domain.ImpactedObject;
import inno.ipr.ai_incident_responder_service.domain.Incident;
import inno.ipr.ai_incident_responder_service.domain.MetricSummary;
import inno.ipr.ai_incident_responder_service.exception.InvalidAlertException;
import inno.ipr.ai_incident_responder_service.repository.GraphRepository;
import inno.ipr.ai_incident_responder_service.repository.IncidentRepository;
import inno.ipr.ai_incident_responder_service.repository.MetricHistoryRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

/** The per-alert pipeline in isolation: parse → dedup → context → plan → persist. */
class AlertConsumerTest {

    private static final String VALID = """
            {"alert_id":"a-1001","sensor_id":"s-00042","metric_type":"pipe_pressure","value":0.0,
             "detected_at":"2026-09-06T10:05:00Z","rule":"pipe_pressure_min<0.5"}""";

    private final IncidentRepository incidents = mock(IncidentRepository.class);
    private final MetricHistoryRepository metrics = mock(MetricHistoryRepository.class);
    private final GraphRepository graph = mock(GraphRepository.class);
    private final IncidentPlanner planner = mock(IncidentPlanner.class);
    private AlertConsumer consumer;

    @BeforeEach
    void setUp() {
        consumer = new AlertConsumer(JsonMapper.builder().build(), incidents, metrics, graph, planner);
        given(metrics.findLastHour(anyString(), anyString(), any()))
                .willReturn(new MetricSummary(60, 0.0, 4.2, 3.0, 0.0, Instant.parse("2026-09-06T10:04:55Z")));
        given(graph.findImpacted("s-00042")).willReturn(List.of(
                new ImpactedObject("node-5", "DistributionNode", Map.of("name", "North")),
                new ImpactedObject("bld-12", "ResidentialBuilding", Map.of())));
        given(planner.model()).willReturn("test-model");
        given(planner.plan(any(), any(), any())).willReturn(new IncidentPlan("HIGH", "Zero pressure at node-5",
                List.of("DistributionNode node-5", "ResidentialBuilding bld-12"),
                List.of("Dispatch a crew.", "Confirm restoration.")));
        given(incidents.save(any())).willReturn(1);
    }

    @Test
    @DisplayName("a new alert is enriched, planned and persisted with the plan serialised to JSON arrays")
    void happyPath() {
        consumer.onMessage(VALID);

        verify(metrics).findLastHour("s-00042", "pipe_pressure", Instant.parse("2026-09-06T10:05:00Z"));
        verify(graph).findImpacted("s-00042");
        ArgumentCaptor<Incident> saved = ArgumentCaptor.forClass(Incident.class);
        verify(incidents).save(saved.capture());
        Incident incident = saved.getValue();
        assertThat(incident.incidentId()).startsWith("i-");
        assertThat(incident.alertId()).isEqualTo("a-1001");
        assertThat(incident.sensorId()).isEqualTo("s-00042");
        assertThat(incident.metricType()).isEqualTo("pipe_pressure");
        assertThat(incident.criticality()).isEqualTo("HIGH");
        assertThat(incident.impactedObjectsJson()).isEqualTo("[\"DistributionNode node-5\",\"ResidentialBuilding bld-12\"]");
        assertThat(incident.remediationStepsJson()).isEqualTo("[\"Dispatch a crew.\",\"Confirm restoration.\"]");
        assertThat(incident.summary()).isEqualTo("Zero pressure at node-5");
        assertThat(incident.model()).isEqualTo("test-model");
    }

    @Test
    @DisplayName("an alert that already has an incident is skipped before any context or planner call")
    void duplicateIsSkippedBeforeThePlanner() {
        given(incidents.existsByAlertId("a-1001")).willReturn(true);

        consumer.onMessage(VALID);

        verifyNoInteractions(metrics, graph, planner);
        verify(incidents, never()).save(any());
    }

    @Test
    @DisplayName("a concurrent duplicate that loses the ON CONFLICT race is reported, not thrown")
    void lostInsertRaceIsNotAnError() {
        given(incidents.save(any())).willReturn(0);

        assertThatCode(() -> consumer.onMessage(VALID)).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("null plan lists are persisted as empty JSON arrays")
    void nullListsBecomeEmptyArrays() {
        given(planner.plan(any(), any(), any())).willReturn(new IncidentPlan("LOW", "s", null, null));

        consumer.onMessage(VALID);

        ArgumentCaptor<Incident> saved = ArgumentCaptor.forClass(Incident.class);
        verify(incidents).save(saved.capture());
        assertThat(saved.getValue().impactedObjectsJson()).isEqualTo("[]");
        assertThat(saved.getValue().remediationStepsJson()).isEqualTo("[]");
    }

    @Test
    @DisplayName("malformed JSON is poison")
    void malformedJsonIsPoison() {
        assertThatThrownBy(() -> consumer.onMessage("{\"alert_id\":\"a-1\""))
                .isInstanceOf(InvalidAlertException.class)
                .hasMessageContaining("Malformed alert JSON");
        verifyNoInteractions(incidents, planner);
    }

    @Test
    @DisplayName("a well-formed alert missing its identity fields is poison")
    void missingIdentityIsPoison() {
        String noAlertId = "{\"sensor_id\":\"s-00042\",\"metric_type\":\"pipe_pressure\",\"value\":0.0,"
                + "\"detected_at\":\"2026-09-06T10:05:00Z\",\"rule\":\"r\"}";
        String blankSensor = "{\"alert_id\":\"a-1\",\"sensor_id\":\" \",\"metric_type\":\"pipe_pressure\",\"value\":0.0,"
                + "\"detected_at\":\"2026-09-06T10:05:00Z\",\"rule\":\"r\"}";

        assertThatThrownBy(() -> consumer.onMessage(noAlertId))
                .isInstanceOf(InvalidAlertException.class)
                .hasMessageContaining("alert_id");
        assertThatThrownBy(() -> consumer.onMessage(blankSensor))
                .isInstanceOf(InvalidAlertException.class)
                .hasMessageContaining("sensor_id");
        verifyNoInteractions(incidents, planner);
    }

    @Test
    @DisplayName("an alert without detected_at is poison: it anchors the metric-history window")
    void missingDetectedAtIsPoison() {
        String noDetectedAt = "{\"alert_id\":\"a-1\",\"sensor_id\":\"s-00042\",\"metric_type\":\"pipe_pressure\","
                + "\"value\":0.0,\"rule\":\"r\"}";

        assertThatThrownBy(() -> consumer.onMessage(noDetectedAt))
                .isInstanceOf(InvalidAlertException.class)
                .hasMessageContaining("detected_at");
        verifyNoInteractions(incidents, planner);
    }

    @Test
    @DisplayName("a missing numeric value is poison too: Jackson 3 refuses to fabricate a 0.0 reading")
    void missingValueIsPoison() {
        assertThatThrownBy(() -> consumer.onMessage("{\"alert_id\":\"a-1\",\"sensor_id\":\"s-00042\"}"))
                .isInstanceOf(InvalidAlertException.class)
                .hasMessageContaining("Malformed alert JSON");
        verifyNoInteractions(incidents, planner);
    }

    @Test
    @DisplayName("a planner failure propagates unchanged so the container retries rather than dead-letters")
    void plannerFailurePropagates() {
        given(planner.plan(any(), any(), any())).willThrow(new IllegalStateException("Claude unavailable"));

        assertThatThrownBy(() -> consumer.onMessage(VALID))
                .isInstanceOf(IllegalStateException.class)
                .isNotInstanceOf(InvalidAlertException.class);
        verify(incidents, never()).save(any());
    }
}
