package inno.ipr.ai_incident_responder_service.consumer;

import inno.ipr.ai_incident_responder_service.ai.IncidentContext;
import inno.ipr.ai_incident_responder_service.ai.IncidentPlan;
import inno.ipr.ai_incident_responder_service.ai.IncidentPlanner;
import inno.ipr.ai_incident_responder_service.correlation.IncidentCorrelator;
import inno.ipr.ai_incident_responder_service.domain.Crew;
import inno.ipr.ai_incident_responder_service.domain.GraphContext;
import inno.ipr.ai_incident_responder_service.domain.ImpactedObject;
import inno.ipr.ai_incident_responder_service.domain.Incident;
import inno.ipr.ai_incident_responder_service.domain.MetricSummary;
import inno.ipr.ai_incident_responder_service.exception.InvalidAlertException;
import inno.ipr.ai_incident_responder_service.repository.GraphRepository;
import inno.ipr.ai_incident_responder_service.repository.IncidentRepository;
import inno.ipr.ai_incident_responder_service.repository.IncidentRepository.RootIncident;
import inno.ipr.ai_incident_responder_service.repository.MetricHistoryRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

/**
 * The per-alert pipeline in isolation: parse → dedup → context → root-cause check → plan or
 * correlated child → persist. The graph, repositories, planner and correlator are mocks; the
 * correlation SQL itself is covered by {@code IncidentRepositoryIntegrationTest} and
 * {@code CorrelationIntegrationTest}.
 */
class AlertConsumerTest {

    private static final String VALID = """
            {"alert_id":"a-1001","sensor_id":"s-00042","metric_type":"pipe_pressure","value":0.0,
             "detected_at":"2026-09-06T10:05:00Z","rule":"pipe_pressure_min<0.5"}""";
    private static final Instant DETECTED = Instant.parse("2026-09-06T10:05:00Z");
    private static final ImpactedObject NODE = new ImpactedObject("node-5", "DistributionNode", Map.of("name", "North"));
    private static final Crew GRID = new Crew("crew-grid", "Distribution grid crew", "+7-900-000-0102", List.of("node-5"));
    private static final GraphContext CONTEXT = new GraphContext(NODE,
            List.of(NODE, new ImpactedObject("bld-12", "ResidentialBuilding", Map.of())),
            List.of(GRID), List.of("sub-1", "node-5"));

    private final IncidentRepository incidents = mock(IncidentRepository.class);
    private final MetricHistoryRepository metrics = mock(MetricHistoryRepository.class);
    private final GraphRepository graph = mock(GraphRepository.class);
    private final IncidentPlanner planner = mock(IncidentPlanner.class);
    private final IncidentCorrelator correlator = mock(IncidentCorrelator.class);
    private AlertConsumer consumer;

    @BeforeEach
    void setUp() {
        consumer = new AlertConsumer(JsonMapper.builder().build(), incidents, metrics, graph, planner, correlator);
        given(metrics.findLastHour(anyString(), anyString(), any()))
                .willReturn(new MetricSummary(60, 0.0, 4.2, 3.0, 0.0, Instant.parse("2026-09-06T10:04:55Z")));
        given(correlator.enabled()).willReturn(true);
        given(graph.loadContext("s-00042", true)).willReturn(CONTEXT);
        given(correlator.findRoot(any(), any())).willReturn(Optional.empty());
        given(correlator.persist(any(), any())).willReturn(new IncidentCorrelator.Outcome(true, null, 0));
        given(planner.model()).willReturn("test-model");
        given(planner.plan(any())).willReturn(new IncidentPlan("HIGH", "Zero pressure at node-5",
                List.of("DistributionNode node-5", "ResidentialBuilding bld-12"),
                List.of("Dispatch a crew.", "Confirm restoration.")));
    }

    @Test
    @DisplayName("a new alert is enriched, planned and persisted with the plan and the graph's crews as JSON")
    void happyPath() {
        consumer.onMessage(VALID);

        verify(metrics).findLastHour("s-00042", "pipe_pressure", DETECTED);
        verify(graph).loadContext("s-00042", true);
        ArgumentCaptor<IncidentContext> context = ArgumentCaptor.forClass(IncidentContext.class);
        verify(planner).plan(context.capture());
        assertThat(context.getValue().impacted()).isEqualTo(CONTEXT.impacted());
        assertThat(context.getValue().crews()).containsExactly(GRID);
        Incident incident = persisted();
        assertThat(incident.incidentId()).startsWith("i-");
        assertThat(incident.alertId()).isEqualTo("a-1001");
        assertThat(incident.sensorId()).isEqualTo("s-00042");
        assertThat(incident.metricType()).isEqualTo("pipe_pressure");
        assertThat(incident.assetId()).isEqualTo("node-5");
        assertThat(incident.rootIncidentId()).isNull();
        assertThat(incident.detectedAt()).isEqualTo(DETECTED);
        assertThat(incident.criticality()).isEqualTo("HIGH");
        assertThat(incident.impactedObjectsJson()).isEqualTo("[\"DistributionNode node-5\",\"ResidentialBuilding bld-12\"]");
        assertThat(incident.remediationStepsJson()).isEqualTo("[\"Dispatch a crew.\",\"Confirm restoration.\"]");
        assertThat(incident.crewsJson()).isEqualTo(
                "[{\"crew_id\":\"crew-grid\",\"name\":\"Distribution grid crew\",\"phone\":\"+7-900-000-0102\","
                        + "\"assets\":[\"node-5\"]}]");
        assertThat(incident.summary()).isEqualTo("Zero pressure at node-5");
        assertThat(incident.model()).isEqualTo("test-model");
    }

    @Test
    @DisplayName("an alert under an active upstream incident becomes its child without calling the planner")
    void secondaryAlertSkipsThePlanner() {
        given(correlator.findRoot(CONTEXT, DETECTED))
                .willReturn(Optional.of(new RootIncident("i-root", "MEDIUM", "sub-1")));
        given(correlator.persist(any(), any())).willReturn(new IncidentCorrelator.Outcome(true, "i-root", 0));

        consumer.onMessage(VALID);

        verify(planner, never()).plan(any());
        Incident incident = persisted();
        assertThat(incident.rootIncidentId()).isEqualTo("i-root");
        assertThat(incident.criticality()).isEqualTo("MEDIUM");
        assertThat(incident.model()).isEqualTo("correlated");
        assertThat(incident.assetId()).isEqualTo("node-5");
        assertThat(incident.summary()).startsWith("Secondary effect of incident i-root on sub-1");
        assertThat(incident.remediationStepsJson()).contains("Handled under root incident i-root");
        assertThat(incident.impactedObjectsJson()).isEqualTo("[\"DistributionNode node-5\",\"ResidentialBuilding bld-12\"]");
        assertThat(incident.crewsJson()).contains("crew-grid");
    }

    @Test
    @DisplayName("with correlation off the upstream walk is skipped and every alert is planned")
    void correlationDisabled() {
        given(correlator.enabled()).willReturn(false);
        given(graph.loadContext("s-00042", false)).willReturn(CONTEXT);

        consumer.onMessage(VALID);

        verify(graph).loadContext("s-00042", false);
        verify(planner).plan(any());
    }

    @Test
    @DisplayName("a sensor that is not in the graph is planned with no asset and no crews")
    void unlinkedSensor() {
        given(graph.loadContext(anyString(), anyBoolean())).willReturn(GraphContext.none());

        consumer.onMessage(VALID);

        Incident incident = persisted();
        assertThat(incident.assetId()).isNull();
        assertThat(incident.crewsJson()).isEqualTo("[]");
    }

    @Test
    @DisplayName("an alert that already has an incident is skipped before any context or planner call")
    void duplicateIsSkippedBeforeThePlanner() {
        given(incidents.existsByAlertId("a-1001")).willReturn(true);

        consumer.onMessage(VALID);

        verifyNoInteractions(metrics, graph, planner);
        verify(correlator, never()).persist(any(), any());
    }

    @Test
    @DisplayName("a concurrent duplicate that loses the ON CONFLICT race is reported, not thrown")
    void lostInsertRaceIsNotAnError() {
        given(correlator.persist(any(), any())).willReturn(new IncidentCorrelator.Outcome(false, null, 0));

        assertThatCode(() -> consumer.onMessage(VALID)).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("null plan lists are persisted as empty JSON arrays")
    void nullListsBecomeEmptyArrays() {
        given(planner.plan(any())).willReturn(new IncidentPlan("LOW", "s", null, null));

        consumer.onMessage(VALID);

        Incident incident = persisted();
        assertThat(incident.impactedObjectsJson()).isEqualTo("[]");
        assertThat(incident.remediationStepsJson()).isEqualTo("[]");
    }

    @Test
    @DisplayName("malformed JSON is poison")
    void malformedJsonIsPoison() {
        assertThatThrownBy(() -> consumer.onMessage("{\"alert_id\":\"a-1\""))
                .isInstanceOf(InvalidAlertException.class)
                .hasMessageContaining("Malformed alert JSON");
        verifyNoInteractions(incidents, planner, correlator);
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
        verifyNoInteractions(incidents, planner, correlator);
    }

    @Test
    @DisplayName("an alert without detected_at is poison: it anchors the history and correlation windows")
    void missingDetectedAtIsPoison() {
        String noDetectedAt = "{\"alert_id\":\"a-1\",\"sensor_id\":\"s-00042\",\"metric_type\":\"pipe_pressure\","
                + "\"value\":0.0,\"rule\":\"r\"}";

        assertThatThrownBy(() -> consumer.onMessage(noDetectedAt))
                .isInstanceOf(InvalidAlertException.class)
                .hasMessageContaining("detected_at");
        verifyNoInteractions(incidents, planner, correlator);
    }

    @Test
    @DisplayName("a missing numeric value is poison too: Jackson 3 refuses to fabricate a 0.0 reading")
    void missingValueIsPoison() {
        assertThatThrownBy(() -> consumer.onMessage("{\"alert_id\":\"a-1\",\"sensor_id\":\"s-00042\"}"))
                .isInstanceOf(InvalidAlertException.class)
                .hasMessageContaining("Malformed alert JSON");
        verifyNoInteractions(incidents, planner, correlator);
    }

    @Test
    @DisplayName("a planner failure propagates unchanged so the container retries rather than dead-letters")
    void plannerFailurePropagates() {
        given(planner.plan(any())).willThrow(new IllegalStateException("Claude unavailable"));

        assertThatThrownBy(() -> consumer.onMessage(VALID))
                .isInstanceOf(IllegalStateException.class)
                .isNotInstanceOf(InvalidAlertException.class);
        verify(correlator, never()).persist(any(), any());
    }

    private Incident persisted() {
        ArgumentCaptor<Incident> saved = ArgumentCaptor.forClass(Incident.class);
        verify(correlator).persist(saved.capture(), any());
        return saved.getValue();
    }
}
