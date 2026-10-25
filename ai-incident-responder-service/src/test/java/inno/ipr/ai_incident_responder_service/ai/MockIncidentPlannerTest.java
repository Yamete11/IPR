package inno.ipr.ai_incident_responder_service.ai;

import inno.ipr.ai_incident_responder_service.alert.Alert;
import inno.ipr.ai_incident_responder_service.domain.ImpactedObject;
import inno.ipr.ai_incident_responder_service.domain.MetricSummary;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** The offline planner must produce a plan of the same shape the real one would, deterministically. */
class MockIncidentPlannerTest {

    private static final Alert ALERT = new Alert("a-1001", "s-00042", "pipe_pressure", 0.0,
            Instant.parse("2026-09-06T10:05:00Z"), "pipe_pressure_min<0.5");
    private static final MetricSummary NO_HISTORY = new MetricSummary(0, null, null, null, null, null);

    private final MockIncidentPlanner planner = new MockIncidentPlanner();

    @Test
    @DisplayName("records itself as the 'mock' model for provenance")
    void modelIsMock() {
        assertThat(planner.model()).isEqualTo("mock");
    }

    @Test
    @DisplayName("lists the graph-derived impacted objects and targets the first one")
    void usesImpactedObjects() {
        IncidentPlan plan = planner.plan(ALERT, NO_HISTORY, List.of(
                new ImpactedObject("node-5", "DistributionNode", Map.of()),
                new ImpactedObject("bld-12", "ResidentialBuilding", Map.of())));

        assertThat(plan.criticality()).isEqualTo("HIGH");
        assertThat(plan.impactedObjects()).containsExactly("DistributionNode node-5", "ResidentialBuilding bld-12");
        assertThat(plan.remediationSteps()).hasSize(3);
        assertThat(plan.remediationSteps().get(0)).contains("DistributionNode node-5");
        assertThat(plan.remediationSteps().get(1)).contains("pipe_pressure").contains("s-00042").contains("pipe_pressure_min<0.5");
        assertThat(plan.summary()).contains("pipe_pressure").contains("s-00042").contains("0.0");
    }

    @Test
    @DisplayName("falls back to the sensor itself when the graph resolves nothing")
    void fallsBackToSensor() {
        assertThat(planner.plan(ALERT, NO_HISTORY, List.of()).impactedObjects()).containsExactly("s-00042");
    }

    @Test
    @DisplayName("is deterministic for the same input")
    void deterministic() {
        List<ImpactedObject> impacted = List.of(new ImpactedObject("node-5", "DistributionNode", Map.of()));
        assertThat(planner.plan(ALERT, NO_HISTORY, impacted)).isEqualTo(planner.plan(ALERT, NO_HISTORY, impacted));
    }
}
