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

/** The prompt is the RAG contract with the model: every context section must land, verbatim. */
class IncidentPromptBuilderTest {

    private static final Alert ALERT = new Alert("a-1001", "s-00042", "pipe_pressure", 0.0,
            Instant.parse("2026-09-06T10:05:00Z"), "pipe_pressure_min<0.5");

    @Test
    @DisplayName("includes the alert, the hour summary and every impacted object with its name")
    void fullContext() {
        MetricSummary history = new MetricSummary(120, 0.0, 4.4, 3.1, 0.0, Instant.parse("2026-09-06T10:04:58Z"));
        List<ImpactedObject> impacted = List.of(
                new ImpactedObject("node-5", "DistributionNode", Map.of("name", "North Node")),
                new ImpactedObject("bld-12", "ResidentialBuilding", Map.of()),
                new ImpactedObject("bld-19", "ResidentialBuilding", null));

        String prompt = IncidentPromptBuilder.build(ALERT, history, impacted);

        assertThat(prompt)
                .startsWith("You are the incident-response coordinator")
                .contains("- sensor_id: s-00042")
                .contains("- metric_type: pipe_pressure")
                .contains("- value: 0.0")
                .contains("- rule: pipe_pressure_min<0.5")
                .contains("- detected_at: 2026-09-06T10:05:00Z")
                .contains("- count: 120").contains("- min: 0.0").contains("- max: 4.4").contains("- avg: 3.1")
                .contains("- latest: 0.0 (at 2026-09-06T10:04:58Z)")
                .contains("- DistributionNode node-5 (North Node)")
                .contains("- ResidentialBuilding bld-12\n")
                .contains("- ResidentialBuilding bld-19\n")
                .endsWith("give ordered, concrete remediation steps.");
    }

    @Test
    @DisplayName("says so explicitly when there is no history and nothing in the graph")
    void emptyContext() {
        String prompt = IncidentPromptBuilder.build(ALERT, new MetricSummary(0, null, null, null, null, null), List.of());

        assertThat(prompt)
                .contains("- no readings recorded in the last hour")
                .contains("- none resolved (sensor not linked in the topology graph)")
                .doesNotContain("- count:");
    }

    @Test
    @DisplayName("null history and null impacted list are treated as empty, not as errors")
    void nullsAreEmpty() {
        String prompt = IncidentPromptBuilder.build(ALERT, null, null);

        assertThat(prompt)
                .contains("- no readings recorded in the last hour")
                .contains("- none resolved");
    }
}
