package inno.ipr.ai_incident_responder_service.ai;

import inno.ipr.ai_incident_responder_service.alert.Alert;
import inno.ipr.ai_incident_responder_service.domain.Crew;
import inno.ipr.ai_incident_responder_service.domain.ImpactedObject;
import inno.ipr.ai_incident_responder_service.domain.MetricSummary;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/** The prompt is the RAG contract with the model: every context section must land, verbatim. */
class IncidentPromptBuilderTest {

    private static final Alert ALERT = new Alert("a-1001", "s-00042", "pipe_pressure", 0.0,
            Instant.parse("2026-09-06T10:05:00Z"), "pipe_pressure_min<0.5");
    private static final MetricSummary NO_HISTORY = new MetricSummary(0, null, null, null, null, null);

    @Test
    @DisplayName("includes the alert, the hour summary and every impacted object with its name")
    void fullContext() {
        MetricSummary history = new MetricSummary(120, 0.0, 4.4, 3.1, 0.0, Instant.parse("2026-09-06T10:04:58Z"));
        List<ImpactedObject> impacted = List.of(
                new ImpactedObject("node-5", "DistributionNode", Map.of("name", "North Node")),
                new ImpactedObject("bld-12", "ResidentialBuilding", Map.of()),
                new ImpactedObject("bld-19", "ResidentialBuilding", null));

        String prompt = IncidentPromptBuilder.build(new IncidentContext(ALERT, history, impacted, List.of()));

        assertThat(prompt)
                .startsWith("A monitoring rule has fired.")
                .doesNotContain("You are the incident-response coordinator")
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
                .contains("## Responsible crews\n- none assigned\n")
                .endsWith("steps addressed to the responsible crews.");
    }

    @Test
    @DisplayName("says so explicitly when there is no history and nothing in the graph")
    void emptyContext() {
        String prompt = IncidentPromptBuilder.build(new IncidentContext(ALERT, NO_HISTORY, List.of(), List.of()));

        assertThat(prompt)
                .contains("- no readings recorded in the last hour")
                .contains("- none resolved (sensor not linked in the topology graph)")
                .doesNotContain("- count:");
    }

    @Test
    @DisplayName("lists at most MAX_IMPACTED_IN_PROMPT objects and counts the rest instead of dropping them silently")
    void impactedListIsCapped() {
        List<ImpactedObject> impacted = IntStream.range(0, 120)
                .mapToObj(i -> new ImpactedObject("bld-" + i, "ResidentialBuilding", Map.of()))
                .toList();

        String prompt = IncidentPromptBuilder.build(new IncidentContext(ALERT, NO_HISTORY, impacted, List.of()));

        assertThat(prompt)
                .contains("- ResidentialBuilding bld-49\n")
                .doesNotContain("bld-50\n")
                .contains("- ... and 70 more downstream objects (120 in total; list truncated to fit the prompt)");
    }

    @Test
    @DisplayName("lists each responsible crew with its phone and at most MAX_CREW_ASSETS_IN_PROMPT assets")
    void crewsAreListed() {
        List<String> district = IntStream.range(0, 14).mapToObj(i -> "bld-d00-" + i).toList();
        List<Crew> crews = List.of(
                new Crew("crew-d00", "District 00 crew", "+7-900-100-0000", district),
                new Crew("crew-x", "Night crew", null, List.of("node-5")));

        String prompt = IncidentPromptBuilder.build(new IncidentContext(ALERT, NO_HISTORY, List.of(), crews));

        assertThat(prompt)
                .contains("- District 00 crew (crew-d00, +7-900-100-0000): bld-d00-0, ")
                .contains("bld-d00-9 and 4 more\n")
                .doesNotContain("bld-d00-10")
                .contains("- Night crew (crew-x): node-5\n");
        assertThat(IncidentPromptBuilder.SYSTEM_PROMPT).contains("do not invent crews");
    }

    @Test
    @DisplayName("the role lives in the system prompt, not in the per-alert message")
    void systemPromptHoldsTheRole() {
        assertThat(IncidentPromptBuilder.SYSTEM_PROMPT)
                .startsWith("You are the incident-response coordinator")
                .contains("exactly one of HIGH, MEDIUM or LOW");
    }
}
