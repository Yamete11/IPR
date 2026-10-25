package inno.ipr.ai_incident_responder_service.ai;

import com.anthropic.models.messages.MessageCreateParams;
import com.anthropic.models.messages.OutputConfig;
import inno.ipr.ai_incident_responder_service.alert.Alert;
import inno.ipr.ai_incident_responder_service.domain.MetricSummary;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** The request sent to Claude, built without a client or a network call. */
class ClaudeIncidentPlannerTest {

    private static final Alert ALERT = new Alert("a-1001", "s-00042", "pipe_pressure", 0.0,
            Instant.parse("2026-09-06T10:05:00Z"), "pipe_pressure_min<0.5");

    @Test
    @DisplayName("effort and the structured-output schema both reach the request's output_config")
    void effortAndSchemaTogether() {
        ClaudeIncidentPlanner planner = new ClaudeIncidentPlanner(null, CircuitBreaker.ofDefaults("test"),
                "claude-opus-5-5", "high");

        MessageCreateParams raw = planner.buildParams(ALERT,
                new MetricSummary(0, null, null, null, null, null), List.of()).rawParams();

        assertThat(raw.model().asString()).isEqualTo("claude-opus-5-5");
        assertThat(raw.thinking()).hasValueSatisfying(t -> assertThat(t.isAdaptive()).isTrue());
        OutputConfig output = raw.outputConfig().orElseThrow();
        assertThat(output.effort()).contains(OutputConfig.Effort.HIGH);
        assertThat(output.format()).as("IncidentPlan JSON schema").isPresent();
        assertThat(raw.system()).hasValueSatisfying(system ->
                assertThat(system.asString()).isEqualTo(IncidentPromptBuilder.SYSTEM_PROMPT));
        assertThat(raw.messages()).as("stateless: one user message, no history").hasSize(1);
    }
}
