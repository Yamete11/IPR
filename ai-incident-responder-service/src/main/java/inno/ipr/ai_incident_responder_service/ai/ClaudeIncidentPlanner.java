package inno.ipr.ai_incident_responder_service.ai;

import com.anthropic.client.AnthropicClient;
import com.anthropic.models.messages.MessageCreateParams;
import com.anthropic.models.messages.StructuredMessageCreateParams;
import com.anthropic.models.messages.ThinkingConfigAdaptive;
import inno.ipr.ai_incident_responder_service.alert.Alert;
import inno.ipr.ai_incident_responder_service.domain.ImpactedObject;
import inno.ipr.ai_incident_responder_service.domain.MetricSummary;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * The real {@link IncidentPlanner}: calls the Claude API to turn an alert plus its gathered context
 * (the lightweight RAG) into a structured {@link IncidentPlan}. The call goes through the Claude
 * {@link CircuitBreaker}, and the SDK's structured-output feature
 * ({@code .outputConfig(IncidentPlan.class)}) constrains the response to the plan schema and
 * deserializes it directly — no hand-parsing, no malformed-JSON risk.
 *
 * <p>Active unless {@code ai.claude.mock=true}, in which case {@link MockIncidentPlanner} takes over
 * and this bean (and the {@code AnthropicClient} it needs) is not created.
 */
@Component
@ConditionalOnProperty(name = "ai.claude.mock", havingValue = "false", matchIfMissing = true)
public class ClaudeIncidentPlanner implements IncidentPlanner {

    private static final Logger log = LoggerFactory.getLogger(ClaudeIncidentPlanner.class);

    private static final long MAX_TOKENS = 4096;

    private final AnthropicClient client;
    private final CircuitBreaker circuitBreaker;
    private final String model;

    public ClaudeIncidentPlanner(AnthropicClient client,
                                 CircuitBreaker claudeCircuitBreaker,
                                 @Value("${ai.claude.model:claude-opus-4-8}") String model) {
        this.client = client;
        this.circuitBreaker = claudeCircuitBreaker;
        this.model = model;
    }

    @Override
    public String model() {
        return model;
    }

    @Override
    public IncidentPlan plan(Alert alert, MetricSummary history, List<ImpactedObject> impacted) {
        StructuredMessageCreateParams<IncidentPlan> params = MessageCreateParams.builder()
                .model(model)
                .maxTokens(MAX_TOKENS)
                // Adaptive thinking: let Claude reason about severity + dependencies before answering.
                .thinking(ThinkingConfigAdaptive.builder().build())
                .outputConfig(IncidentPlan.class)
                .addUserMessage(buildPrompt(alert, history, impacted))
                .build();

        var response = CircuitBreaker
                .decorateSupplier(circuitBreaker, () -> client.messages().create(params))
                .get();

        IncidentPlan plan = response.content().stream()
                .flatMap(block -> block.text().stream())
                .map(text -> text.text())
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "Claude returned no structured content for alert " + alert.alertId()));

        log.info("Claude produced plan for alert {}: criticality={}, {} impacted, {} steps",
                alert.alertId(), plan.criticality(),
                plan.impactedObjects() == null ? 0 : plan.impactedObjects().size(),
                plan.remediationSteps() == null ? 0 : plan.remediationSteps().size());
        return plan;
    }

    /**
     * Assembles the RAG prompt: the crew's role, the raw alert, a one-hour metric summary, and the
     * dependency-graph objects the failure impacts. Kept compact so it stays well within token
     * limits regardless of ingestion volume.
     */
    private static String buildPrompt(Alert alert, MetricSummary history, List<ImpactedObject> impacted) {
        StringBuilder sb = new StringBuilder();
        sb.append("You are the incident-response coordinator for a Smart City infrastructure network. ")
                .append("A monitoring rule has fired. Using the context below, produce a structured ")
                .append("remediation plan for the field repair crew.\n\n");

        sb.append("## Alert\n")
                .append("- sensor_id: ").append(alert.sensorId()).append('\n')
                .append("- metric_type: ").append(alert.metricType()).append('\n')
                .append("- value: ").append(alert.value()).append('\n')
                .append("- rule: ").append(alert.rule()).append('\n')
                .append("- detected_at: ").append(alert.detectedAt()).append("\n\n");

        sb.append("## Last hour of readings for this sensor\n");
        if (history == null || history.isEmpty()) {
            sb.append("- no readings recorded in the last hour\n");
        } else {
            sb.append("- count: ").append(history.count()).append('\n')
                    .append("- min: ").append(history.min()).append('\n')
                    .append("- max: ").append(history.max()).append('\n')
                    .append("- avg: ").append(history.avg()).append('\n')
                    .append("- latest: ").append(history.latest())
                    .append(" (at ").append(history.latestAt()).append(")\n");
        }
        sb.append('\n');

        sb.append("## Impacted objects (from the network dependency graph)\n");
        if (impacted == null || impacted.isEmpty()) {
            sb.append("- none resolved (sensor not linked in the topology graph)\n");
        } else {
            for (ImpactedObject o : impacted) {
                Object name = o.properties() == null ? null : o.properties().get("name");
                sb.append("- ").append(o.label()).append(' ').append(o.id());
                if (name != null) {
                    sb.append(" (").append(name).append(')');
                }
                sb.append('\n');
            }
        }
        sb.append('\n');

        sb.append("Assess criticality (HIGH/MEDIUM/LOW), summarise the incident in one sentence, ")
                .append("list the impacted objects, and give ordered, concrete remediation steps.");
        return sb.toString();
    }
}
