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

    /**
     * Adaptive thinking bills its reasoning against this same ceiling, so a tight budget risks the
     * model spending it on a hard alert and getting cut off mid-structured-output — which surfaces
     * as a parse failure, not as a short answer. 16k leaves ample room for both halves.
     */
    private static final long MAX_TOKENS = 16_000;

    private final AnthropicClient client;
    private final CircuitBreaker circuitBreaker;
    private final String model;

    public ClaudeIncidentPlanner(AnthropicClient client,
                                 CircuitBreaker claudeCircuitBreaker,
                                 @Value("${ai.claude.model:claude-opus-5}") String model) {
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
                .addUserMessage(IncidentPromptBuilder.build(alert, history, impacted))
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
}
