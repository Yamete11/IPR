package inno.ipr.ai_incident_responder_service.ai;

import com.anthropic.client.AnthropicClient;
import com.anthropic.models.messages.MessageCreateParams;
import com.anthropic.models.messages.OutputConfig;
import com.anthropic.models.messages.StopReason;
import com.anthropic.models.messages.StructuredMessageCreateParams;
import com.anthropic.models.messages.ThinkingConfigAdaptive;
import inno.ipr.ai_incident_responder_service.alert.Alert;
import inno.ipr.ai_incident_responder_service.domain.ImpactedObject;
import inno.ipr.ai_incident_responder_service.domain.MetricSummary;
import inno.ipr.ai_incident_responder_service.exception.UnplannableAlertException;
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
 *
 * <p>{@code MAX_TOKENS} is 16k because adaptive thinking bills its reasoning against this same
 * ceiling: a tight budget risks the model spending it on a hard alert and getting cut off
 * mid-structured-output — which surfaces as a parse failure, not as a short answer. 16k leaves ample
 * room for both halves.
 *
 * <p>{@link #plan(Alert, MetricSummary, List)} checks for a safety-classifier decline first: that is
 * HTTP 200 with {@code stop_reason} {@code refusal} and no plan, so it is reported as such instead of
 * failing on the missing content. A refusal, a {@code max_tokens} cut-off and a response with no
 * structured content all throw {@link UnplannableAlertException}: retrying the same prompt would
 * fail the same way, so the alert is dead-lettered rather than re-billed forever. Every planned
 * alert then logs one line with the plan's shape and {@code response.usage()} — the API is
 * stateless and bills every request in full, so token usage per alert is the number to watch: input
 * = system + RAG context + schema, output = thinking + the plan. {@code sizeOf(List)} counts a plan
 * list for that line; Claude may leave a list null.
 *
 * <p>{@link #buildParams(Alert, MetricSummary, List)} builds the request: adaptive thinking (Claude
 * reasons about severity + dependencies before answering) at an explicit effort — Claude Opus 5.5
 * defaults to {@code medium}, one level below Opus 5, so leaving it unset would silently lower the
 * depth of every plan — the plan schema as structured output, the fixed role as the system prompt
 * and the per-alert RAG context as the single user message (no conversation history: each alert is
 * independent). {@code outputConfig(Class)} replaces the whole {@code output_config} with one
 * holding only the generated schema, so effort is merged into that config afterwards rather than
 * set beforehand. The method is package-private so the test can check both reach the request body.
 */
@Component
@ConditionalOnProperty(name = "ai.claude.mock", havingValue = "false", matchIfMissing = true)
public class ClaudeIncidentPlanner implements IncidentPlanner {

    private static final Logger log = LoggerFactory.getLogger(ClaudeIncidentPlanner.class);

    private static final long MAX_TOKENS = 16_000;

    private final AnthropicClient client;
    private final CircuitBreaker circuitBreaker;
    private final String model;
    private final OutputConfig.Effort effort;

    public ClaudeIncidentPlanner(AnthropicClient client,
                                 CircuitBreaker claudeCircuitBreaker,
                                 @Value("${ai.claude.model}") String model,
                                 @Value("${ai.claude.effort}") String effort) {
        this.client = client;
        this.circuitBreaker = claudeCircuitBreaker;
        this.model = model;
        this.effort = OutputConfig.Effort.of(effort);
    }

    @Override
    public String model() {
        return model;
    }

    @Override
    public IncidentPlan plan(Alert alert, MetricSummary history, List<ImpactedObject> impacted) {
        StructuredMessageCreateParams<IncidentPlan> params = buildParams(alert, history, impacted);

        var response = CircuitBreaker
                .decorateSupplier(circuitBreaker, () -> client.messages().create(params))
                .get();

        StopReason stopReason = response.stopReason().orElse(null);
        if (StopReason.REFUSAL.equals(stopReason)) {
            throw new UnplannableAlertException("Claude declined to plan alert " + alert.alertId()
                    + " (stop_reason=refusal)");
        }
        if (StopReason.MAX_TOKENS.equals(stopReason)) {
            throw new UnplannableAlertException("Claude's plan for alert " + alert.alertId()
                    + " was cut off (stop_reason=max_tokens)");
        }

        IncidentPlan plan = response.content().stream()
                .flatMap(block -> block.text().stream())
                .map(text -> text.text())
                .findFirst()
                .orElseThrow(() -> new UnplannableAlertException(
                        "Claude returned no structured content for alert " + alert.alertId()));

        var usage = response.usage();
        log.info("Claude planned alert {}: criticality={}, {} impacted, {} steps | input_tokens={}, "
                        + "output_tokens={}, cache_read={}, cache_write={}",
                alert.alertId(), plan.criticality(), sizeOf(plan.impactedObjects()),
                sizeOf(plan.remediationSteps()), usage.inputTokens(), usage.outputTokens(),
                usage.cacheReadInputTokens().orElse(0L), usage.cacheCreationInputTokens().orElse(0L));
        return plan;
    }

    StructuredMessageCreateParams<IncidentPlan> buildParams(Alert alert, MetricSummary history,
                                                           List<ImpactedObject> impacted) {
        StructuredMessageCreateParams.Builder<IncidentPlan> builder = MessageCreateParams.builder()
                .model(model)
                .maxTokens(MAX_TOKENS)
                .thinking(ThinkingConfigAdaptive.builder().build())
                .outputConfig(IncidentPlan.class)
                .system(IncidentPromptBuilder.SYSTEM_PROMPT)
                .addUserMessage(IncidentPromptBuilder.build(alert, history, impacted));
        OutputConfig withSchema = builder.build().rawParams().outputConfig().orElseThrow();
        return builder.outputConfig(withSchema.toBuilder().effort(effort).build()).build();
    }

    private static int sizeOf(List<String> list) {
        return list == null ? 0 : list.size();
    }
}
