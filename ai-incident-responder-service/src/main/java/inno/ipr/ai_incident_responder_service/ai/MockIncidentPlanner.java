package inno.ipr.ai_incident_responder_service.ai;

import inno.ipr.ai_incident_responder_service.alert.Alert;
import inno.ipr.ai_incident_responder_service.domain.ImpactedObject;
import inno.ipr.ai_incident_responder_service.domain.MetricSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Offline {@link IncidentPlanner} used when {@code ai.claude.mock=true}: builds a deterministic plan
 * from the alert and the (real) graph-derived impacted objects, without calling the Claude API — so
 * the full pipeline (Kafka → context gathering → persist {@code incidents}) can be exercised
 * end-to-end with no {@code ANTHROPIC_API_KEY} and no billed request. When active, the
 * {@link ClaudeIncidentPlanner} and its {@code AnthropicClient} are not created.
 */
@Component
@ConditionalOnProperty(name = "ai.claude.mock", havingValue = "true")
public class MockIncidentPlanner implements IncidentPlanner {

    private static final Logger log = LoggerFactory.getLogger(MockIncidentPlanner.class);

    @Override
    public String model() {
        return "mock";
    }

    @Override
    public IncidentPlan plan(Alert alert, MetricSummary history, List<ImpactedObject> impacted) {
        List<String> objects = (impacted == null || impacted.isEmpty())
                ? List.of(alert.sensorId())
                : impacted.stream().map(o -> o.label() + " " + o.id()).toList();
        String primary = objects.get(0);

        List<String> steps = List.of(
                "Dispatch a repair crew to " + primary + ".",
                "Inspect the " + alert.metricType() + " reading at sensor " + alert.sensorId()
                        + " (rule " + alert.rule() + ").",
                "Confirm downstream service is restored to all impacted objects.");

        String summary = "Simulated incident: " + alert.metricType() + " breach on "
                + alert.sensorId() + " (value " + alert.value() + ").";

        log.warn("MOCK planner producing canned plan for alert {} (no Claude call); {} impacted objects",
                alert.alertId(), objects.size());
        return new IncidentPlan("HIGH", summary, objects, steps);
    }
}
