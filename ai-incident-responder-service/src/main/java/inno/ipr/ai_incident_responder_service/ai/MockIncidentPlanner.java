package inno.ipr.ai_incident_responder_service.ai;

import inno.ipr.ai_incident_responder_service.alert.Alert;
import inno.ipr.ai_incident_responder_service.domain.Crew;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Offline {@link IncidentPlanner} used when {@code ai.claude.mock=true}: builds a deterministic plan
 * from the alert and the (real) graph-derived context, without calling the Claude API — so the full
 * pipeline (Kafka → context gathering → persist {@code incidents}) can be exercised end-to-end with no
 * {@code ANTHROPIC_API_KEY} and no billed request. When active, the {@link ClaudeIncidentPlanner} and
 * its {@code AnthropicClient} are not created.
 *
 * <p>{@link #plan(IncidentContext)} lists the impacted objects (the sensor itself when the graph has
 * none) and dispatches the first responsible crew by name and phone, or "a repair crew" when the graph
 * assigns none.
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
    public IncidentPlan plan(IncidentContext context) {
        Alert alert = context.alert();
        List<String> objects = context.impacted().isEmpty()
                ? List.of(alert.sensorId())
                : context.impacted().stream().map(o -> o.label() + " " + o.id()).toList();
        String primary = objects.get(0);
        String crew = context.crews().isEmpty() ? "a repair crew" : describe(context.crews().get(0));

        List<String> steps = List.of(
                "Dispatch " + crew + " to " + primary + ".",
                "Inspect the " + alert.metricType() + " reading at sensor " + alert.sensorId()
                        + " (rule " + alert.rule() + ").",
                "Confirm downstream service is restored to all impacted objects.");

        String summary = "Simulated incident: " + alert.metricType() + " breach on "
                + alert.sensorId() + " (value " + alert.value() + ").";

        log.warn("MOCK planner producing canned plan for alert {} (no Claude call); {} impacted objects, {} crews",
                alert.alertId(), objects.size(), context.crews().size());
        return new IncidentPlan("HIGH", summary, objects, steps);
    }

    private static String describe(Crew crew) {
        return crew.phone() == null ? crew.name() : crew.name() + " (" + crew.phone() + ")";
    }
}
