package inno.ipr.ai_incident_responder_service.ai;

import inno.ipr.ai_incident_responder_service.alert.Alert;
import inno.ipr.ai_incident_responder_service.domain.ImpactedObject;
import inno.ipr.ai_incident_responder_service.domain.MetricSummary;

import java.util.List;

/**
 * Produces a structured {@link IncidentPlan} for an alert from its gathered context (metric history
 * + impacted graph objects). Two implementations, selected by {@code ai.claude.mock}:
 * {@link ClaudeIncidentPlanner} (real Claude API call, the default) and {@link MockIncidentPlanner}
 * (offline canned plan, no API key / no billed call — for local end-to-end runs).
 *
 * <p>{@link #model()} returns the model id that produced the plan — recorded on the incident row
 * for provenance.
 */
public interface IncidentPlanner {

    IncidentPlan plan(Alert alert, MetricSummary history, List<ImpactedObject> impacted);

    String model();
}
