package inno.ipr.ai_incident_responder_service.ai;

/**
 * Produces a structured {@link IncidentPlan} for an alert from its gathered {@link IncidentContext}
 * (metric history, impacted graph objects and the crews responsible for them). Two
 * implementations, selected by {@code ai.claude.mock}: {@link ClaudeIncidentPlanner} (real Claude API call, the default) and {@link MockIncidentPlanner}
 * (offline canned plan, no API key / no billed call — for local end-to-end runs).
 *
 * <p>{@link #model()} returns the model id that produced the plan — recorded on the incident row
 * for provenance.
 */
public interface IncidentPlanner {

    IncidentPlan plan(IncidentContext context);

    String model();
}
