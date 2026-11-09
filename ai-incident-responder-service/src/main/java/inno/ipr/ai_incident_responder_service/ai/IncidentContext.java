package inno.ipr.ai_incident_responder_service.ai;

import inno.ipr.ai_incident_responder_service.alert.Alert;
import inno.ipr.ai_incident_responder_service.domain.Crew;
import inno.ipr.ai_incident_responder_service.domain.ImpactedObject;
import inno.ipr.ai_incident_responder_service.domain.MetricSummary;

import java.util.List;

/**
 * The retrieved context a planner turns into an {@link IncidentPlan}: the alert, its metric history
 * from TimescaleDB, and from the AGE graph the impacted objects and the crews responsible for them.
 *
 * @param alert    the alert being planned
 * @param history  summary of the alert's metric over the hour up to {@code detected_at}
 * @param impacted the monitored asset first, then everything downstream of it
 * @param crews    crews assigned to any impacted asset
 */
public record IncidentContext(
        Alert alert,
        MetricSummary history,
        List<ImpactedObject> impacted,
        List<Crew> crews) {
}
